/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.bigtable

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.{BigtableDataClient, BigtableSession}
import com.google.common.cache.Cache
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.spotify.scio.bigtable.BigtableDoFn._
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{FinishBundle, ProcessElement, StartBundle}
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.{KV, ValueInSingleWindow}
import simulacrum._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.language.implicitConversions

object BigtableDoFn {
  type LookupFn[I, O] = (BigtableDataClient, I) => Future[O]
  type Lookup[I, O] = KV[I, Either[Throwable, O]]
  type WindowLookup[I, O] = ValueInSingleWindow[Lookup[I, O]]

  private val DefaultClient = (options: BigtableOptions) =>
    new BigtableSession(options).createAsyncExecutor().getClient

  def apply[I, O](options: BigtableOptions)(lookupFn: LookupFn[I, O]): BigtableDoFn[I, O] =
    BigtableDoFn[I, O](DefaultClient(options), None)(lookupFn)

  def apply[I, O](options: BigtableOptions, cache: Cache[I, O])(
      lookupFn: LookupFn[I, O]): BigtableDoFn[I, O] =
    BigtableDoFn[I, O](DefaultClient(options), Some(cache))(lookupFn)

  def apply[I, O](client: => BigtableDataClient, cache: => Option[Cache[I, O]])(
      lookupFn: LookupFn[I, O]): BigtableDoFn[I, O] =
    new BigtableDoFn[I, O](client, cache, lookupFn)

  // probably move this to Utils?
  implicit class RichListenableFuture[T](lf: ListenableFuture[T]) {
    def asScala: Future[T] = {
      val p = Promise[T]()
      Futures.addCallback(lf, new FutureCallback[T] {
        def onFailure(t: Throwable): Unit = p.failure(t)

        def onSuccess(result: T): Unit = p.success(result)
      })
      p.future
    }
  }
}

class BigtableDoFn[I, O] private (client: => BigtableDataClient,
                                  cache: => Option[Cache[I, O]],
                                  lookupFn: LookupFn[I, O])
    extends DoFn[I, Lookup[I, O]] {

  import BigtableResources._

  private[this] val instanceId = UUID.randomUUID()
  @transient private[this] lazy val lookups =
    new ConcurrentLinkedQueue[Future[WindowLookup[I, O]]]()
  @transient private[this] lazy val clientResource =
    Resource[BigtableDataClient].get(instanceId.toString, client)
  @transient private[this] lazy val cacheResource = cache.map { value =>
    Resource[Cache[I, O]].get(instanceId.toString, value)
  }

  @StartBundle
  def startBundle(): Unit = {
    lookups.clear()
  }

  @ProcessElement
  def processElement(ctx: ProcessContext, window: BoundedWindow): Unit = {
    val input: I = ctx.element()
    val lookup = cacheResource
      .map { cache =>
        Option(cache.getIfPresent(input))
          .map(Future.successful)
          .getOrElse {
            lookupFn(clientResource, input).map { value =>
              cache.put(input, value)
              value
            }
          }
      }
      .getOrElse(lookupFn(clientResource, input))
      .transform { result =>
        result.map { _ =>
          val kv = KV.of(input, result.toEither)
          ValueInSingleWindow.of(kv, ctx.timestamp(), window, ctx.pane())
        }
      }

    lookups.add(lookup)
  }

  @FinishBundle
  def finishBundle(ctx: FinishBundleContext): Unit = {
    val finish = Future.sequence(lookups.asScala).map { lookups =>
      lookups.foreach { lookup =>
        ctx.output(lookup.getValue, lookup.getTimestamp, lookup.getWindow)
      }
    }

    Await.result(finish, Duration.Inf)
  }

}

@typeclass trait Resource[T] {
  def get(resourceId: String, builder: => T): T
}

object BigtableResources {
  private val Caches = new ConcurrentHashMap[String, Cache[_, _]]()

  implicit def guavaResource[K, B]: Resource[Cache[K, B]] =
    (resourceId, builder) => {
      val cache =
        Caches.computeIfAbsent(resourceId, _ => builder.asInstanceOf[Cache[_, _]])
      cache.asInstanceOf[Cache[K, B]]
    }

  private val Clients = new ConcurrentHashMap[String, BigtableDataClient]()

  implicit def bigtableClient: Resource[BigtableDataClient] =
    (resourceId, builder) => Clients.computeIfAbsent(resourceId, _ => builder)
}
