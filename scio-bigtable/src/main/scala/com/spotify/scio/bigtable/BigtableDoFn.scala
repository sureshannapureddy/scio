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

import java.{lang, util}
import java.util.UUID
import java.util.concurrent.{Callable, ConcurrentHashMap, ConcurrentLinkedQueue, ConcurrentMap}

import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.{BigtableDataClient, BigtableSession}
import com.google.common.cache.{Cache, CacheStats}
import com.google.common.collect.ImmutableMap
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
  import Resource._

  type LookupFn[I, O] = (BigtableDataClient, I) => Future[O]
  type Lookup[I, O] = KV[I, Either[Throwable, O]]
  type WindowLookup[I, O] = ValueInSingleWindow[Lookup[I, O]]

  def apply[I, O](options: BigtableOptions)(lookupFn: LookupFn[I, O]): BigtableDoFn[I, O] = {
    val sessionFn = (uuid: UUID) =>
      Resource[BigtableSession]
        .get(uuid.toString, () => new BigtableSession(options))
    val cacheFn = (uuid: UUID) =>
      Resource[Cache[I, O]]
        .get(uuid.toString, () => new NoOpCache[I, O]())

    BigtableDoFn[I, O](sessionFn, cacheFn)(lookupFn)
  }

  def apply[I, O](options: BigtableOptions, cache: Cache[I, O])(
      lookupFn: LookupFn[I, O]): BigtableDoFn[I, O] = {
    val sessionFn = (uuid: UUID) =>
      Resource[BigtableSession]
        .get(uuid.toString, () => new BigtableSession(options))
    val cacheFn = (uuid: UUID) =>
      Resource[Cache[I, O]]
        .get(uuid.toString, () => cache)

    BigtableDoFn[I, O](sessionFn, cacheFn)(lookupFn)
  }

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

case class BigtableDoFn[I, O](sessionFn: UUID => BigtableSession, cacheFn: UUID => Cache[I, O])(
    lookupFn: LookupFn[I, O])
    extends DoFn[I, Lookup[I, O]] {

  private[this] val instanceId = UUID.randomUUID()
  @transient private[this] lazy val lookups =
    new ConcurrentLinkedQueue[Future[WindowLookup[I, O]]]()

  @StartBundle
  def startBundle(): Unit = {
    lookups.clear()
  }

  @ProcessElement
  def processElement(ctx: ProcessContext, window: BoundedWindow): Unit = {
    val input: I = ctx.element()
    val cache = cacheFn(instanceId)
    val session = sessionFn(instanceId)

    val lookup = Option(cache.getIfPresent(input))
      .map(Future.successful)
      .getOrElse {
        lookupFn(session.getDataClient, input).map { value =>
          cache.put(input, value)
          value
        }
      }
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
  def get(resourceId: String, builder: () => T): T
}

object Resource {
  private val Caches = new ConcurrentHashMap[String, Cache[_, _]]()

  implicit def guavaResource[K, B]: Resource[Cache[K, B]] =
    (resourceId, builder) => {
      val cache =
        Caches.computeIfAbsent(resourceId, _ => builder().asInstanceOf[Cache[_, _]])
      cache.asInstanceOf[Cache[K, B]]
    }

  private val Sessions = new ConcurrentHashMap[String, BigtableSession]()

  implicit def bigtableSession: Resource[BigtableSession] =
    (resourceId, builder) => Sessions.computeIfAbsent(resourceId, _ => builder())
}

class NoOpCache[I, O] extends Cache[I, O] {
  override def getAllPresent(keys: lang.Iterable[_]): ImmutableMap[I, O] = ImmutableMap.of()

  override def asMap(): ConcurrentMap[I, O] = new ConcurrentHashMap[I, O]()

  override def invalidate(key: scala.Any): Unit = Unit

  override def put(key: I, value: O): Unit = Unit

  override def invalidateAll(keys: lang.Iterable[_]): Unit = Unit

  override def invalidateAll(): Unit = Unit

  override def size(): Long = 0

  override def stats(): CacheStats = new CacheStats(0, 0, 0, 0, 0, 0)

  override def cleanUp(): Unit = Unit

  override def putAll(m: util.Map[_ <: I, _ <: O]): Unit = Unit

  override def get(key: I, loader: Callable[_ <: O]): O = loader.call()

  override def getIfPresent(key: scala.Any): O = null.asInstanceOf[O]
}
