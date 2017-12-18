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

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.beam.sdk.transforms.DoFnTester
import org.apache.beam.sdk.values.KV
import org.scalatest._

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.Future

class BigtableDoFnTest extends FlatSpec with Matchers {

  "BigtableDoFn" should "work" in {
    val fn = BigtableDoFn[Int, String](null, None) {
      case (client, input) => Future.successful(input.toString)
    }
    val output = DoFnTester.of(fn).processBundle((1 to 10).asJava)
    output shouldBe (1 to 10).map(x => KV.of(x, Right(x.toString))).asJava
  }

  it should "work with cache" in {
    val fn = BigtableDoFn[java.lang.Integer, String](null, Some(BigtableDoFnTest.cache)) {
      case (client, input) =>
        BigtableDoFnTest.queue.enqueue(input)
        Future.successful(input.toString)
    }

    val input = ((1 to 10) ++ (5 to 15)).map(a => new Integer(a)).asJava
    val output = DoFnTester.of(fn).processBundle(input)
    output shouldBe ((1 to 10) ++ (5 to 15)).map(x => KV.of(x, Right(x.toString))).asJava

    BigtableDoFnTest.queue.dequeueAll(_ => true)
    DoFnTester.of(fn).processBundle(input)
    BigtableDoFnTest.queue shouldBe mutable.Queue.empty
  }
}

object BigtableDoFnTest {
  val queue: mutable.Queue[Int] = mutable.Queue.empty
  val cache: Cache[java.lang.Integer, String] =
    CacheBuilder.newBuilder().build[java.lang.Integer, String]()
}
