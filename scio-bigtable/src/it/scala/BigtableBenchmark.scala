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

import com.google.bigtable.v2.{ReadRowsRequest, RowFilter, RowSet}
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.{BigtableDataClient, BigtableInstanceName}
import com.google.common.cache.CacheBuilder
import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.bigtable._
import com.spotify.scio.bigtable.BigtableDoFn._
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object BigtableBenchmark {
  val projectId: String = "scio-playground"
  val instanceId: String = "side-input-test"
  val tableId: String = "side-input-test"

  val bigtableOptions: BigtableOptions = new BigtableOptions.Builder()
    .setProjectId(projectId)
    .setInstanceId(instanceId)
    .setUserAgent("bigtable-test")
    .build()

  val familyName: String = "side"
  val columnQualifier: ByteString = ByteString.copyFromUtf8("value")

  val readRowsRequestBuilder: ReadRowsRequest.Builder = ReadRowsRequest
    .newBuilder()
    .setTableName(new BigtableInstanceName(projectId, instanceId).toTableNameStr(tableId))
    .setFilter(
      RowFilter
        .newBuilder()
        .setFamilyNameRegexFilter(familyName)
        .setColumnQualifierRegexFilter(columnQualifier))
    .setRowsLimit(1L)

  val lowerLetters: Seq[String] = (0 until 26).map('a'.toInt + _).map(_.toChar.toString)
  val upperLetters: Seq[String] = lowerLetters.map(_.toUpperCase)
  val letters: Seq[String] = lowerLetters ++ upperLetters

  class FillDoFn(val n: Int) extends DoFn[String, String] {
    @ProcessElement
    def processElement(c: DoFn[String, String]#ProcessContext): Unit = {
      val prefix = c.element()
      var i = 0
      while (i < n) {
        c.output("%s%010d".format(prefix, i))
        i += 1
      }
    }
  }

  def bigtableLookup(client: BigtableDataClient, input: String): Future[String] = {
    val s = input
    val key = ByteString.copyFromUtf8(s"key-$s")
    val expected = ByteString.copyFromUtf8(s"val-$s")
    val request = readRowsRequestBuilder
      .setRows(RowSet.newBuilder().addRowKeys(key).build())
      .build()
    client.readFlatRowsAsync(request).asScala.transform { resultTry =>
      resultTry.map { input =>
        val result = input
        assert(result.size() == 1)
        val cells = result.get(0).getCells
        assert(result.get(0).getCells.size() == 1)
        val value = cells.get(0).getValue
        assert(value == expected)
        value.toStringUtf8
      }
    }
  }
}

// Generate 52 million key value pairs
object BigtableWrite {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.ensureTables(bigtableOptions, Map(tableId -> List(familyName)))
    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1000000)))
      .map { s =>
        val key = ByteString.copyFromUtf8(s"key-$s")
        val value = ByteString.copyFromUtf8(s"val-$s")
        val m = Mutations.newSetCell(familyName, columnQualifier, value, 0L)
        (key, Iterable(m))
      }
      .saveAsBigtable(bigtableOptions, tableId)
    sc.close()
  }
}

// Async key value lookup
object AsyncBigtableRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1000000)))
      .applyTransform(ParDo.of(BigtableDoFn[String, String](bigtableOptions) {
        case (client, input) => bigtableLookup(client, input)
      }))
      .count
    sc.close()
  }
}

// Async key value lookup with caching
object AsyncCachingBigtableRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val cache =  CacheBuilder.newBuilder()
        .maximumSize(1000000)
        .build[String, String]()

    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1)))
      .flatMap(s => Seq.fill(1000000)(s))
      .applyTransform(ParDo.of(BigtableDoFn[String, String](bigtableOptions, cache) {
        case (client, input) => bigtableLookup(client, input)
      }))
      .count
    sc.close()
  }
}

//// Blocking key value lookup
object BlockingBigtableRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1000000)))
      .applyTransform(ParDo.of(BigtableDoFn[String, String](bigtableOptions) {
        case (client, input) =>
          val output = Await.result(bigtableLookup(client, input), Duration.Inf)
          Future.successful(output)
      }))
      .count
    sc.close()
  }
}

// Blocking key value lookup with caching
object BlockingCachingBigtableRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigtableBenchmark._
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val cache =  CacheBuilder.newBuilder()
      .maximumSize(1000000)
      .build[String, String]()

    sc.parallelize(letters)
      .applyTransform(ParDo.of(new FillDoFn(1)))
      .flatMap(s => Seq.fill(1000000)(s))
      .applyTransform(ParDo.of(BigtableDoFn[String, String](bigtableOptions, cache) {
        case (client, input) =>
          val output = Await.result(bigtableLookup(client, input), Duration.Inf)
          Future.successful(output)
      }))
      .count
    sc.close()
  }
}
