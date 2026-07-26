/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.source.distribution

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions.ExcludeIndexes
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryLister
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class CloudPartitionSearcherTest extends AnyFlatSpec with Matchers {

  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val connectorTaskId = ConnectorTaskId("sourceName", 1, 0)
  private val bucket          = "bucket"
  private val filesLimit      = 1000

  private case class Search(depth: Int, prefixAsConfigured: Boolean, exclude: Set[String])

  /** Records what it was asked to search for, and returns a given batch of directories per call. */
  private class RecordingLister(results: List[Set[String]] = List.empty) extends DirectoryLister {

    var searches: List[(String, Search)] = List.empty

    private var remaining = results

    override def findDirectories(
      bucketAndPrefix:    CloudLocation,
      filesLimit:         Int,
      partitionDepth:     Int,
      exclude:            Set[String],
      wildcardExcludes:   Set[String],
      prefixAsConfigured: Boolean,
    ): IO[Set[String]] =
      IO {
        searches =
          searches :+ (bucketAndPrefix.prefixOrDefault() -> Search(partitionDepth, prefixAsConfigured, exclude))
        remaining match {
          case head :: tail => remaining = tail; head
          case Nil          => Set.empty
        }
      }
  }

  private def options(partitionDepth: Int, prefixAsConfigured: Boolean): PartitionSearcherOptions =
    PartitionSearcherOptions(
      partitionDepth     = partitionDepth,
      prefixAsConfigured = prefixAsConfigured,
      continuous         = true,
      interval           = 100.millis,
      wildcardExcludes   = ExcludeIndexes,
    )

  private val roots = Seq(
    CloudLocation(bucket, "backups/avro".some),
    CloudLocation(bucket, "topic-1/".some),
    CloudLocation(bucket, None),
  )

  private def searchesFor(settings: PartitionSearcherOptions): Map[String, Search] = {
    val lister = new RecordingLister
    new CloudPartitionSearcher(_ => Right(filesLimit), lister, roots, settings, connectorTaskId)
      .find(Seq.empty)
      .unsafeRunSync()
    lister.searches.toMap
  }

  // A legacy configuration searched from the prefix exactly as configured, so the searcher has to ask for that or a
  // prefix without a trailing slash stops behaving as it did.
  "the searcher" should "ask for the configured depth and prefix handling for every root" in {
    searchesFor(options(partitionDepth = 1, prefixAsConfigured = true)) should be(
      Map(
        "backups/avro" -> Search(1, prefixAsConfigured = true, Set.empty),
        "topic-1/"     -> Search(1, prefixAsConfigured = true, Set.empty),
        ""             -> Search(1, prefixAsConfigured = true, Set.empty),
      ),
    )

    searchesFor(options(partitionDepth = 2, prefixAsConfigured = false)) should be(
      Map(
        "backups/avro" -> Search(2, prefixAsConfigured = false, Set.empty),
        "topic-1/"     -> Search(2, prefixAsConfigured = false, Set.empty),
        ""             -> Search(2, prefixAsConfigured = false, Set.empty),
      ),
    )
  }

  "the searcher" should "exclude the partitions a previous round already found and accumulate them" in {
    val root   = CloudLocation(bucket, "prefix/".some)
    val lister = new RecordingLister(List(Set("prefix/topicA/"), Set("prefix/topicB/")))

    val searcher =
      new CloudPartitionSearcher(
        _ => Right(filesLimit),
        lister,
        Seq(root),
        options(partitionDepth = 1, prefixAsConfigured = false),
        connectorTaskId,
      )

    val firstRound  = searcher.find(Seq.empty).unsafeRunSync()
    val secondRound = searcher.find(firstRound).unsafeRunSync()

    lister.searches.map { case (_, search) => search.exclude } should be(
      List(Set.empty, Set("prefix/topicA/")),
    )

    firstRound.map(_.results) should be(List(Set("prefix/topicA/")))
    // Only the newly found directory is reported, so the reader manager for topicA is not built a second time.
    secondRound.map(_.results) should be(List(Set("prefix/topicB/")))
    secondRound.map(_.allPartitions) should be(List(Set("prefix/topicA/", "prefix/topicB/")))
  }
}
