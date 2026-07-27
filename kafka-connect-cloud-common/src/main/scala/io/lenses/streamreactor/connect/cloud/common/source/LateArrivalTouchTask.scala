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
package io.lenses.streamreactor.connect.cloud.common.source

import cats.effect.IO
import cats.effect.Ref
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceBucketOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.OrderingType
import io.lenses.streamreactor.connect.cloud.common.source.state.CloudLocationKey
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.PollLoop

import java.time.Instant
import scala.concurrent.duration._

/**
 * A background task that periodically touches files older than the watermark to handle late arrivals.
 * This task runs on an interval and updates the lastModified timestamp of files that were uploaded
 * with timestamps older than the current watermark, ensuring they get processed.
 *
 * Watermarks are stored per discovered directory (the source partition key is `{container, directoryPath}`), so the
 * task reads the directories this task has discovered via `discoveredPathsFn` and looks up the watermark for each of
 * them. Because directories are partitioned across tasks by ownership, every task can safely touch its own
 * directories without duplicating work.
 *
 * The implementation uses IO throughout and checks cancelledRef between operations to ensure
 * the task can be stopped promptly even during long-running touch operations.
 */
object LateArrivalTouchTask extends LazyLogging {

  /**
   * Creates an IO action that runs the touch task on an interval.
   *
   * @param connectorTaskId   The task identifier for logging
   * @param storageInterface  The storage interface to interact with cloud storage
   * @param bucketOptions     The bucket options containing late arrival configuration
   * @param interval          The interval between touch operations
   * @param contextOffsetFn   Function to read the current watermark from Kafka Connect offsets
   * @param discoveredPathsFn Function returning the (root, discovered directory) pairs this task has discovered
   * @param cancelledRef      Reference to check if the task should stop
   * @return An IO action that runs the touch task loop
   */
  def run[M <: FileMetadata](
    connectorTaskId:   ConnectorTaskId,
    storageInterface:  StorageInterface[M],
    bucketOptions:     Seq[CloudSourceBucketOptions[M]],
    interval:          FiniteDuration,
    contextOffsetFn:   CloudLocation => Option[CloudLocation],
    discoveredPathsFn: () => IO[Seq[(CloudLocation, CloudLocation)]],
    cancelledRef:      Ref[IO, Boolean],
  ): IO[Unit] = {
    val lateArrivalOptions = bucketOptions.filter(_.processLateArrival)

    // Touching a file only influences file selection through DateOrderingBatchLister, which is used solely by the
    // LastModified ordering. Under the default AlphaNumeric ordering touching has no effect, so it is skipped to avoid
    // a wasted copy operation per file on every interval.
    val (alphaNumericOptions, effectiveOptions) =
      lateArrivalOptions.partition(_.orderingType == OrderingType.AlphaNumeric)

    val warnAlphaNumeric = IO.delay {
      alphaNumericOptions.foreach { sbo =>
        logger.warn(
          s"[${connectorTaskId.show}] Late arrival processing is enabled for ${sbo.sourceBucketAndPrefix.show} but the ordering type is AlphaNumeric, under which touching files has no effect. Set the ordering type to LastModified or remove `post.process.action.watermark.process.late.arrival`. Skipping late arrival processing for this source.",
        )
      }
    }

    val enabledRootKeys: Set[CloudLocationKey] = effectiveOptions.map(_.sourceBucketAndPrefix.toKey).toSet

    warnAlphaNumeric >> {
      if (enabledRootKeys.isEmpty) {
        val reason =
          if (alphaNumericOptions.nonEmpty)
            "all sources with late arrival processing use AlphaNumeric ordering, under which touching has no effect"
          else
            "no bucket options have late arrival processing enabled"
        IO.delay(logger.info(s"[${connectorTaskId.show}] Late arrival touch task will not run: $reason."))
      } else {
        IO.delay(
          logger.info(
            s"[${connectorTaskId.show}] Starting late arrival touch task with interval ${interval.toSeconds}s for ${enabledRootKeys.size} bucket(s).",
          ),
        ) >>
          PollLoop.run(interval, cancelledRef) { () =>
            touchFilesForDiscoveredPaths(connectorTaskId,
                                         storageInterface,
                                         enabledRootKeys,
                                         contextOffsetFn,
                                         discoveredPathsFn,
                                         cancelledRef,
            )
              .handleErrorWith { err =>
                IO.delay(
                  logger.error(
                    s"[${connectorTaskId.show}] Error in late arrival touch task. Task will resume on next interval.",
                    err,
                  ),
                )
              }
          }
      }
    }
  }

  /**
   * Touches files older than the watermark for every discovered directory owned by this task whose root has late
   * arrival processing enabled. Checks cancelledRef between each directory to allow early exit.
   */
  private def touchFilesForDiscoveredPaths[M <: FileMetadata](
    connectorTaskId:   ConnectorTaskId,
    storageInterface:  StorageInterface[M],
    enabledRootKeys:   Set[CloudLocationKey],
    contextOffsetFn:   CloudLocation => Option[CloudLocation],
    discoveredPathsFn: () => IO[Seq[(CloudLocation, CloudLocation)]],
    cancelledRef:      Ref[IO, Boolean],
  ): IO[Unit] =
    discoveredPathsFn().flatMap { pairs =>
      pairs
        .collect { case (root, path) if enabledRootKeys.contains(root.toKey) => path }
        .traverse_ { path =>
          checkCancelledAndRun(cancelledRef) {
            touchFilesForPath(connectorTaskId, storageInterface, path, contextOffsetFn, cancelledRef)
          }
        }
    }

  /**
   * Helper to check if cancelled before running an IO action.
   * Returns IO.unit if cancelled, otherwise runs the action.
   */
  private def checkCancelledAndRun(cancelledRef: Ref[IO, Boolean])(action: IO[Unit]): IO[Unit] =
    cancelledRef.get.flatMap { cancelled =>
      if (cancelled) IO.unit
      else action
    }

  /**
   * Touches files older than the watermark for a specific discovered directory.
   */
  private def touchFilesForPath[M <: FileMetadata](
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface[M],
    path:             CloudLocation,
    contextOffsetFn:  CloudLocation => Option[CloudLocation],
    cancelledRef:     Ref[IO, Boolean],
  ): IO[Unit] =
    IO.delay {
      // Read the current watermark from Kafka Connect offsets, keyed on the discovered directory
      contextOffsetFn(path)
    }.flatMap {
      case Some(watermarkLocation) =>
        watermarkLocation.timestamp match {
          case Some(watermarkTimestamp) =>
            touchFilesOlderThan(connectorTaskId, storageInterface, path, watermarkTimestamp, cancelledRef)
          case None =>
            IO.delay(
              logger.debug(
                s"[${connectorTaskId.show}] No timestamp in watermark for ${path.bucket}/${path.prefixOrDefault()}, skipping touch.",
              ),
            )
        }
      case None =>
        IO.delay(
          logger.debug(
            s"[${connectorTaskId.show}] No watermark found for ${path.bucket}/${path.prefixOrDefault()}, skipping touch.",
          ),
        )
    }

  /**
   * Lists all files and touches those with lastModified older than the watermark timestamp.
   * Checks cancelledRef between each file touch to allow early exit.
   */
  private def touchFilesOlderThan[M <: FileMetadata](
    connectorTaskId:    ConnectorTaskId,
    storageInterface:   StorageInterface[M],
    path:               CloudLocation,
    watermarkTimestamp: Instant,
    cancelledRef:       Ref[IO, Boolean],
  ): IO[Unit] = {
    val bucket = path.bucket
    val prefix = path.prefix

    // Use IO.blocking for network I/O operations to avoid blocking the compute pool
    IO.blocking(storageInterface.listFileMetaRecursive(bucket, prefix)).flatMap {
      case Right(Some(response)) =>
        val olderFiles = response.files.filter(_.lastModified.isBefore(watermarkTimestamp))

        if (olderFiles.nonEmpty) {
          IO.delay(
            logger.info(
              s"[${connectorTaskId.show}] Late arrival touch: Found ${olderFiles.size} files older than watermark ($watermarkTimestamp) in $bucket/${prefix.getOrElse("")}. Touching them.",
            ),
          ) >> touchFiles(connectorTaskId, storageInterface, bucket, olderFiles, cancelledRef)
        } else {
          IO.delay(
            logger.debug(
              s"[${connectorTaskId.show}] No files older than watermark found in $bucket/${prefix.getOrElse("")}",
            ),
          )
        }

      case Right(None) =>
        IO.delay(logger.debug(s"[${connectorTaskId.show}] No files found in $bucket/${prefix.getOrElse("")}"))

      case Left(error) =>
        IO.delay(logger.warn(s"[${connectorTaskId.show}] Failed to list files for touch operation: ${error.message()}"))
    }
  }

  /**
   * Touches a sequence of files, checking cancelledRef between each operation.
   * Uses IO.cede to yield control and IO.blocking for network I/O to avoid blocking the compute pool.
   */
  private def touchFiles[M <: FileMetadata](
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface[M],
    bucket:           String,
    files:            Seq[M],
    cancelledRef:     Ref[IO, Boolean],
  ): IO[Unit] =
    files.traverse_ { fileMeta =>
      checkCancelledAndRun(cancelledRef) {
        IO.blocking(storageInterface.touchFile(bucket, fileMeta.file)).flatMap {
          case Right(_) =>
            IO.delay(
              logger.debug(
                s"[${connectorTaskId.show}] Successfully touched file ${fileMeta.file} to update lastModified timestamp",
              ),
            )
          case Left(error) =>
            // Log warning but don't fail - best effort approach
            IO.delay(
              logger.warn(s"[${connectorTaskId.show}] Failed to touch file ${fileMeta.file}: ${error.message()}"),
            )
        }
      }
    }
}
