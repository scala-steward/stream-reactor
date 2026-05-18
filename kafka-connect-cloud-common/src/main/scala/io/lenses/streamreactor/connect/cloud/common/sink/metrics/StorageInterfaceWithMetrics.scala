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
package io.lenses.streamreactor.connect.cloud.common.sink.metrics

import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.storage._
import io.circe.Encoder

import java.io.InputStream
import java.time.Instant

/**
 * Decorator around [[StorageInterface]] that records per-operation latency and error
 * counts into [[CloudSinkMetrics]].
 *
 * This decorator MUST be the outermost wrapper applied to the real storage implementation.
 * If other wrappers are introduced in future, add a comment at the wrapping call-site to
 * prevent mis-ordering.
 *
 * Instrumented methods:
 *   - [[uploadFile]]         → storageUpload timer + error counter
 *   - [[mvFile]]             → storageCopy  timer + error counter
 *   - [[deleteFile]]         → storageDelete timer + error counter
 *   - [[deleteFiles]]        → storageDelete timer + error counter (same counters)
 *   - [[getBlobAsStringAndEtag]] → storageGet timer + error counter
 *   - [[listKeysRecursive]]  → storageList timer + error counter
 *   - [[listFileMetaRecursive]] → storageList timer + error counter (same counters)
 *
 * All other methods are pure delegation with no instrumentation overhead.
 */
class StorageInterfaceWithMetrics[SM <: FileMetadata](
  delegate: StorageInterface[SM],
  metrics:  CloudSinkMetrics,
) extends StorageInterface[SM] {

  private def timed[A](f: => A): (A, Long) = {
    val start  = System.currentTimeMillis()
    val result = f
    (result, System.currentTimeMillis() - start)
  }

  override def system(): String = delegate.system()

  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
    val (result, elapsed) = timed(delegate.uploadFile(source, bucket, path))
    metrics.recordStorageUpload(elapsed, result.isLeft)
    result
  }

  override def close(): Unit = delegate.close()

  override def pathExists(bucket: String, path: String): Either[PathError, Boolean] =
    delegate.pathExists(bucket, path)

  override def list(
    bucket:     String,
    prefix:     Option[String],
    lastFile:   Option[SM],
    numResults: Int,
  ): Either[FileListError, Option[ListOfKeysResponse[SM]]] =
    delegate.list(bucket, prefix, lastFile, numResults)

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[SM]]] = {
    val (result, elapsed) = timed(delegate.listFileMetaRecursive(bucket, prefix))
    metrics.recordStorageList(elapsed, result.isLeft)
    result
  }

  override def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[SM]]] = {
    val (result, elapsed) = timed(delegate.listKeysRecursive(bucket, prefix))
    metrics.recordStorageList(elapsed, result.isLeft)
    result
  }

  override def seekToFile(
    bucket:       String,
    fileName:     String,
    lastModified: Option[Instant],
  ): Option[SM] = delegate.seekToFile(bucket, fileName, lastModified)

  override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] =
    delegate.getBlob(bucket, path)

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] =
    delegate.getBlobAsString(bucket, path)

  override def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)] = {
    val (result, elapsed) = timed(delegate.getBlobAsStringAndEtag(bucket, path))
    metrics.recordStorageGet(elapsed, result.isLeft)
    result
  }

  override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
    delegate.getMetadata(bucket, path)

  override def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit] =
    delegate.writeStringToFile(bucket, path, data)

  override def writeBlobToFile[O](
    bucket:           String,
    path:             String,
    objectProtection: ObjectProtection[O],
  )(implicit encoder: Encoder[O]): Either[UploadError, ObjectWithETag[O]] =
    delegate.writeBlobToFile(bucket, path, objectProtection)

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] = {
    val (result, elapsed) = timed(delegate.deleteFiles(bucket, files))
    metrics.recordStorageDelete(elapsed, result.isLeft)
    result
  }

  override def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit] = {
    val (result, elapsed) = timed(delegate.deleteFile(bucket, file, eTag))
    metrics.recordStorageDelete(elapsed, result.isLeft)
    result
  }

  override def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
    maybeEtag: Option[String],
  ): Either[FileMoveError, Unit] = {
    val (result, elapsed) = timed(delegate.mvFile(oldBucket, oldPath, newBucket, newPath, maybeEtag))
    metrics.recordStorageCopy(elapsed, result.isLeft)
    result
  }

  override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] =
    delegate.createDirectoryIfNotExists(bucket, path)

  override def touchFile(bucket: String, path: String): Either[FileTouchError, Unit] =
    delegate.touchFile(bucket, path)
}
