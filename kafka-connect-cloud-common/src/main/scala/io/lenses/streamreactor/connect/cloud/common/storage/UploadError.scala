/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.storage

import java.io.File

trait UploadError {
  def message(): String
}

case class NonExistingFileError(file: File) extends UploadError {
  override def message() = s"attempt to upload non-existing file (${file.getPath})"
}

case class ZeroByteFileError(file: File) extends UploadError {
  override def message() = s"zero byte upload prevented (${file.getPath})"
}

case class UploadFailedError(exception: Throwable, file: File) extends UploadError {
  override def message(): String = s"upload error (${file.getPath}) ${exception.getMessage}"
}

case class EmptyContentsStringError(data: String) extends UploadError {
  override def message() = s"attempt to upload empty string (${data})"
}

case class FileMoveError(exception: Throwable, oldName: String, newName: String) extends UploadError {
  override def message() = s"error moving file from ($oldName) to ($newName) ${exception.getMessage}"
}

case class FileCreateError(exception: Throwable, data: String) extends UploadError {
  override def message() = s"error writing file (${data}) ${exception.getMessage}"
}

case class FileDeleteError(exception: Throwable, fileName: String) extends UploadError {
  override def message() = s"error deleting file ($fileName) ${exception.getMessage}"
}

case class FileLoadError(exception: Throwable, fileName: String) extends UploadError {
  override def message() = s"error loading file ($fileName) ${exception.getMessage}"

  def toException = new RuntimeException(message(), exception)
}

case class FileNameParseError(exception: Throwable, fileName: String) extends UploadError {
  override def message() = s"error parsing file name ($fileName) ${exception.getMessage}"

  def toException = new RuntimeException(message(), exception)
}
case class FileListError(exception: Throwable, bucket: String, path: Option[String]) extends UploadError {
  override def message() = s"error listing files (${path}) ${exception.getMessage}"
}
