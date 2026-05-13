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
package io.lenses.streamreactor.connect.cloud.common.model.location

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.BufferedOutputStream
import java.lang.reflect.Field
import java.nio.file.Files
import scala.util.Using

class FileUtilsTest extends AnyFunSuite with Matchers {

  test("create parent files") {
    val tmpDir   = Files.createTempDirectory("FileUtilsTest")
    val filePath = tmpDir.resolve("i/can/haz/cheeseburger.txt")
    val file     = filePath.toFile

    file.exists should be(false)

    FileUtils.createFileAndParents(filePath.toFile)

    file.exists should be(true)
    file.isFile should be(true)
  }

  test("toBufferedOutputStream uses default 64 KB buffer") {
    val tmpFile = Files.createTempFile("FileUtilsTest", ".tmp").toFile
    tmpFile.deleteOnExit()
    Using.resource(FileUtils.toBufferedOutputStream(tmpFile)) { out =>
      bufSize(out) should be(FileUtils.DefaultStagingWriteBufferSize)
    }
  }

  test("toBufferedOutputStream uses supplied custom buffer size") {
    val tmpFile = Files.createTempFile("FileUtilsTest", ".tmp").toFile
    tmpFile.deleteOnExit()
    val customSize = 128 * 1024
    Using.resource(FileUtils.toBufferedOutputStream(tmpFile, customSize)) { out =>
      bufSize(out) should be(customSize)
    }
  }

  private def bufSize(bos: BufferedOutputStream): Int = {
    val f: Field = classOf[java.io.BufferedOutputStream].getSuperclass.getDeclaredField("buf")
    f.setAccessible(true)
    f.get(bos).asInstanceOf[Array[Byte]].length
  }

}
