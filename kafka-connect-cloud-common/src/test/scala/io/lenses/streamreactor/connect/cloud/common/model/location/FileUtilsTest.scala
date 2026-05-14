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

import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

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
      verifyBufferSize(out, tmpFile, FileUtils.DefaultStagingWriteBufferSize)
    }
  }

  test("toBufferedOutputStream uses supplied custom buffer size") {
    val tmpFile = Files.createTempFile("FileUtilsTest", ".tmp").toFile
    tmpFile.deleteOnExit()
    val customSize = 128 * 1024
    Using.resource(FileUtils.toBufferedOutputStream(tmpFile, customSize)) { out =>
      verifyBufferSize(out, tmpFile, customSize)
    }
  }

  /**
   * Verifies buffer size behaviorally without reflection.
   *
   * BufferedOutputStream.write(byte[]) writes directly to the underlying stream (bypassing the
   * buffer) when len >= buf.length, so we stay below that threshold by writing (bufSize - 1)
   * bytes as a chunk. count becomes bufSize-1, nothing flushed.
   * Two subsequent single-byte write(int) calls bring count to bufSize (no flush yet) and then
   * trigger the auto-flush (count >= buf.length), writing exactly bufSize bytes to disk.
   * A buffer that is smaller than expected causes the chunk write to bypass the buffer and
   * land on disk immediately; a larger buffer means the flush never fires — both deviate from
   * the expected file length, making the assertion sensitive to off-by-one buffer mismatches.
   */
  private def verifyBufferSize(out: java.io.OutputStream, file: java.io.File, bufSize: Int): Assertion = {
    out.write(new Array[Byte](bufSize - 1)) // chunk: len < bufSize, copied into buffer
    out.write(0)                            // count = bufSize, still buffered
    out.write(0)                            // count >= bufSize → flushBuffer(); bufSize bytes hit disk
    file.length() should be(bufSize.toLong)
  }

}
