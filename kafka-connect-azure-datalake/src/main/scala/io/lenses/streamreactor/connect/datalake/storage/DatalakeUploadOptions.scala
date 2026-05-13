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
package io.lenses.streamreactor.connect.datalake.storage

import com.azure.storage.common.ParallelTransferOptions

/**
 * Optional upload tuning parameters for Azure Data Lake Storage Gen2.
 *
 * All fields are optional; when absent the Azure SDK default is used for that parameter.
 *
 * Memory note: peak buffer usage per concurrent upload is approximately
 * `maxConcurrency * blockSizeBytes`. Increase carefully in memory-constrained deployments.
 */
case class DatalakeUploadOptions(
  maxConcurrency:         Option[Int],
  blockSizeBytes:         Option[Long],
  maxSingleUploadBytes:   Option[Long],
) {

  def toParallelTransferOptions: ParallelTransferOptions = {
    var opts = new ParallelTransferOptions()
    maxConcurrency.foreach(c => opts = opts.setMaxConcurrency(c))
    blockSizeBytes.foreach(b => opts = opts.setBlockSizeLong(b))
    maxSingleUploadBytes.foreach(s => opts = opts.setMaxSingleUploadSizeLong(s))
    opts
  }
}

object DatalakeUploadOptions {
  val default: DatalakeUploadOptions = DatalakeUploadOptions(None, None, None)
}
