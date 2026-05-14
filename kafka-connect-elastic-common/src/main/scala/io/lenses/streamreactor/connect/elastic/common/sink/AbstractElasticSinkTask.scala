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
package io.lenses.streamreactor.connect.elastic.common.sink

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * Abstract base SinkTask for all elastic/opensearch connectors.
 * Subclasses must implement [[createWriter]], [[errorRetryIntervalKey]], [[progressEnabledKey]],
 * and provide the config-def parse step inside [[createWriter]].
 */
abstract class AbstractElasticSinkTask extends SinkTask with StrictLogging with JarManifestProvided {

  private var writer: Option[JsonBulkWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false

  protected def asciiArtResource: String

  /**
   * Create the writer from the resolved config map. Also responsible for calling
   * `configDef.parse(conf)` BEFORE constructing settings — this surfaces missing
   * required keys as ConfigException at the start boundary.
   */
  protected def createWriter(conf: Map[String, String]): JsonBulkWriter

  protected def errorRetryIntervalKey: String
  protected def progressEnabledKey:    String

  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, asciiArtResource)

    val conf    = if (context.configs().isEmpty) props else context.configs()
    val confMap = conf.asScala.toMap

    val w = createWriter(confMap)

    // Wire the retry interval into the context timeout when policy is RETRY
    w.settings.errorPolicy match {
      case RetryErrorPolicy() =>
        val retryInterval = conf.asScala.get(errorRetryIntervalKey).map(_.toLong).getOrElse(
          ElasticCommonConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
        )
        context.timeout(retryInterval)
      case _ =>
    }

    enableProgress = conf.asScala.get(progressEnabledKey).exists(_.toBoolean)
    writer         = Some(w)
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.asScala.toVector
    writer.foreach(_.write(seq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  override def stop(): Unit = {
    logger.info("Stopping sink connector.")
    writer.foreach(w => w.close())
    progressCounter.empty()
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit =
    logger.info("Flushing sink")
}
