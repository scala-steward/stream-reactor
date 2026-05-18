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
import io.lenses.streamreactor.common.config.Helpers
import io.lenses.streamreactor.common.utils.JarManifestProvided
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import java.util
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Abstract base for all elastic/opensearch sink connectors.
 * Subclasses provide:
 *  - [[taskClass]]: the concrete SinkTask implementation
 *  - [[kcqlConfigKey]]: the full KCQL config key for this connector's prefix
 *  - [[configDef]]: the connector's ConfigDef
 */
abstract class AbstractElasticSinkConnector extends SinkConnector with StrictLogging with JarManifestProvided {

  private var configProps: Option[util.Map[String, String]] = None

  protected def kcqlConfigKey: String
  protected def configDef:     ConfigDef

  override def config(): ConfigDef = configDef

  override def taskClass(): Class[_ <: Task]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps.get).toList.asJava
  }

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting sink connector.")
    Helpers.checkInputTopics(kcqlConfigKey, props.asScala.toMap)
    configProps = Some(props)
  }

  override def stop(): Unit = {}
}
