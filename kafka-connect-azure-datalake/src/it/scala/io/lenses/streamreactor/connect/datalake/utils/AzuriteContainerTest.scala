package io.lenses.streamreactor.connect.datalake.utils

import com.azure.storage.file.datalake.DataLakeServiceClient
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.TaskIndexKey
import io.lenses.streamreactor.connect.cloud.common.sink.CloudPlatformEmulatorSuite
import io.lenses.streamreactor.connect.datalake.auth.DatalakeClientCreator
import io.lenses.streamreactor.connect.datalake.config.AuthMode
import io.lenses.streamreactor.connect.datalake.config.AzureConnectionConfig
import io.lenses.streamreactor.connect.datalake.config.AzureConfigSettings._
import io.lenses.streamreactor.connect.datalake.sink.DatalakeSinkTask
import io.lenses.streamreactor.connect.datalake.sink.config.DatalakeSinkConfig
import io.lenses.streamreactor.connect.datalake.storage.DatalakeFileMetadata
import io.lenses.streamreactor.connect.datalake.storage.DatalakeStorageInterface
import io.lenses.streamreactor.connect.testcontainers.AzuriteContainer
import io.lenses.streamreactor.connect.testcontainers.PausableContainer
import io.lenses.streamreactor.connect.testcontainers.TestContainersPausableContainer
import org.apache.kafka.common.config.types.Password

import java.io.File
import java.nio.file.Files
import scala.util.Try

trait AzuriteContainerTest
    extends CloudPlatformEmulatorSuite[
      DatalakeFileMetadata,
      DatalakeStorageInterface,
      DatalakeSinkConfig,
      AzureConnectionConfig,
      DataLakeServiceClient,
      DatalakeSinkTask,
    ]
    with TaskIndexKey
    with LazyLogging {

  implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unit-tests", 1, 1)

  val azuriteContainer:   AzuriteContainer  = AzuriteContainer()
  override val container: PausableContainer = new TestContainersPausableContainer(azuriteContainer)

  override val prefix: String = CONNECTOR_PREFIX

  override def createStorageInterface(client: DataLakeServiceClient): Either[Throwable, DatalakeStorageInterface] =
    Try(new DatalakeStorageInterface(connectorTaskId, client)).toEither

  override def createClient(): Either[Throwable, DataLakeServiceClient] = {
    val azureConfig = AzureConnectionConfig(
      authMode = AuthMode.Credentials(
        accountName = azuriteContainer.accountName,
        accountKey  = new Password(azuriteContainer.accountKey),
      ),
      endpoint = Some(azuriteContainer.getEndpointUrl),
    )
    DatalakeClientCreator.make(azureConfig)
  }

  override def createSinkTask(): DatalakeSinkTask = new DatalakeSinkTask()

  lazy val defaultProps: Map[String, String] =
    Map(
      s"$CONNECTOR_PREFIX.azure.auth.mode"    -> "credentials",
      s"$CONNECTOR_PREFIX.azure.account.name" -> azuriteContainer.accountName,
      s"$CONNECTOR_PREFIX.azure.account.key"  -> azuriteContainer.accountKey,
      ENDPOINT                                -> azuriteContainer.getEndpointUrl,
      "name"                                  -> "azureDatalakeSinkTaskTest",
      TASK_INDEX                              -> "1:1",
    )

  val localRoot: File = Files.createTempDirectory("blah").toFile
  val localFile: File = Files.createTempFile("blah", "blah").toFile

  override def connectorPrefix: String = CONNECTOR_PREFIX

  override def createBucket(client: DataLakeServiceClient): Either[Throwable, Unit] =
    Try {
      client.createFileSystem(BucketName)
      ()
    }.toEither

}
