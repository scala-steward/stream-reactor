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
package io.lenses.streamreactor.connect.testcontainers

import io.lenses.streamreactor.connect.testcontainers.AzuriteContainer.defaultBlobPort
import io.lenses.streamreactor.connect.testcontainers.AzuriteContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.AzuriteContainer.defaultTag
import io.lenses.streamreactor.connect.testcontainers.AzuriteContainer.wellKnownAccountKey
import io.lenses.streamreactor.connect.testcontainers.AzuriteContainer.wellKnownAccountName
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * Testcontainer wrapping the Microsoft Azurite Azure Storage emulator.
 *
 * Azurite exposes an ADLS Gen2 / Azure Blob endpoint on port 10000.
 * The well-known Azurite development credentials are hardcoded; do NOT use in production.
 *
 * Endpoint URL pattern (for use with Azure DataLake SDK):
 *   http://{host}:{mappedPort}/{accountName}
 */
class AzuriteContainer(
  dockerImage:  DockerImageName,
  dockerTag:    String = defaultTag,
  networkAlias: String = defaultNetworkAlias,
  blobPort:     Int    = defaultBlobPort,
) extends GenericContainer[AzuriteContainer](dockerImage.withTag(dockerTag)) {

  withNetworkAliases(networkAlias)
  withExposedPorts(blobPort)
  // Azurite is ready when its blob service port is listening.
  waitingFor(Wait.forListeningPort())

  /** Azurite well-known development account name. */
  val accountName: String = wellKnownAccountName

  /** Azurite well-known development account key. */
  val accountKey: String = wellKnownAccountKey

  /**
   * Returns the ADLS Gen2 service endpoint URL for the mapped host/port.
   * Pattern: http://{host}:{port}/{accountName}
   */
  def getEndpointUrl: String =
    s"http://$getHost:${getMappedPort(blobPort)}/$accountName"

  def container: PausableContainer = new TestContainersPausableContainer(this)
}

object AzuriteContainer {

  /** Well-known Azurite development account name (public; safe for local testing only). */
  val wellKnownAccountName: String = "devstoreaccount1"

  /**
   * Well-known Azurite development account key (public; safe for local testing only).
   * Source: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key
   */
  val wellKnownAccountKey: String =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

  private val dockerImage         = DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite")
  private val defaultTag          = "3.33.0"
  private val defaultNetworkAlias = "azurite"
  private val defaultBlobPort     = 10000

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
    blobPort:     Int    = defaultBlobPort,
  ): AzuriteContainer =
    new AzuriteContainer(dockerImage, dockerTag, networkAlias, blobPort)
}
