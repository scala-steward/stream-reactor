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

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.testcontainers.OpensearchContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.OpensearchContainer.defaultTag
import org.opensearch.testcontainers.{ OpensearchContainer => JavaOpensearchContainer }
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration

/**
 * Wrapper around the testcontainers-java [[JavaOpensearchContainer]].
 *
 * Default image tag MUST equal `Dependencies.openSearchVersion` in `project/Dependencies.scala`.
 * See `OpensearchContainerTagDriftTest` for the compile-time drift guard.
 */
class OpensearchContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String = defaultNetworkAlias,
) extends SingleContainer[JavaOpensearchContainer[_]]
    with StrictLogging {

  val port: Int = 9200

  override val container: JavaOpensearchContainer[_] =
    new JavaOpensearchContainer(dockerImage.withTag(dockerTag))

  container.withNetworkAliases(networkAlias)

  private var securityEnabled: Boolean                    = false
  private var pkiFixture:      Option[SecurityPkiFixture] = None

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def httpHostAddress: String = s"${container.getHost}:${container.getMappedPort(port)}"
  }

  /** Expose client JKS paths (set after [[withSecurityEnabled]] is called). */
  def clientKeystorePath:   String = pkiFixture.map(_.keystorePath.toString).getOrElse("")
  def clientTruststorePath: String = pkiFixture.map(_.truststorePath.toString).getOrElse("")

  /**
   * Enable the OpenSearch Security plugin and bootstrap demo PKI + securityconfig.
   *
   * Must be called before [[start]].
   */
  def withSecurityEnabled(enabled: Boolean): OpensearchContainer = {
    securityEnabled = enabled
    if (enabled) {
      val pki = SecurityPkiFixture.shared
      pki.generate(networkAlias)
      pkiFixture = Some(pki)
      configureSecurityPlugin()
    }
    this
  }

  private def configureSecurityPlugin(): Unit = {
    val pki = pkiFixture.getOrElse(throw new IllegalStateException("PKI not initialised"))

    // Security plugin environment variables — use individual statements to avoid wildcard-type chaining issues
    container.withEnv("discovery.type", "single-node")
    container.withEnv("plugins.security.disabled", "false")
    container.withEnv("plugins.security.ssl.transport.pemcert_filepath", "node.pem")
    container.withEnv("plugins.security.ssl.transport.pemkey_filepath", "node-key.pem")
    container.withEnv("plugins.security.ssl.transport.pemtrustedcas_filepath", "root-ca.pem")
    container.withEnv("plugins.security.ssl.http.enabled", "true")
    container.withEnv("plugins.security.ssl.http.pemcert_filepath", "node.pem")
    container.withEnv("plugins.security.ssl.http.pemkey_filepath", "node-key.pem")
    container.withEnv("plugins.security.ssl.http.pemtrustedcas_filepath", "root-ca.pem")
    container.withEnv("plugins.security.allow_unsafe_democertificates", "true")
    container.withEnv("plugins.security.authcz.admin_dn", "CN=admin,O=lenses,L=test,C=GB")
    container.withEnv("plugins.security.audit.type", "internal_opensearch")
    container.withEnv("plugins.security.restapi.roles_enabled", "all_access,security_rest_api_access")

    // Mount PKI files into the container config directory
    val configDir = "/usr/share/opensearch/config/"
    container.withCopyFileToContainer(MountableFile.forHostPath(pki.rootCaPem), configDir + "root-ca.pem")
    container.withCopyFileToContainer(MountableFile.forHostPath(pki.rootCaKeyPem), configDir + "root-ca-key.pem")
    container.withCopyFileToContainer(MountableFile.forHostPath(pki.nodePem), configDir + "node.pem")
    container.withCopyFileToContainer(MountableFile.forHostPath(pki.nodeKeyPem), configDir + "node-key.pem")
    container.withCopyFileToContainer(MountableFile.forHostPath(pki.adminPem), configDir + "admin.pem")
    container.withCopyFileToContainer(MountableFile.forHostPath(pki.adminKeyPem), configDir + "admin-key.pem")
    container.withCopyFileToContainer(MountableFile.forHostPath(pki.clientPem), configDir + "connect-client.pem")
    container.withCopyFileToContainer(MountableFile.forHostPath(pki.clientKeyPem), configDir + "connect-client-key.pem")

    // Mount securityconfig YAML files
    val securityDir = configDir + "opensearch-security/"
    mountSecurityConfigYaml(securityDir)

    // Use HTTPS for health-check when security is enabled
    container.setWaitStrategy(
      new HttpWaitStrategy()
        .forPort(port)
        .forPath("/_cluster/health")
        .withBasicCredentials("admin", "admin")
        .usingTls()
        .allowInsecure()
        .withStartupTimeout(Duration.ofMinutes(3)),
    )
  }

  private def mountSecurityConfigYaml(securityDir: String): Unit = {
    val resourceBase = "/opensearch-security/"
    Seq("config.yml", "internal_users.yml", "roles.yml", "roles_mapping.yml").foreach { fileName =>
      val resource = getClass.getResourceAsStream(resourceBase + fileName)
      if (resource != null) {
        val tmpFile = Files.createTempFile("opensearch-security-" + fileName.replace(".", "-"), ".yml")
        val content = new String(resource.readAllBytes(), StandardCharsets.UTF_8)
        Files.write(tmpFile, content.getBytes(StandardCharsets.UTF_8))
        container.withCopyFileToContainer(MountableFile.forHostPath(tmpFile), securityDir + fileName)
      } else {
        logger.warn(s"SecurityPlugin: resource not found: $resourceBase$fileName")
      }
    }
  }

  /** Mount a config.yml with BASE64_HMAC_KEY substituted. */
  def withJwtSigningKey(base64HmacKey: String): OpensearchContainer = {
    val resource = getClass.getResourceAsStream("/opensearch-security/config.yml")
    require(resource != null, "config.yml template resource not found")
    val template = new String(resource.readAllBytes(), StandardCharsets.UTF_8)
    val resolved = template.replace("${BASE64_HMAC_KEY}", base64HmacKey)
    val tmpFile  = Files.createTempFile("opensearch-security-config", ".yml")
    Files.write(tmpFile, resolved.getBytes(StandardCharsets.UTF_8))
    container.withCopyFileToContainer(
      MountableFile.forHostPath(tmpFile),
      "/usr/share/opensearch/config/opensearch-security/config.yml",
    )
    this
  }

  override def start(): Unit = {
    super.start()
    if (securityEnabled) {
      runSecurityAdmin()
    }
  }

  private def runSecurityAdmin(): Unit = {
    require(pkiFixture.isDefined, "PKI not initialised")
    logger.info("SecurityPlugin: running securityadmin.sh to apply security configuration")
    val result = container.execInContainer(
      "/usr/share/opensearch/plugins/opensearch-security/tools/securityadmin.sh",
      "-cd",
      "/usr/share/opensearch/config/opensearch-security/",
      "-icl",
      "-nhnv",
      "-cacert",
      "/usr/share/opensearch/config/root-ca.pem",
      "-cert",
      "/usr/share/opensearch/config/admin.pem",
      "-key",
      "/usr/share/opensearch/config/admin-key.pem",
      "-h",
      "localhost",
    )
    if (result.getExitCode != 0) {
      logger.error(s"securityadmin.sh failed:\nstdout: ${result.getStdout}\nstderr: ${result.getStderr}")
      throw new RuntimeException(
        s"securityadmin.sh exited with code ${result.getExitCode}: ${result.getStderr}",
      )
    }
    logger.info("SecurityPlugin: security configuration applied successfully")
  }
}

object OpensearchContainer {

  // MUST equal Dependencies.openSearchVersion in project/Dependencies.scala
  val defaultTag: String = "2.13.0"

  private val dockerImage         = DockerImageName.parse("opensearchproject/opensearch")
  private val defaultNetworkAlias = "opensearch"

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
  ): OpensearchContainer =
    new OpensearchContainer(dockerImage, dockerTag, networkAlias)
}
