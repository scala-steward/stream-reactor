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

  private var securityEnabled: Boolean                    = false
  private var pkiFixture:      Option[SecurityPkiFixture] = None

  // ---------------------------------------------------------------------------
  // The Java testcontainers OpensearchContainer.configure() overrides ANY wait
  // strategy we set before start(). When security is enabled it installs an
  // HttpWaitStrategy that accepts 200 or 401; but an uninitialised security
  // backend returns 503 → the wait times out.
  //
  // SecuredContainer subclasses the Java container, calls super.configure()
  // (so env vars, ports, etc. are set) and then replaces the wait strategy with
  // one that also accepts 503.  securityadmin.sh is run AFTER start() returns.
  //
  // Using lazy val so the right container class is chosen once securityEnabled
  // is known (withSecurityEnabled() is called before start()).
  // ---------------------------------------------------------------------------

  /**
   * A subclass of the Java OpensearchContainer that overrides configure() to
   * install a permissive HttpWaitStrategy (200 | 401 | 503) after the super
   * call. This is the only way to prevent super.configure() from clobbering
   * our strategy, since configure() is invoked inside tryStart().
   */
  private class SecuredContainer(image: DockerImageName) extends JavaOpensearchContainer[SecuredContainer](image) {

    override def configure(): Unit = {
      super.configure()
      // super.configure() sets a strategy that accepts 200|401 only.
      // The uninitialised security backend returns 503; override here so the
      // container is considered "ready" once it is responding at all.
      setWaitStrategy(
        new HttpWaitStrategy()
          .usingTls()
          .allowInsecure()
          .forPort(port)
          .withBasicCredentials("admin", "admin")
          .forStatusCodeMatching(code => code == 200 || code == 401 || code == 503)
          .withReadTimeout(Duration.ofSeconds(10))
          .withStartupTimeout(Duration.ofMinutes(3)),
      )
    }
  }

  /** Lazily-initialised underlying Java container (chosen based on securityEnabled). */
  override lazy val container: JavaOpensearchContainer[_] = {
    val c: JavaOpensearchContainer[_] =
      if (securityEnabled) new SecuredContainer(dockerImage.withTag(dockerTag))
      else new JavaOpensearchContainer(dockerImage.withTag(dockerTag))
    c.withNetworkAliases(networkAlias)
    c
  }

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

    // Tell the Java testcontainer that security is on so configure() does NOT
    // emit DISABLE_SECURITY_PLUGIN=true.
    container.withSecurityEnabled()

    // Skip the image's built-in demo-config installer (install_demo_configuration.sh).
    // From OpenSearch 2.12.0+ that script requires OPENSEARCH_INITIAL_ADMIN_PASSWORD and exits if
    // not set.  We supply our own PKI certs and securityconfig YAMLs.
    container.withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")

    // Mount a fully-specified opensearch.yml. Docker env vars with dots in their
    // names (e.g. plugins.security.ssl.*) are NOT reliably translated to -E params
    // by the OpenSearch entrypoint script; an explicit opensearch.yml is the only
    // fully portable approach.
    val nodeYmlStream = getClass.getResourceAsStream("/opensearch-security-node.yml")
    require(nodeYmlStream != null, "opensearch-security-node.yml resource not found")
    val nodeYmlContent = new String(nodeYmlStream.readAllBytes(), StandardCharsets.UTF_8)
    nodeYmlStream.close()
    val nodeYmlTmp = Files.createTempFile("opensearch-node", ".yml")
    Files.write(nodeYmlTmp, nodeYmlContent.getBytes(StandardCharsets.UTF_8))
    // 420 → world-readable so the opensearch user inside the container can read it
    nodeYmlTmp.toFile.setReadable(true, false)
    container.withCopyFileToContainer(
      MountableFile.forHostPath(nodeYmlTmp, 420),
      "/usr/share/opensearch/config/opensearch.yml",
    )

    // Mount PKI files into the container config directory.
    // 420 → world-readable so the opensearch user inside the container can read them.
    val configDir = "/usr/share/opensearch/config/"
    def pkiFile(src: java.nio.file.Path, dst: String): Unit = {
      val _ = container.withCopyFileToContainer(MountableFile.forHostPath(src, 420), configDir + dst)
    }
    pkiFile(pki.rootCaPem, "root-ca.pem")
    pkiFile(pki.rootCaKeyPem, "root-ca-key.pem")
    pkiFile(pki.nodePem, "node.pem")
    pkiFile(pki.nodeKeyPem, "node-key.pem")
    pkiFile(pki.adminPem, "admin.pem")
    pkiFile(pki.adminKeyPem, "admin-key.pem")
    pkiFile(pki.clientPem, "connect-client.pem")
    pkiFile(pki.clientKeyPem, "connect-client-key.pem")

    // Mount securityconfig YAML files
    val securityDir = configDir + "opensearch-security/"
    mountSecurityConfigYaml(securityDir)
  }

  private def mountSecurityConfigYaml(securityDir: String): Unit = {
    val resourceBase = "/opensearch-security/"
    Seq("config.yml", "internal_users.yml", "roles.yml", "roles_mapping.yml").foreach { fileName =>
      val resource = getClass.getResourceAsStream(resourceBase + fileName)
      if (resource != null) {
        val tmpFile = Files.createTempFile("opensearch-security-" + fileName.replace(".", "-"), ".yml")
        val content = new String(resource.readAllBytes(), StandardCharsets.UTF_8)
        Files.write(tmpFile, content.getBytes(StandardCharsets.UTF_8))
        container.withCopyFileToContainer(MountableFile.forHostPath(tmpFile, 420), securityDir + fileName)
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
      MountableFile.forHostPath(tmpFile, 420),
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
    logger.info("SecurityPlugin: security configuration applied; waiting for backend to reload")

    // After securityadmin writes the security config to the index, the BackendRegistry
    // needs a few seconds to reload.  Poll /_cluster/health until we get HTTP 200.
    awaitSecurityReady()
  }

  private def awaitSecurityReady(): Unit = {
    val maxAttempts = 30
    var attempt     = 0
    var ready       = false
    while (!ready && attempt < maxAttempts) {
      val check = container.execInContainer(
        "curl",
        "-sk",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "--user",
        "admin:admin",
        "https://localhost:9200/_cluster/health",
      )
      val code = check.getStdout.trim
      if (code == "200") {
        ready = true
        logger.info("SecurityPlugin: backend ready (HTTP 200 from health endpoint)")
      } else {
        attempt += 1
        logger.info(s"SecurityPlugin: health check returned $code, retrying (attempt $attempt/$maxAttempts)")
        Thread.sleep(1000)
      }
    }
    if (!ready) {
      throw new RuntimeException("Timed out waiting for OpenSearch security backend to become ready")
    }
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
