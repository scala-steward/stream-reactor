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

import java.io.File
import java.nio.file.Files
import java.nio.file.Path

/**
 * Generates a demo PKI for OpenSearch IT tests.
 *
 * Produces:
 *  - root-ca.pem + root-ca-key.pem (self-signed root CA)
 *  - node.pem + node-key.pem (signed by root CA, SANs: localhost, host.docker.internal, opensearch)
 *  - admin.pem + admin-key.pem (signed by root CA, CN=admin)
 *  - connect-client.pem + connect-client-key.pem (signed by root CA, CN=connect-client)
 *  - keystore.jks (connect-client cert+key, password=changeit)
 *  - truststore.jks (root-ca cert, password=changeit)
 *
 * All files are written to [[pkiDir]]. Results are cached per JVM run.
 */
class SecurityPkiFixture(val pkiDir: Path) extends StrictLogging {

  val keystorePath:   Path = pkiDir.resolve("keystore.jks")
  val truststorePath: Path = pkiDir.resolve("truststore.jks")
  val rootCaPem:      Path = pkiDir.resolve("root-ca.pem")
  val rootCaKeyPem:   Path = pkiDir.resolve("root-ca-key.pem")
  val nodePem:        Path = pkiDir.resolve("node.pem")
  val nodeKeyPem:     Path = pkiDir.resolve("node-key.pem")
  val adminPem:       Path = pkiDir.resolve("admin.pem")
  val adminKeyPem:    Path = pkiDir.resolve("admin-key.pem")
  val clientPem:      Path = pkiDir.resolve("connect-client.pem")
  val clientKeyPem:   Path = pkiDir.resolve("connect-client-key.pem")

  /** Generate all PKI material. Idempotent if already generated. */
  def generate(networkAlias: String = "opensearch"): Unit = {
    if (rootCaPem.toFile.exists()) {
      logger.info(s"SecurityPkiFixture: PKI already generated at $pkiDir, reusing")
      return
    }
    logger.info(s"SecurityPkiFixture: generating PKI in $pkiDir")

    // Root CA: generate a self-signed CA key+cert using keytool
    runKeytool(
      "-genkeypair",
      "-alias",
      "root-ca",
      "-keyalg",
      "RSA",
      "-keysize",
      "2048",
      "-validity",
      "1",
      "-keystore",
      pkiDir.resolve("root-ca.jks").toString,
      "-storepass",
      "changeit",
      "-keypass",
      "changeit",
      "-dname",
      "CN=root-ca,O=lenses,L=test,C=GB",
      "-ext",
      "BasicConstraints:critical=ca:true",
    )
    exportKeytoolCert("root-ca.jks", "root-ca", rootCaPem)
    exportKeytoolKey("root-ca.jks", "root-ca", rootCaKeyPem)

    // Node cert signed by root CA
    generateSignedCert(
      alias      = "node",
      dname      = "CN=node,O=lenses,L=test,C=GB",
      san        = s"DNS:localhost,DNS:host.docker.internal,DNS:$networkAlias",
      outputCert = nodePem,
      outputKey  = nodeKeyPem,
    )

    // Admin cert signed by root CA
    generateSignedCert(
      alias      = "admin",
      dname      = "CN=admin,O=lenses,L=test,C=GB",
      san        = "DNS:localhost",
      outputCert = adminPem,
      outputKey  = adminKeyPem,
    )

    // Connect-client cert signed by root CA
    generateSignedCert(
      alias      = "connect-client",
      dname      = "CN=connect-client,O=lenses,L=test,C=GB",
      san        = "DNS:localhost",
      outputCert = clientPem,
      outputKey  = clientKeyPem,
    )

    // Build keystore.jks with connect-client cert+key
    runKeytool(
      "-importkeystore",
      "-srckeystore",
      pkiDir.resolve("connect-client.jks").toString,
      "-destkeystore",
      keystorePath.toString,
      "-srcstorepass",
      "changeit",
      "-deststorepass",
      "changeit",
      "-srcalias",
      "connect-client",
      "-destalias",
      "connect-client",
      "-noprompt",
    )

    // Build truststore.jks with root-ca cert
    runKeytool(
      "-importcert",
      "-alias",
      "root-ca",
      "-file",
      rootCaPem.toString,
      "-keystore",
      truststorePath.toString,
      "-storepass",
      "changeit",
      "-noprompt",
    )

    logger.info(s"SecurityPkiFixture: PKI generation complete in $pkiDir")
  }

  private def generateSignedCert(
    alias:      String,
    dname:      String,
    san:        String,
    outputCert: Path,
    outputKey:  Path,
  ): Unit = {
    val ks = pkiDir.resolve(s"$alias.jks")
    // generate key pair
    runKeytool(
      "-genkeypair",
      "-alias",
      alias,
      "-keyalg",
      "RSA",
      "-keysize",
      "2048",
      "-validity",
      "1",
      "-keystore",
      ks.toString,
      "-storepass",
      "changeit",
      "-keypass",
      "changeit",
      "-dname",
      dname,
      "-ext",
      s"SAN=$san",
    )
    // generate CSR
    val csrPath = pkiDir.resolve(s"$alias.csr")
    runKeytool(
      "-certreq",
      "-alias",
      alias,
      "-keystore",
      ks.toString,
      "-storepass",
      "changeit",
      "-file",
      csrPath.toString,
    )
    // sign with root CA
    val signedCert = pkiDir.resolve(s"$alias-signed.pem")
    runKeytool(
      "-gencert",
      "-alias",
      "root-ca",
      "-keystore",
      pkiDir.resolve("root-ca.jks").toString,
      "-storepass",
      "changeit",
      "-infile",
      csrPath.toString,
      "-outfile",
      signedCert.toString,
      "-validity",
      "1",
      "-ext",
      s"SAN=$san",
      "-rfc",
    )
    // import root CA cert into the alias keystore (chain trust)
    runKeytool(
      "-importcert",
      "-alias",
      "root-ca",
      "-file",
      rootCaPem.toString,
      "-keystore",
      ks.toString,
      "-storepass",
      "changeit",
      "-noprompt",
    )
    // import signed cert into alias keystore
    runKeytool(
      "-importcert",
      "-alias",
      alias,
      "-file",
      signedCert.toString,
      "-keystore",
      ks.toString,
      "-storepass",
      "changeit",
      "-noprompt",
    )
    // export signed cert as PEM
    exportKeytoolCert(s"$alias.jks", alias, outputCert)
    exportKeytoolKey(s"$alias.jks", alias, outputKey)
  }

  private def exportKeytoolCert(keystoreFile: String, alias: String, outputPem: Path): Unit =
    runKeytool(
      "-exportcert",
      "-alias",
      alias,
      "-keystore",
      pkiDir.resolve(keystoreFile).toString,
      "-storepass",
      "changeit",
      "-file",
      outputPem.toString,
      "-rfc",
    )

  private def exportKeytoolKey(keystoreFile: String, alias: String, outputPem: Path): Unit = {
    // keytool cannot export private keys directly; use PKCS12 export + openssl
    val p12 = pkiDir.resolve(s"$alias.p12")
    runKeytool(
      "-importkeystore",
      "-srckeystore",
      pkiDir.resolve(keystoreFile).toString,
      "-destkeystore",
      p12.toString,
      "-srcstorepass",
      "changeit",
      "-deststorepass",
      "changeit",
      "-srcalias",
      alias,
      "-destalias",
      alias,
      "-deststoretype",
      "pkcs12",
      "-noprompt",
    )
    // openssl to extract private key PEM
    runProcess(
      "openssl",
      "pkcs12",
      "-in",
      p12.toString,
      "-nocerts",
      "-nodes",
      "-passin",
      "pass:changeit",
      "-out",
      outputPem.toString,
    )
  }

  private def runKeytool(args: String*): Unit = {
    val keytool = s"${System.getProperty("java.home")}${File.separator}bin${File.separator}keytool"
    runProcess((keytool +: args): _*)
  }

  private def runProcess(cmd: String*): Unit = {
    logger.debug(s"SecurityPkiFixture: running: ${cmd.mkString(" ")}")
    val pb     = new ProcessBuilder(cmd: _*).directory(pkiDir.toFile).redirectErrorStream(true)
    val proc   = pb.start()
    val output = new String(proc.getInputStream.readAllBytes())
    val code   = proc.waitFor()
    if (code != 0) {
      throw new RuntimeException(
        s"SecurityPkiFixture: command failed (exit $code): ${cmd.mkString(" ")}\nOutput:\n$output",
      )
    }
  }
}

object SecurityPkiFixture {

  /**
   * Lazily initialised shared fixture for the JVM run.
   * Thread-safe via synchronized lazy val initialisation.
   */
  lazy val shared: SecurityPkiFixture = {
    val dir = Files.createTempDirectory("opensearch-pki-")
    val f   = new SecurityPkiFixture(dir)
    f.generate()
    f
  }
}
