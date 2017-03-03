/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.util.hadoop;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.impl.client.HttpClients;

import org.bouncycastle.asn1.x500.X500Name;

import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;

public abstract class CertFSFileSystem extends FileSystem {

  private Path _workingDir;
  private URI _uri;
  private KeyPair _keyPair;
  private static final Logger log = LoggerFactory.getLogger(CertFSFileSystem.class);

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    log.debug("init uri {}", uri.toString());
    setConf(conf);
    log.info("config_is:" + conf);
    log.info("CertFSFileSystem_config_for csr.ssl.bcstyle.x500name is " + conf.get("csr.ssl.bcstyle.x500name", "NON_EXISTING"));

    String host = uri.getHost();
    if (host == null) {
      throw new IOException("Incomplete Https URI, no host: "+ uri);
    }

    try {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance(CertFSConstants.KEY_ALGORITHM);//job config
      keyGen.initialize(CertFSConstants.KEY_SIZE);
      _keyPair = keyGen.generateKeyPair();
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Key generation cannot get the algorithm for " + CertFSConstants.KEY_ALGORITHM, e);
    }

    _uri = uri;

    _workingDir = getHomeDirectory();
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return <code>certfs</code>
   */
  @Override
  public String getScheme() {
    return _uri.getScheme();
  }

  @Override
  public URI getUri() {
    return _uri;
  }

  /**
   * convert certfs URI to https URI
   * @param certFsUri certFS URI where the CSR should be sent to
   * @param excludesQuery whether or not to exclude the query params
   * @return https URI
   * @throws CertFSException when cannot construct https URI
   */
  private URI createHttpsUri(final URI certFsUri, final boolean excludesQuery) throws CertFSException{
    String certFsScheme = certFsUri.getScheme();
    if (!CertFSConstants.CERTFS_URI_SCHEME.equalsIgnoreCase(certFsScheme)) {
      throw new CertFSException("Invalid scheme for certFsUri: " + certFsUri);
    }
    try {
      String query = excludesQuery ? null : certFsUri.getQuery();
      URI httpsUri = new URI(
          CertFSConstants.HTTPS_URI_SCHEME,
          certFsUri.getUserInfo(),
          certFsUri.getHost(),
          certFsUri.getPort(),
          certFsUri.getPath(),
          query,
          certFsUri.getFragment()
      );
      return httpsUri;

    } catch (URISyntaxException e) {
      throw new CertFSException("cannot create the corresponding https URI due to syntax error for " + certFsUri, e);
    }
  }

  /**
   * get bcstyle x500name from config and replace the placeholder (such as __HOSTNAME__) if needed
   * @return bcstyle X500Name in the format of string, e.g. "CN=myhost.com, OU=Security, O=LinkedIn, L=Mountain View, ST=California, C=US"
   * @throws CertFSException when no valid bcstyle X500Name
   * @throws IOException when generating hostname for bcstyle x500Name
   */
  private String getBcstyleX500Name() throws CertFSException, IOException {
    // sample of raw bcstyle x500name
    // "CN=__HOSTNAME__, OU=Security, O=LinkedIn, L=Mountain View, ST=California, C=US"
    String bcstyleX500NameRaw = getConf().get(CertFSConstants.BCSTYLE_X500NAME);
    if (null == bcstyleX500NameRaw) {
      throw new CertFSException("no csr.ssl.bcstyle.x500name configured");
    }
    String bcstyleX500Name;
    if (bcstyleX500NameRaw.indexOf(CertFSConstants.HOSTNAME_PLACEHOLDER) >=0) {//contains placeholder
      bcstyleX500Name = bcstyleX500NameRaw.replace(CertFSConstants.HOSTNAME_PLACEHOLDER, getHostName());
    } else {
      bcstyleX500Name = bcstyleX500NameRaw;
    }
    return bcstyleX500Name;
  }

  /**
   * create the certificate signing request with the giving {@link BCStyle} {@link X500Name}
   * @param bcstyleX500Name in String format, e.g."CN=hostname.com, OU=Security, O=LinkedIn, L=Mountain View, ST=California, C=US"
   * @return Certificate signing request in string format
   * @throws CertFSException certificate local signing issue
   * @throws IOException reader/writer related exception
   */
  private String createCSR(final String bcstyleX500Name) throws CertFSException, IOException {
    X500Name name = new X500Name(bcstyleX500Name);
    PKCS10CertificationRequestBuilder pkcs10CertificationRequestBuilder = new JcaPKCS10CertificationRequestBuilder(
        name, _keyPair.getPublic()
    );
    JcaContentSignerBuilder contentSignerBuilder = new JcaContentSignerBuilder(CertFSConstants.SIGNATURE_ALGORITHM);
    ContentSigner signer;
    try {
      signer = contentSignerBuilder.build(_keyPair.getPrivate());
    } catch (OperatorCreationException e) {
      throw new CertFSException("cannot create content signer for csr with private key");
    }
    PKCS10CertificationRequest csr = pkcs10CertificationRequestBuilder.build(signer);

    StringWriter sw = new StringWriter();
    JcaPEMWriter pemWriter = new JcaPEMWriter(sw);
    pemWriter.writeObject(csr);
    pemWriter.close();
    return sw.toString();
  }

  /**
   * construct and return the private key in pem format from the keyPair
   * @return String
   */
  private String getPrivateKeyPem() {
    // The stream is already base64 encoded.
    byte[] privateKey = _keyPair.getPrivate().getEncoded();

    Base64.Encoder encoder = Base64.getMimeEncoder(64, new byte[] {'\r', '\n'});
    String privateKeyPem = String.format(
        "%s\r\n%s\r\n%s",
        "-----BEGIN RSA PRIVATE KEY-----",
        encoder.encodeToString(privateKey),
        "-----END RSA PRIVATE KEY-----"
    );
    return privateKeyPem;
  }

  /**
   * get https client based on the configuration settings
   * @return HttpClient
   * @throws CertFSException when there is any exception when creating httpClient for SSL
   */
  private HttpClient getHttpsClient() throws CertFSException {
    String trustStoreType = getConf().get(CertFSConstants.TRUSTSTORE_TYPE, "JKS");
    String keyStoreType = getConf().get(CertFSConstants.KEYSTORE_TYPE, "PKCS12");
    try {
      KeyStore trustStore = KeyStore.getInstance(trustStoreType);
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);

      String trustStoreLocation = getConf().get(CertFSConstants.TRUSTSTORE_LOCATION, "");
      log.info("trust store location is: {}", trustStoreLocation);
      String trustStorePassword = getConf().get(CertFSConstants.TRUSTSTORE_PASSWORD, "");
      trustStore.load(new FileInputStream(trustStoreLocation), trustStorePassword.toCharArray());
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      TrustManager[] tms = tmf.getTrustManagers();

      String keyStoreLocation = getConf().get(CertFSConstants.KEYSTORE_LOCATION, "");
      log.info("key store location is: {}", keyStoreLocation);

      String keyStorePassword = getConf().get(CertFSConstants.KEYSTORE_PASSWORD, "");
      String keyPassword = getConf().get(CertFSConstants.KEY_PASSWORD, "");
      keyStore.load(new FileInputStream(keyStoreLocation), keyStorePassword.toCharArray());
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, keyPassword.toCharArray());
      KeyManager[] kms = kmf.getKeyManagers();

      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(kms, tms, new SecureRandom());

      return HttpClients.custom().setSslcontext(sslContext).build();
    } catch (Exception e) {
      throw new CertFSException("Exception on getting https client", e);
    }
  }

  /**
   * send https POST request to URI
   * @param httpsUri https uri the request is sent to
   * @param payloadStr payload json in the https body
   * @return response entity from the URI service
   * @throws CertFSException
   * @throws IOException
   */
  private String sendRequest(final URI httpsUri, final String payloadStr) throws CertFSException, IOException {
    log.info("httpsUri: {} with payloadStr: {}", httpsUri, payloadStr);
    HttpClient client = getHttpsClient();

    HttpPost httpPost = new HttpPost(httpsUri.toString());
    StringEntity params = new StringEntity(payloadStr);

    httpPost.addHeader("content-type", "application/json; charset=UTF-8"); //hard coded here
    httpPost.setEntity(params);
    HttpResponse response = client.execute(httpPost);
    if (response.getStatusLine().getStatusCode() != CertFSConstants.HTTP_STATUS_OK){
      throw new IOException("failed on certificate fetching, status code: " + response.getStatusLine().getStatusCode());
    }
    return EntityUtils.toString(response.getEntity());
  }

  /**
   * get the current host name
   * @return hostname in format of string
   * @throws IOException when the hostname is unknown
   */
  private String getHostName() throws IOException {
    String host = "";
    try {
      java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
      host = localMachine.getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new IOException("Cannot get the host name for X500Name");
    }

    log.info("localMachineHost:"+host);
    return host;
  }

  /**
   * get the certificate from the certfs path
   * @param f the certfs path for sending the certificate signing request (CSR)
   * @return the response as a string from the CSR service
   * @throws CertFSException certfs related exception
   * @throws IOException https related exception
   */
  public String getCertificate(final Path f) throws CertFSException, IOException {

    URI rawUri = f.toUri();
    String rawUriDecodedStr = URLDecoder.decode(rawUri.toString(), "UTF-8");

    URI decodedUri;
    try {
      decodedUri = new URI(rawUriDecodedStr);
    } catch (URISyntaxException e) {
      throw new CertFSException("URI syntax error when constructing uri from " + rawUriDecodedStr);
    }

    //extract the query params and put them in Post body payload later
    List<NameValuePair> params = URLEncodedUtils.parse(decodedUri, "UTF-8");
    log.info("parsing rawUriDecodedStr {}, and get query params {}, total {} query params", rawUriDecodedStr, params,
        params.size());
    //set excludesQuery=true to remove the query params from the httpsUri
    URI httpsUri = createHttpsUri(decodedUri, true);
    if (null == httpsUri) {
      throw new CertFSException("cert request path is not existing as httpsUri");
    }

    String bcstyleX500Name = getBcstyleX500Name();

    String csr = createCSR(bcstyleX500Name);

    String payloadStr = constructRequestPayload(params, csr);

    String cert_responses = sendRequest(httpsUri, payloadStr);
    log.info("cert response is: {}", cert_responses);

    if (cert_responses == null) {
      throw new CertFSException("null response for CSR request from CA");
    }
    return parseResponseForCert(cert_responses);
  }

  /**
   * Construct the http request post body payload for CSR request
   * This method can be overriden for customized payload construction
   * @param params addtional params put into the payload
   * @param csr certificate signing request
   * @return payload used for https post body
   * @throws IOException https related exception
   */
  public abstract String constructRequestPayload(final List<NameValuePair> params, final String csr) throws IOException;

  /**
   * Parse the response of the CSR request from the server
   * This method can be overriden for customized response parsing
   * @param response https response from server
   * @return the cert part from the response
   * @throws IOException https related exception
   */
  public abstract String parseResponseForCert(final String response) throws IOException;


  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path f, final int bufferSize)
      throws IOException {
    log.info("open certfs file {}", f);
    statistics.incrementReadOps(1);
    try {
      String certificate = getCertificate(f);
      String privateKeyPem = getPrivateKeyPem();

      // TODO: use security cert client lib to convert the certificate/key to expected key type
      // currently, output to pem format for testing localization for certificate/key
      String pem = certificate + "\r\n" + privateKeyPem;
      InputStream stream = new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8));
      FSDataInputStream fsStream = new FSDataInputStream(new CertFSInputStream(stream));
      return fsStream;
    } catch (Exception e) {
      throw new IOException("Error on fetching the certificate and creating keys.", e);
    }
  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    log.info("getting file status for CertFSFileSystem");
    int length = -1;
    boolean isDir = false;
    int blockReplication = 1;
    int blockSize = CertFSConstants.DEFAULT_BLOCK_SIZE;
    int modTime = 0;
    FileStatus fs = new FileStatus(length, isDir, blockReplication, blockSize, modTime, f);
    log.info("file status for {} is {}", f, fs);
    return fs;
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return false;
  }


  @Override
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    return null;

  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException {
    return null;
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    _workingDir = new_dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return _workingDir;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

}
