/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.openssl.OpenSSLProvider;
import org.wildfly.openssl.SSL;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.logging.Level;


/**
 * Extension to use native OpenSSL library instead of JSE for better performance.
 */
public class SSLSocketFactoryEx extends SSLSocketFactory {

  /**
   * Defines which SSL implementation should be used for outbound connections.
   */
  public enum SSLChannelMode {
    /** Always use the native OpenSSL provider. */
    OpenSSL,
    /** Prefer the native OpenSSL provider, falling back to the JDK implementation on failure. */
    Default,
    /** Always use the JDK SSL implementation. */
    Default_JSE
  }

  private static SSLSocketFactoryEx instance = null;
  private static Object lock = new Object();
  private static final Logger log = LoggerFactory
      .getLogger("com.microsoft.azure.datalake.store.SSLSocketFactoryEx");
  private String userAgent;
  private SSLContext m_ctx;
  private String[] m_ciphers;
  private SSLChannelMode channelMode;

  /**
   * Returns a cached {@link SSLSocketFactoryEx} configured for the requested SSL channel mode.
   *
   * @param sslChannelMode SSL implementation preference.
   * @return shared socket factory instance.
   * @throws IOException when the SSL context cannot be initialized.
   */
  public static SSLSocketFactoryEx getDefaultFactory(SSLChannelMode sslChannelMode) throws IOException {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new SSLSocketFactoryEx(sslChannelMode);
          log.debug("SSLSocketFactoryEx created in " + sslChannelMode.name() + " mode");
        }
      }
    }
    return instance;
  }

  static {
    OpenSSLProvider.register();
  }

  /**
   * Creates a new socket factory honoring the specified SSL channel mode.
   *
   * @param channelMode SSL implementation preference.
   * @throws IOException when the SSL context cannot be initialized.
   */
  public SSLSocketFactoryEx(SSLChannelMode channelMode) throws IOException {
    this.channelMode = channelMode;
    try {
      initSSLSocketFactoryEx(null, null, null);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    } catch (KeyManagementException e) {
      throw new IOException(e);
    }

    Provider provider = m_ctx.getProvider();
    userAgent = provider.getName() + "-" + resolveProviderVersion(provider);
  }

  /**
   * Returns the user-agent string describing the active SSL provider.
   *
   * @return user-agent fragment identifying the SSL provider.
   */
  public String getUserAgent() {
    return userAgent;
  }

  public String[] getDefaultCipherSuites() {
    return m_ciphers;
  }

  public String[] getSupportedCipherSuites() {
    return m_ciphers;
  }

  public Socket createSocket() throws IOException {
    SSLSocketFactory factory = m_ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory.createSocket();
    configureSocket(ss);
    return ss;
  }

  @Override
  public Socket createSocket(Socket s, String host, int port, boolean autoClose)
      throws IOException {
    SSLSocketFactory factory = m_ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory.createSocket(s, host, port, autoClose);

    configureSocket(ss);
    return ss;
  }

  @Override
  public Socket createSocket(InetAddress address, int port,
                             InetAddress localAddress, int localPort) throws IOException {
    SSLSocketFactory factory = m_ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory
        .createSocket(address, port, localAddress, localPort);

    configureSocket(ss);
    return ss;
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost,
                             int localPort) throws IOException {
    SSLSocketFactory factory = m_ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory
        .createSocket(host, port, localHost, localPort);

    configureSocket(ss);

    return ss;
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    SSLSocketFactory factory = m_ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory.createSocket(host, port);

    configureSocket(ss);

    return ss;
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    SSLSocketFactory factory = m_ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory.createSocket(host, port);

    configureSocket(ss);

    return ss;
  }

  private void configureSocket(SSLSocket ss) throws SocketException {
    ss.setEnabledCipherSuites(m_ciphers);
  }

  private void initSSLSocketFactoryEx(KeyManager[] km, TrustManager[] tm,
                                      SecureRandom random)
      throws NoSuchAlgorithmException, KeyManagementException, IOException {

    switch (channelMode) {
    case Default:
      try {
        java.util.logging.Logger.getLogger(SSL.class.getName()).setLevel(Level.WARNING);
        m_ctx = SSLContext.getInstance("openssl.TLS");
        m_ctx.init(km, tm, random);
        channelMode = SSLChannelMode.OpenSSL;
      } catch (NoSuchAlgorithmException e) {
        log.info("Failed to load OpenSSL library. Fallback to default JSE. ", e);
        m_ctx = SSLContext.getDefault();
        channelMode = SSLChannelMode.Default_JSE;
      }
      break;

    case Default_JSE:
      m_ctx = SSLContext.getDefault();
      break;

    case OpenSSL:
      m_ctx = SSLContext.getInstance("openssl.TLS");
      m_ctx.init(km, tm, random);
      break;
    }

    // Get list of supported cipher suits from the SSL factory.
    SSLSocketFactory factory = m_ctx.getSocketFactory();
    String[] defaultCiphers = factory.getSupportedCipherSuites();
    String version = System.getProperty("java.version");

    m_ciphers = (channelMode == SSLChannelMode.Default_JSE && version.startsWith("1.8")) ?
         alterCipherList(defaultCiphers) :  defaultCiphers;
  }

  private String[] alterCipherList(String[] defaultCiphers) {

    ArrayList<String> preferredSuits = new ArrayList<>();

    // Remove GCM mode based ciphers from the supported list.
    for (int i = 0; i < defaultCiphers.length; i++) {
      if (defaultCiphers[i].contains("_GCM_")) {
        log.info("Removed Cipher - " + defaultCiphers[i]);
      } else {
        preferredSuits.add(defaultCiphers[i]);
      }
    }

    m_ciphers = preferredSuits.toArray(new String[0]);
    return m_ciphers;
  }

  private String resolveProviderVersion(Provider provider) {
    try {
      Method versionMethod = Provider.class.getMethod("getVersionStr");
      Object version = versionMethod.invoke(provider);
      if (version != null) {
        return version.toString();
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignored) {
      // fall back to other mechanisms when version string is unavailable
    }

    Object versionProperty = provider.get("ProviderVersion");
    if (versionProperty != null) {
      return versionProperty.toString();
    }

    return provider.getInfo();
  }
}
