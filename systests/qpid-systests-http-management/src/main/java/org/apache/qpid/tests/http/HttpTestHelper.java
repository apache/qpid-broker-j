/*
 *
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
 *
 */
package org.apache.qpid.tests.http;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.DatatypeConverter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class HttpTestHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTestHelper.class);

    private static final TypeReference<List<LinkedHashMap<String, Object>>> TYPE_LIST_OF_LINKED_HASH_MAPS = new TypeReference<List<LinkedHashMap<String, Object>>>()
    {
    };

    private static final TypeReference<LinkedHashMap<String, Object>> TYPE_LINKED_HASH_MAPS = new TypeReference<LinkedHashMap<String, Object>>()
    {
    };

    private static final String API_BASE = "/api/latest/";
    private final BrokerAdmin _admin;
    private final int _httpPort;
    private String _username;
    private String _password;
    private final String _requestHostName;
    private final int _connectTimeout = Integer.getInteger("qpid.resttest_connection_timeout", 30000);

    private String _acceptEncoding;
    private boolean _tls = false;

    private KeyStore _keyStore;
    private String _keyStorePassword;

    public HttpTestHelper(final BrokerAdmin admin)
    {
        this(admin, null);
    }

    public HttpTestHelper(BrokerAdmin admin, final String requestHostName)
    {
        this(admin, requestHostName, admin.getBrokerAddress(BrokerAdmin.PortType.HTTP).getPort());
    }

    public HttpTestHelper(BrokerAdmin admin, final String requestHostName, final int httpPort)
    {
        _admin = admin;
        _httpPort = httpPort;
        _username = admin.getValidUsername();
        _password = admin.getValidPassword();
        _requestHostName = requestHostName;
    }

    public void setTls(final boolean tls)
    {
        _tls = tls;
    }

    private int getHttpPort()
    {
        return _httpPort;
    }

    private String getHostName()
    {
        return "localhost";
    }

    private String getManagementURL()
    {
        return (_tls ? "https" : "http") + "://" + getHostName() + ":" + getHttpPort();
    }

    private URL getManagementURL(String path) throws MalformedURLException
    {
        return new URL(getManagementURL() + path);
    }

    public HttpURLConnection openManagementConnection(String path, String method) throws IOException
    {
        if (!path.startsWith("/"))
        {
            path = API_BASE + path;
        }
        URL url = getManagementURL(path);
        HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
        if (httpCon instanceof HttpsURLConnection)
        {
            HttpsURLConnection httpsCon = (HttpsURLConnection) httpCon;
            try
            {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                TrustManager[] trustAllCerts = new TrustManager[] {new TrustAllTrustManager()};

                KeyManager[] keyManagers = null;
                if (_keyStore != null)
                {
                    char[] keyStoreCharPassword = _keyStorePassword == null ? null : _keyStorePassword.toCharArray();
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(_keyStore, keyStoreCharPassword);
                    keyManagers = kmf.getKeyManagers();
                }
                sslContext.init(keyManagers, trustAllCerts, null);
                httpsCon.setSSLSocketFactory(sslContext.getSocketFactory());
                httpsCon.setHostnameVerifier((s, sslSession) -> true);
            }
            catch (KeyStoreException | UnrecoverableKeyException | KeyManagementException | NoSuchAlgorithmException e)
            {
                throw new RuntimeException(e);
            }
        }
        httpCon.setConnectTimeout(_connectTimeout);
        if (_requestHostName != null)
        {
            httpCon.setRequestProperty("Host", _requestHostName);
        }

        if(_username != null)
        {
            String encoded = DatatypeConverter.printBase64Binary((_username + ":" + _password).getBytes(UTF_8));
            httpCon.setRequestProperty("Authorization", "Basic " + encoded);
        }

        if (_acceptEncoding != null && !"".equals(_acceptEncoding))
        {
            httpCon.setRequestProperty("Accept-Encoding", _acceptEncoding);
        }

        httpCon.setDoOutput(true);
        httpCon.setRequestMethod(method);
        return httpCon;
    }

    public List<Map<String, Object>> readJsonResponseAsList(HttpURLConnection connection) throws IOException
    {
        byte[] data = readConnectionInputStream(connection);
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> providedObject = mapper.readValue(new ByteArrayInputStream(data), TYPE_LIST_OF_LINKED_HASH_MAPS);
        return providedObject;
    }

    public Map<String, Object> readJsonResponseAsMap(HttpURLConnection connection) throws IOException
    {
        byte[] data = readConnectionInputStream(connection);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> providedObject = mapper.readValue(new ByteArrayInputStream(data), TYPE_LINKED_HASH_MAPS);
        return providedObject;
    }

    public <T> T readJsonResponse(HttpURLConnection connection, Class<T> valueType) throws IOException
    {
        byte[] data = readConnectionInputStream(connection);

        ObjectMapper mapper = new ObjectMapper();

        return mapper.readValue(new ByteArrayInputStream(data), valueType);
    }

    private byte[] readConnectionInputStream(HttpURLConnection connection) throws IOException
    {
        InputStream is = connection.getInputStream();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len = -1;
        while ((len = is.read(buffer)) != -1)
        {
            baos.write(buffer, 0, len);
        }
        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace("RESPONSE:" + new String(baos.toByteArray(), UTF_8));
        }
        return baos.toByteArray();
    }

    private void writeJsonRequest(HttpURLConnection connection, Object data) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(connection.getOutputStream(), data);
    }

    public Map<String, Object> find(String name, Object value, List<Map<String, Object>> data)
    {
        if (data == null)
        {
            return null;
        }

        for (Map<String, Object> map : data)
        {
            Object mapValue = map.get(name);
            if (value.equals(mapValue))
            {
                return map;
            }
        }
        return null;
    }

    public Map<String, Object> find(Map<String, Object> searchAttributes, List<Map<String, Object>> data)
    {
        for (Map<String, Object> map : data)
        {
            boolean equals = true;
            for (Map.Entry<String, Object> entry : searchAttributes.entrySet())
            {
                Object mapValue = map.get(entry.getKey());
                if (!entry.getValue().equals(mapValue))
                {
                    equals = false;
                    break;
                }
            }
            if (equals)
            {
                return map;
            }
        }
        return null;
    }

    public Map<String, Object> getJsonAsSingletonList(String path) throws IOException
    {
        List<Map<String, Object>> response = getJsonAsList(path);

        Assert.assertNotNull("Response cannot be null", response);
        Assert.assertEquals("Unexpected response from " + path, 1, response.size());
        return response.get(0);
    }

    public Map<String, Object> postDataToPathAndGetObject(String path, Map<String, Object> data) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(path, "POST");
        connection.connect();
        writeJsonRequest(connection, data);
        Map<String, Object> response = readJsonResponseAsMap(connection);
        return response;
    }

    public List<Map<String, Object>> getJsonAsList(String path) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(path, "GET");
        connection.connect();
        List<Map<String, Object>> response = readJsonResponseAsList(connection);
        return response;
    }

    public List<Object> getJsonAsSimpleList(String path) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(path, "GET");
        connection.connect();
        byte[] data = readConnectionInputStream(connection);
        ObjectMapper mapper = new ObjectMapper();
        List<Object> providedObject = mapper.readValue(new ByteArrayInputStream(data), new TypeReference<List<Object>>()
        {
        });
        return providedObject;
    }

    public Map<String, Object> getJsonAsMap(String path) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(path, "GET");
        try
        {
            connection.connect();
            Map<String, Object> response = readJsonResponseAsMap(connection);
            return response;
        }
        finally
        {
            connection.disconnect();
        }
    }

    public <T> T getJson(String path, final TypeReference valueTypeRef, int expectedResponseCode) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(path, "GET");
        connection.connect();
        int responseCode = connection.getResponseCode();
        Assert.assertEquals(String.format("Unexpected response code from : %s", path), expectedResponseCode, responseCode);

        return new ObjectMapper().readValue(new ByteArrayInputStream(readConnectionInputStream(connection)), valueTypeRef);
    }

    public <T> T postJson(String path, final Object data, final TypeReference valueTypeRef, int expectedResponseCode) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(path, "POST");
        connection.connect();
        writeJsonRequest(connection, data);
        int responseCode = connection.getResponseCode();
        Assert.assertEquals(String.format("Unexpected response code from : %s", path), expectedResponseCode, responseCode);

        return new ObjectMapper().readValue(new ByteArrayInputStream(readConnectionInputStream(connection)), valueTypeRef);
    }


    public int submitRequest(String url, String method, Object data) throws IOException
    {
        return submitRequest(url, method, data, null);
    }

    public int submitRequest(String url, String method, Object data, Map<String, List<String>> responseHeadersToCapture) throws IOException
    {
        LOGGER.debug("HttpURLConnection : open  : {} : {}", method, url);
        RuntimeException ex = null;
        int responseCode = -1;
        try
        {
            HttpURLConnection connection = openManagementConnection(url, method);
            try
            {
                if (data != null)
                {
                    writeJsonRequest(connection, data);
                }
                responseCode = connection.getResponseCode();
                if (responseHeadersToCapture != null)
                {
                    responseHeadersToCapture.putAll(connection.getHeaderFields());
                }
                return responseCode;
            }
            catch (RuntimeException e)
            {
                ex = e;
                throw e;
            }
            finally
            {
                connection.disconnect();
            }
        }
        finally
        {
            LOGGER.debug("HttpURLConnection : close : {} : {} : {}", method, responseCode, url, ex);
        }
    }

    public int submitRequest(String url, String method) throws IOException
    {
        return submitRequest(url, method, (byte[])null);
    }

    public void submitRequest(String url, String method, Object data, int expectedResponseCode) throws IOException
    {
        Map<String, List<String>> headers = new HashMap<>();
        int responseCode = submitRequest(url, method, data, headers);
        Assert.assertEquals("Unexpected response code from " + method + " " + url , expectedResponseCode, responseCode);
        if (expectedResponseCode == 201)
        {
            List<String> location = headers.get("Location");
            Assert.assertTrue("Location is not returned by REST create request", location != null && location.size() == 1);
        }
    }

    public void submitRequest(String url, String method, int expectedResponseCode) throws IOException
    {
        submitRequest(url, method, null, expectedResponseCode);
    }

    public int submitRequest(String url, String method, byte[] parameters) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(url, method);
        if (parameters != null)
        {
            OutputStream os = connection.getOutputStream();
            os.write(parameters);
            os.flush();
        }
        int responseCode = connection.getResponseCode();
        connection.disconnect();
        return responseCode;
    }

    public byte[] getBytes(String path) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(path, "GET");
        connection.connect();
        return readConnectionInputStream(connection);
    }

    public String encode(String value, String encoding) throws UnsupportedEncodingException
    {
        return URLEncoder.encode(value, encoding).replace("+", "%20");
    }

    public String getAcceptEncoding()
    {
        return _acceptEncoding;
    }

    public void setAcceptEncoding(String acceptEncoding)
    {
        _acceptEncoding = acceptEncoding;
    }

    public void setKeyStore(final String keystore, final String password) throws Exception
    {
        _keyStorePassword = password;

        if (keystore != null)
        {
            try
            {
                URL ks = new URL(keystore);
                _keyStore = SSLUtil.getInitializedKeyStore(ks, password, KeyStore.getDefaultType());
            }
            catch (MalformedURLException e)
            {
                _keyStore = SSLUtil.getInitializedKeyStore(keystore, password, KeyStore.getDefaultType());
            }
        }
        else
        {
            _keyStore = null;
        }
    }

    public void setPassword(final String password)
    {
        _password = password;
    }

    public void setUserName(final String username)
    {
        _username = username;
    }

    private static class TrustAllTrustManager implements X509TrustManager
    {
        public X509Certificate[] getAcceptedIssuers()
        {
            X509Certificate[] issuers = new X509Certificate[0];
            return issuers;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType)
        {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType)
        {
        }
    }
}
