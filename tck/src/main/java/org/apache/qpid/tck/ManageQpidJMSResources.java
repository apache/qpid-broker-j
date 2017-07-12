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
package org.apache.qpid.tck;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used pre/post-integration-test to create/delete JMS resources required for the TCK run.
 */
public class ManageQpidJMSResources
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageQpidJMSResources.class);

    private static final TypeReference<List<Map<String, Object>>> VALUE_TYPE_REF =
            new TypeReference<List<Map<String, Object>>>()
            {
            };
    private static final String RESOURCES_JSON = "/resources.json";

    private final String _virtualhostnode;
    private final String _virtualhost;
    private final HttpHost _management;
    private final String _queueApiUrl;
    private final String _queueApiClearQueueUrl;
    private final String _topicApiUrl;
    private final ObjectMapper _objectMapper;
    private final CredentialsProvider _credentialsProvider;
    private final HttpClientContext _httpClientContext;

    private enum NodeType
    {
        QUEUE, EXCHANGE
    }

    public static void main(String[] argv) throws Exception
    {
        final ManageQpidJMSResources manageQpidJMSResources = new ManageQpidJMSResources();

        if (argv.length > 0 && "--delete".equals(argv[0]))
        {
            manageQpidJMSResources.deleteResources();
        }
        else
        {
            manageQpidJMSResources.createResources();

        }
    }

    public ManageQpidJMSResources()
    {
        _objectMapper = new ObjectMapper();
        _objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        final String managementUser = System.getProperty("tck.management-username");
        final String managementPassword = System.getProperty("tck.management-password");

        _virtualhostnode = System.getProperty("tck.broker-virtualhostnode", "default");
        _virtualhost = System.getProperty("tck.broker-virtualhost", "default");

        _management = HttpHost.create(System.getProperty("tck.management-url", "http://localhost:8080"));
        _queueApiUrl = System.getProperty("tck.management-api-queue", "/api/latest/queue/%s/%s/%s");
        _queueApiClearQueueUrl = System.getProperty("tck.management-api-queue-clear", "/api/latest/queue/%s/%s/%s/clearQueue");
        _topicApiUrl = System.getProperty("tck.management-api-topic", "/api/latest/exchange/%s/%s/%s");

        _credentialsProvider = getCredentialsProvider(managementUser, managementPassword);
        _httpClientContext = getHttpClientContext(_management);
    }

    private void createResources() throws IOException
    {

        try (InputStream resourceStream = getClass().getResourceAsStream(RESOURCES_JSON))
        {
            if (resourceStream == null)
            {
                throw new IOException(String.format("Cannot find '%s' on the classpath", RESOURCES_JSON));
            }

            List<Map<String, Object>> resourceDefs = _objectMapper.readValue(resourceStream, VALUE_TYPE_REF);

            for (Map<String, Object> resourceDef : resourceDefs)
            {
                String name = (String) resourceDef.get("name");
                NodeType type = NodeType.valueOf(String.valueOf(resourceDef.get("nodeType")));
                Map<String, Object> arguments =
                        resourceDef.containsKey("arguments") ? (Map<String, Object>) resourceDef.get("arguments")
                                : Collections.<String, Object>emptyMap();
                LOGGER.info("Creating {} type {}", name, type);
                switch (type)
                {
                    case QUEUE:
                        managementCreateQueue(name, arguments);
                        // Clear queue just in case it existed already
                        managementClearQueue(name);
                        break;
                    case EXCHANGE:
                        managementCreateExchange(name, arguments);
                        break;
                    default:
                        throw new RuntimeException(String.format("Unexpected type : %s", type));
                }
            }
        }
    }

    private void deleteResources() throws IOException
    {

        try (InputStream resourceStream = getClass().getResourceAsStream(RESOURCES_JSON))
        {
            if (resourceStream == null)
            {
                throw new IOException(String.format("Cannot find '%s' on the classpath", RESOURCES_JSON));
            }

            List<Map<String, Object>> resourceDefs = _objectMapper.readValue(resourceStream, VALUE_TYPE_REF);

            for (Map<String, Object> resourceDef : resourceDefs)
            {
                String name = (String) resourceDef.get("name");
                NodeType type = NodeType.valueOf(String.valueOf(resourceDef.get("nodeType")));
                LOGGER.info("Deleting {} type {}", name, type);
                switch (type)
                {
                    case QUEUE:
                        managementDeleteQueue(name);
                        break;
                    case EXCHANGE:
                        managementDeleteExchange(name);
                        break;
                    default:
                        throw new RuntimeException(String.format("Unexpected type : %s", type));
                }
            }
        }
    }

    private void managementCreateQueue(final String name, final Map<String, Object> arguments) throws IOException
    {
        HttpPut put = new HttpPut(String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name));

        management(put, arguments);
    }

    private void managementClearQueue(final String name) throws IOException
    {
        HttpPost post = new HttpPost(String.format(_queueApiClearQueueUrl, _virtualhostnode, _virtualhost, name));

        management(post, Collections.emptyMap());
    }

    private void managementCreateExchange(final String name, final Map<String, Object> arguments) throws IOException
    {
        HttpPut put = new HttpPut(String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name));

        management(put, arguments);
    }
    private void managementDeleteQueue(final String name)
    {
        HttpDelete delete = new HttpDelete(String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name));
        executeManagement(delete);
    }

    private void managementDeleteExchange(final String name)
    {
        HttpDelete delete = new HttpDelete(String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name));
        executeManagement(delete);
    }

    private void management(final HttpEntityEnclosingRequestBase request, final Object obj) throws IOException
    {
        StringEntity input = new StringEntity(_objectMapper.writeValueAsString(obj), StandardCharsets.UTF_8);
        input.setContentType("application/json");
        request.setEntity(input);

        int statusCode = executeManagement(request);
        if (statusCode != 200 && statusCode != 201)
        {
            throw new RuntimeException(String.format("Failed : HTTP error code : %d", statusCode));
        }
    }

    private int executeManagement(final HttpRequest httpRequest)
    {

        try(CloseableHttpClient httpClient = HttpClients.custom()
                                                        .setDefaultCredentialsProvider(_credentialsProvider)
                                                        .build())
        {
            try (CloseableHttpResponse response = httpClient.execute(_management, httpRequest, _httpClientContext))
            {
                return response.getStatusLine().getStatusCode();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private HttpClientContext getHttpClientContext(final HttpHost management)
    {
        final BasicAuthCache authCache = new BasicAuthCache();
        authCache.put(management, new BasicScheme());
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setAuthCache(authCache);
        return localContext;
    }

    private CredentialsProvider getCredentialsProvider(final String managementUser, final String managementPassword)
    {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(managementUser, managementPassword));
        return credentialsProvider;
    }

}
