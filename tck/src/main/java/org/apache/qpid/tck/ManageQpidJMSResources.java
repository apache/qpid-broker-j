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

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Used pre/post-integration-test to create/delete JMS resources required for the TCK run.
 */
public class ManageQpidJMSResources
{
    private static final String APPLICATION_JSON = "application/json; charset=UTF-8";
    private static final String AUTHORIZATION = "Authorization";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageQpidJMSResources.class);

    private static final TypeReference<List<Map<String, Object>>> VALUE_TYPE_REF =
            new TypeReference<List<Map<String, Object>>>()
            {
            };
    private static final String RESOURCES_JSON = "/resources.json";

    private final String _virtualhostnode;
    private final String _virtualhost;
    private final URI _management;
    private final HttpClient _httpClient;
    private final String _authorization;
    private final String _queueApiUrl;
    private final String _queueApiClearQueueUrl;
    private final String _topicApiUrl;
    private final ObjectMapper _objectMapper;

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

    public ManageQpidJMSResources() throws URISyntaxException
    {
        _objectMapper = JsonMapper.builder()
                .configure(JsonReadFeature.ALLOW_JAVA_COMMENTS, true)
                .build();

        final String managementUser = System.getProperty("tck.management-username");
        final String managementPassword = System.getProperty("tck.management-password");

        _virtualhostnode = System.getProperty("tck.broker-virtualhostnode", "default");
        _virtualhost = System.getProperty("tck.broker-virtualhost", "default");

        _management = new URI(System.getProperty("tck.management-url", "http://localhost:8080"));
        _httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .proxy(HttpClient.Builder.NO_PROXY)
                .build();
        _authorization = getAuthorization(managementUser, managementPassword);
        _queueApiUrl = System.getProperty("tck.management-api-queue", "/api/latest/queue/%s/%s/%s");
        _queueApiClearQueueUrl = System.getProperty("tck.management-api-queue-clear", "/api/latest/queue/%s/%s/%s/clearQueue");
        _topicApiUrl = System.getProperty("tck.management-api-topic", "/api/latest/exchange/%s/%s/%s");
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
                                : Map.of();
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
        management("PUT", String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name), arguments);
    }

    private void managementClearQueue(final String name) throws IOException
    {
        final String path = String.format(_queueApiClearQueueUrl, _virtualhostnode, _virtualhost, name);
        management("POST", path, Map.of());
    }

    private void managementCreateExchange(final String name, final Map<String, Object> arguments) throws IOException
    {
        final String path = String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name);
        management("PUT", path, arguments);
    }

    private void managementDeleteQueue(final String name)
    {
        final String path = String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name);
        final HttpRequest request = newManagementRequest(path)
                .DELETE()
                .build();
        executeManagement(request);
    }

    private void managementDeleteExchange(final String name)
    {
        final String path = String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name);
        final HttpRequest request = newManagementRequest(path)
                .DELETE()
                .build();
        executeManagement(request);
    }

    private void management(final String method, final String path, final Object obj) throws IOException
    {
        final String body = _objectMapper.writeValueAsString(obj);
        final HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8);
        final HttpRequest request = newManagementRequest(path)
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .method(method, bodyPublisher)
                .build();

        final int statusCode = executeManagement(request);
        if (statusCode != HTTP_OK && statusCode != HTTP_CREATED)
        {
            throw new RuntimeException(String.format("Failed : HTTP error code : %d", statusCode));
        }
    }

    private HttpRequest.Builder newManagementRequest(final String path)
    {
        return HttpRequest.newBuilder(_management.resolve(path)).header(AUTHORIZATION, _authorization);
    }

    private int executeManagement(final HttpRequest httpRequest)
    {
        try
        {
            final HttpResponse<Void> response = _httpClient.send(httpRequest, HttpResponse.BodyHandlers.discarding());
            return response.statusCode();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static String getAuthorization(final String managementUser, final String managementPassword)
    {
        final String credentials = managementUser + ":" + managementPassword;
        final String encodedCredentials = Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encodedCredentials;
    }
}
