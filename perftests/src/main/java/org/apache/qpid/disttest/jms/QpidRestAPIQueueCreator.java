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
 *
 */
package org.apache.qpid.disttest.jms;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.QueueConfig;
import org.apache.qpid.disttest.json.ObjectMapperFactory;

/**
 * Assumes Basic-Auth is enabled
 */
public class QpidRestAPIQueueCreator implements QueueCreator
{
    private static final String APPLICATION_JSON = "application/json; charset=UTF-8";
    private static final String AUTHORIZATION = "Authorization";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidRestAPIQueueCreator.class);
    private static final int DRAIN_POLL_TIMEOUT = Integer.getInteger(QUEUE_CREATOR_DRAIN_POLL_TIMEOUT, 500);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperFactory().createObjectMapper();

    private final URI _management;
    private final HttpClient _httpClient;
    private final String _authorization;
    private final String _virtualhostnode;
    private final String _virtualhost;
    private final String _queueApiUrl;
    private final String _brokerApiUrl;

    public QpidRestAPIQueueCreator() throws URISyntaxException
    {
        final String managementUser = System.getProperty("perftests.manangement-user", "guest");
        final String managementPassword = System.getProperty("perftests.manangement-password", "guest");

        _virtualhostnode = System.getProperty("perftests.broker-virtualhostnode", "default");
        _virtualhost = System.getProperty("perftests.broker-virtualhost", "default");

        _management = new URI(System.getProperty("perftests.manangement-url", "http://localhost:8080"));
        _httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .proxy(HttpClient.Builder.NO_PROXY)
                .build();
        _authorization = getAuthorization(managementUser, managementPassword);
        _queueApiUrl = System.getProperty("perftests.manangement-api-queue", "/api/latest/queue/%s/%s/%s");
        _brokerApiUrl = System.getProperty("perftests.manangement-api-broker", "/api/latest/broker");
    }

    @Override
    public void createQueues(final Connection connection, final Session session, final List<QueueConfig> configs)
    {
        for (final QueueConfig queueConfig : configs)
        {
            final String queueName = queueConfig.getName();
            managementCreateQueue(queueName);
        }
    }

    @Override
    public void deleteQueues(final Connection connection, final Session session, final List<QueueConfig> configs)
    {
        for (final QueueConfig queueConfig : configs)
        {
            final String queueName = queueConfig.getName();
            drainQueue(connection, queueName);
            managementDeleteQueue(queueName);
        }
    }

    @Override
    public String getProtocolVersion(final Connection connection)
    {
        if (connection != null)
        {
            try
            {
                final Method method = connection.getClass().getMethod("getProtocolVersion"); // Qpid 0-8..0-10 method only
                Object version =  method.invoke(connection);
                return String.valueOf(version);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e)
            {
                try
                {
                    ConnectionMetaData metaData = connection.getMetaData();
                    if (metaData != null && ("QpidJMS".equals(metaData.getJMSProviderName()) ||
                                             "AMQP.ORG".equals(metaData.getJMSProviderName())))
                    {
                        return "1.0";
                    }
                }
                catch (JMSException e1)
                {
                    return null;
                }
                return null;
            }
        }
        return null;
    }

    @Override
    public String getProviderVersion(final Connection connection)
    {
        final Map<String, Object> stringObjectMap = managementQueryBroker();
        return stringObjectMap.get("productVersion") == null ? null : String.valueOf(stringObjectMap.get("productVersion"));
    }

    private void drainQueue(Connection connection, String queueName)
    {
        try
        {
            int counter = 0;
            while (queueContainsMessages(connection, queueName))
            {
                if (counter == 0)
                {
                    LOGGER.debug("Draining queue {}", queueName);
                }
                counter += drain(connection, queueName);
            }
            if (counter > 0)
            {
                LOGGER.info("Drained {} message(s) from queue {} ", counter, queueName);
            }
        }
        catch (JMSException e)
        {
            throw new DistributedTestException("Failed to drain queue " + queueName, e);
        }
    }

    private int drain(Connection connection, String queueName) throws JMSException
    {
        int counter = 0;
        Session session = null;
        try
        {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(session.createQueue(queueName));
            try
            {
                while (messageConsumer.receive(DRAIN_POLL_TIMEOUT) != null)
                {
                    counter++;
                }
            }
            finally
            {
                messageConsumer.close();
            }
        }
        finally
        {
            if (session != null)
            {
                session.close();
            }
        }
        return counter;
    }

    private boolean queueContainsMessages(Connection connection, String queueName) throws JMSException
    {
        Session session = null;
        try
        {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueBrowser browser = null;
            try
            {
                browser = session.createBrowser(session.createQueue(queueName));
                return browser.getEnumeration().hasMoreElements();
            }
            finally
            {
                if (browser != null)
                {
                    browser.close();
                }
            }
        }
        finally
        {
            if (session != null)
            {
                session.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> managementQueryBroker()
    {
        final HttpRequest request = newManagementRequest(_brokerApiUrl).GET().build();
        Object obj = executeManagement(request);
        if (obj == null)
        {
            final String error = String.format("Unexpected null response from management query '%s'", request);
            throw new IllegalStateException(error);
        }
        else if (obj instanceof Collection)
        {
            final Iterator<?> itr = ((Collection<?>) obj).iterator();
            if (!itr.hasNext())
            {
                final String error = String.format("Unexpected empty list response from management query '%s'", request);
                throw new IllegalStateException(error);
            }
            obj = itr.next();
        }

        if (obj instanceof Map)
        {
            return (Map<String, Object>) obj;
        }
        else
        {
            final String error = String.format("Unexpected response '%s' from management query '%s'", obj, request);
            throw new IllegalStateException(error);
        }
    }

    private void managementCreateQueue(final String name)
    {
        final String path = String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name);
        final HttpRequest request = newManagementRequest(path)
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .PUT(HttpRequest.BodyPublishers.ofString("{}", StandardCharsets.UTF_8))
                .build();
        executeManagement(request);
    }

    private void managementDeleteQueue(final String name)
    {
        final String path = String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name);
        final HttpRequest request = newManagementRequest(path)
                .DELETE()
                .build();
        executeManagement(request);
    }

    private HttpRequest.Builder newManagementRequest(final String path)
    {
        return HttpRequest.newBuilder(_management.resolve(path)).header(AUTHORIZATION, _authorization);
    }

    private Object executeManagement(final HttpRequest httpRequest)
    {
        try
        {
            final HttpResponse.BodyHandler<byte[]> bodyHandler = HttpResponse.BodyHandlers.ofByteArray();
            final HttpResponse<byte[]> response = _httpClient.send(httpRequest, bodyHandler);
            return handleResponse(response);
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

    private Object handleResponse(final HttpResponse<byte[]> response) throws IOException
    {
        final int status = response.statusCode();
        final HttpClient.Version version = response.version();
        if (status != HTTP_OK && status != HTTP_CREATED)
        {
            final String msg = String.format("Failed: HTTP error code: %d, Version: %s", status, version);
            throw new RuntimeException(msg);
        }

        final byte[] body = response.body();
        if (body.length > 0)
        {
            return OBJECT_MAPPER.readValue(body, Object.class);
        }
        return null;
    }
}
