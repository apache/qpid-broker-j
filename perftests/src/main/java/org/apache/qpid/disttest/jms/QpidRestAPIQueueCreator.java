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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
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

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.QueueConfig;

/**
 * Assumes Basic-Auth is enabled
 */
public class QpidRestAPIQueueCreator implements QueueCreator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidRestAPIQueueCreator.class);
    private static int _drainPollTimeout = Integer.getInteger(QUEUE_CREATOR_DRAIN_POLL_TIMEOUT, 500);

    private final HttpHost _management;
    private final String _virtualhostnode;
    private final String _virtualhost;
    private final String _queueApiUrl;
    private final String _brokerApiUrl;

    private final CredentialsProvider _credentialsProvider;

    public QpidRestAPIQueueCreator()
    {
        final String managementUser = System.getProperty("perftests.manangement-user", "guest");
        final String managementPassword = System.getProperty("perftests.manangement-password", "guest");

        _virtualhostnode = System.getProperty("perftests.broker-virtualhostnode", "default");
        _virtualhost = System.getProperty("perftests.broker-virtualhost", "default");

        _management = HttpHost.create(System.getProperty("perftests.manangement-url", "http://localhost:8080"));
        _queueApiUrl = System.getProperty("perftests.manangement-api-queue", "/api/latest/queue/%s/%s/%s");
        _brokerApiUrl = System.getProperty("perftests.manangement-api-broker", "/api/latest/broker");

        _credentialsProvider = getCredentialsProvider(managementUser, managementPassword);
    }

    @Override
    public void createQueues(Connection connection, Session session, List<QueueConfig> configs)
    {
        HttpClientContext context = getHttpClientContext(_management);

        for (QueueConfig queueConfig : configs)
        {
            final String queueName = queueConfig.getName();
            managementCreateQueue(queueName, context);
        }
    }

    @Override
    public void deleteQueues(Connection connection, Session session, List<QueueConfig> configs)
    {
        HttpClientContext context = getHttpClientContext(_management);

        for (QueueConfig queueConfig : configs)
        {
            final String queueName = queueConfig.getName();
            drainQueue(connection, queueName);
            managementDeleteQueue(queueName, context);
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
        HttpClientContext context = getHttpClientContext(_management);

        final Map<String, Object> stringObjectMap = managementQueryBroker(context);
        return stringObjectMap == null || stringObjectMap.get("productVersion") == null ? null : String.valueOf(stringObjectMap.get("productVersion"));
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
                while (messageConsumer.receive(_drainPollTimeout) != null)
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
    private Map<String, Object> managementQueryBroker(final HttpClientContext context)
    {
        HttpGet get = new HttpGet(_brokerApiUrl);
        Object obj = executeManagement(get, context);
        if (obj == null)
        {
            throw new IllegalStateException(String.format("Unexpected null response from management query '%s'", get));
        }
        else if (obj instanceof Collection)
        {
            Iterator itr = ((Collection) obj).iterator();
            if (!itr.hasNext())
            {
                throw new IllegalStateException(String.format("Unexpected empty list response from management query '%s'", get));
            }
            obj = itr.next();
        }

        if (obj instanceof Map)
        {
            return (Map<String, Object>) obj;
        }
        else
        {
            throw new IllegalStateException(String.format("Unexpected response '%s' from management query '%s'", obj, get));
        }
    }

    private void managementCreateQueue(final String name, final HttpClientContext context)
    {
        HttpPut put = new HttpPut(String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name));

        StringEntity input = new StringEntity("{}", StandardCharsets.UTF_8);
        input.setContentType("application/json");
        put.setEntity(input);

        executeManagement(put, context);
    }

    private void managementDeleteQueue(final String name, final HttpClientContext context)
    {
        HttpDelete delete = new HttpDelete(String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name));
        executeManagement(delete, context);
    }

    private Object executeManagement(final HttpRequest httpRequest, final HttpClientContext context)
    {
        try(CloseableHttpClient httpClient = HttpClients.custom()
                                                        .setDefaultCredentialsProvider(_credentialsProvider)
                                                        .build();
            CloseableHttpResponse response = httpClient.execute(_management, httpRequest, context))
        {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200 && statusCode != 201)
            {
                throw new RuntimeException(String.format("Failed : HTTP error code : %d  status line : %s", statusCode,
                                                         response.getStatusLine()));
            }

            if (response.getEntity() != null)
            {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream())
                {
                    response.getEntity().writeTo(bos);
                    if (bos.size() > 0)
                    {
                        return new ObjectMapper().readValue(bos.toByteArray(), Object.class);
                    }
                }
            }
            return null;

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
