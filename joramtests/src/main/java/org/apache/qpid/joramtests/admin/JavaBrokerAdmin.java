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

package org.apache.qpid.joramtests.admin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.objectweb.jtests.jms.admin.Admin;

public class JavaBrokerAdmin implements Admin
{
    private final String _virtualhostnode;
    private final String _virtualhost;

    private final HttpHost _management;
    private final CredentialsProvider _credentialsProvider;
    private final HttpClientContext _httpClientContext;

    private final InitialContext _context;
    private final String _queueApiUrl;
    private final String _topicApiUrl;

    public JavaBrokerAdmin() throws NamingException
    {
        final Hashtable<String, String> env = new Hashtable<>();
        _context = new InitialContext(env);

        final String managementUser = System.getProperty("joramtests.manangement-user", "guest");
        final String managementPassword = System.getProperty("joramtests.manangement-password", "guest");

        _virtualhostnode = System.getProperty("joramtests.broker-virtualhostnode", "default");
        _virtualhost = System.getProperty("joramtests.broker-virtualhost", "default");

        _management = HttpHost.create(System.getProperty("joramtests.manangement-url", "http://localhost:8080"));
        _queueApiUrl = System.getProperty("joramtests.manangement-api-queue", "/api/latest/queue/%s/%s/%s");
        _topicApiUrl = System.getProperty("joramtests.manangement-api-topic", "/api/latest/exchange/%s/%s/%s");

        _credentialsProvider = getCredentialsProvider(managementUser, managementPassword);
        _httpClientContext = getHttpClientContext(_management);
    }


    @Override
    public String getName()
    {
        return "JavaBroker";
    }

    @Override
    public Context createContext() throws NamingException
    {
        return _context;
    }

    @Override
    public void createConnectionFactory(final String name)
    {
        checkObjectExistsInContext(name, ConnectionFactory.class);

    }

    @Override
    public void createQueueConnectionFactory(final String name)
    {
        checkObjectExistsInContext(name, QueueConnectionFactory.class);
    }

    @Override
    public void createTopicConnectionFactory(final String name)
    {
        checkObjectExistsInContext(name, TopicConnectionFactory.class);

    }

    @Override
    public void createQueue(final String name)
    {
        checkObjectExistsInContext(name, Queue.class);
        managementCreateQueue(name);

    }

    @Override
    public void deleteQueue(final String name)
    {
        managementDeleteQueue(name);
    }

    @Override
    public void createTopic(final String name)
    {
        checkObjectExistsInContext(name, Topic.class);
        managementCreateTopic(name);
    }

    @Override
    public void deleteTopic(final String name)
    {
        managementDeleteTopic(name);
    }

    @Override
    public void deleteConnectionFactory(final String name)
    {

    }

    @Override
    public void deleteQueueConnectionFactory(final String name)
    {

    }

    @Override
    public void deleteTopicConnectionFactory(final String name)
    {

    }

    @Override
    public void startServer() throws Exception
    {

    }

    @Override
    public void stopServer() throws Exception
    {

    }

    @Override
    public void start() throws Exception
    {

    }

    @Override
    public void stop() throws Exception
    {

    }

    private void checkObjectExistsInContext(final String name, final Class<?> clazz)
    {
        try
        {
            final Object object = _context.lookup(name);
            if (!clazz.isInstance(object))
            {
                throw new IllegalArgumentException(String.format("'%s' has unexpected type. It is a '%s', but expected a '%s'",
                                                                 name,
                                                                 object.getClass().getName(),
                                                                 clazz.getName()));
            }
        }
        catch (NamingException e)
        {
            throw new IllegalArgumentException(e);
        }
    }


    private void managementCreateQueue(final String name)
    {
        HttpPut put = new HttpPut(String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name));

        StringEntity input = new StringEntity("{}", StandardCharsets.UTF_8);
        input.setContentType("application/json");
        put.setEntity(input);

        executeManagement(put);
    }

    private void managementCreateTopic(final String name)
    {
        HttpPut put = new HttpPut(String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name));

        StringEntity input = new StringEntity("{\"type\" : \"fanout\"}", StandardCharsets.UTF_8);
        input.setContentType("application/json");

        put.setEntity(input);

        executeManagement(put);
    }

    private void managementDeleteQueue(final String name)
    {
        HttpDelete delete = new HttpDelete(String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name));
        executeManagement(delete);
    }

    private void managementDeleteTopic(final String name)
    {
        HttpDelete delete = new HttpDelete(String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name));
        executeManagement(delete);
    }

    private void executeManagement(final HttpRequest httpRequest)
    {
        try(CloseableHttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(_credentialsProvider).build())
        {
            try (CloseableHttpResponse response = httpClient.execute(_management, httpRequest, _httpClientContext))
            {

                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200 && statusCode != 201)
                {
                    throw new RuntimeException(String.format("Failed : HTTP error code : %d  status line : %s",
                                                             statusCode,
                                                             response.getStatusLine()));
                }
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
