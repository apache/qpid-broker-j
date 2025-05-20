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
import java.net.URISyntaxException;
import java.util.Hashtable;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicAuthCache;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.auth.BasicScheme;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ProtocolVersion;
import org.apache.hc.core5.http.io.entity.StringEntity;
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

    public JavaBrokerAdmin() throws NamingException, URISyntaxException
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
    public Context createContext()
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
    public void startServer()
    {

    }

    @Override
    public void stopServer()
    {

    }

    @Override
    public void start()
    {

    }

    @Override
    public void stop()
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
        final HttpPut put = new HttpPut(String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name));
        final StringEntity input = new StringEntity("{}", ContentType.APPLICATION_JSON, "UTF_8", false);
        put.setEntity(input);
        executeManagement(put);
    }

    private void managementCreateTopic(final String name)
    {
        final HttpPut put = new HttpPut(String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name));
        final StringEntity input = new StringEntity("{\"type\" : \"fanout\"}", ContentType.APPLICATION_JSON, "UTF_8", false);
        put.setEntity(input);
        executeManagement(put);
    }

    private void managementDeleteQueue(final String name)
    {
        final HttpDelete delete = new HttpDelete(String.format(_queueApiUrl, _virtualhostnode, _virtualhost, name));
        executeManagement(delete);
    }

    private void managementDeleteTopic(final String name)
    {
        final HttpDelete delete = new HttpDelete(String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name));
        executeManagement(delete);
    }

    private void executeManagement(final ClassicHttpRequest httpRequest)
    {
        try (final CloseableHttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(_credentialsProvider).build();
             final CloseableHttpResponse response = httpClient.execute(_management, httpRequest, _httpClientContext))
        {
            final int status = response.getCode();
            final ProtocolVersion version = response.getVersion();
            final String reason = response.getReasonPhrase();
            if (status != 200 && status != 201)
            {
                final String msg = String.format("Failed: HTTP error code: %d, Version: %s, Reason: %s", status, version, reason);
                throw new RuntimeException(msg);
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
        final HttpClientContext localContext = HttpClientContext.create();
        localContext.setAuthCache(authCache);
        return localContext;
    }

    private CredentialsProvider getCredentialsProvider(final String managementUser, final String managementPassword)
    {
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope("localhost", 8080), new UsernamePasswordCredentials(managementUser, managementPassword.toCharArray()));
        return credentialsProvider;
    }
}
