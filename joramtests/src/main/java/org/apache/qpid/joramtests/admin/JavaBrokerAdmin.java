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

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Hashtable;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.objectweb.jtests.jms.admin.Admin;

public class JavaBrokerAdmin implements Admin
{
    private static final String APPLICATION_JSON = "application/json; charset=UTF-8";
    private static final String AUTHORIZATION = "Authorization";
    private static final String CONTENT_TYPE = "Content-Type";

    private final String _virtualhostnode;
    private final String _virtualhost;

    private final URI _management;
    private final HttpClient _httpClient;
    private final String _authorization;

    private final InitialContext _context;
    private final String _queueApiUrl;
    private final String _topicApiUrl;

    public JavaBrokerAdmin() throws NamingException, URISyntaxException
    {
        final Hashtable<String, String> env = new Hashtable<>();
        _context = new InitialContext(env);

        final String managementUser = System.getProperty("joramtests.manangement-username",
                System.getProperty("joramtests.manangement-user", "guest"));
        final String managementPassword = System.getProperty("joramtests.manangement-password", "guest");

        _virtualhostnode = System.getProperty("joramtests.broker-virtualhostnode", "default");
        _virtualhost = System.getProperty("joramtests.broker-virtualhost", "default");

        _management = new URI(System.getProperty("joramtests.manangement-url", "http://localhost:8080"));
        _httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .proxy(HttpClient.Builder.NO_PROXY)
                .build();
        _authorization = getAuthorization(managementUser, managementPassword);
        _queueApiUrl = System.getProperty("joramtests.manangement-api-queue", "/api/latest/queue/%s/%s/%s");
        _topicApiUrl = System.getProperty("joramtests.manangement-api-topic", "/api/latest/exchange/%s/%s/%s");
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
                        name, object.getClass().getName(), clazz.getName()));
            }
        }
        catch (NamingException e)
        {
            throw new IllegalArgumentException(e);
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

    private void managementCreateTopic(final String name)
    {
        final String path = String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name);
        final HttpRequest request = newManagementRequest(path)
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .PUT(HttpRequest.BodyPublishers.ofString("{\"type\" : \"fanout\"}", StandardCharsets.UTF_8))
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

    private void managementDeleteTopic(final String name)
    {
        final String path = String.format(_topicApiUrl, _virtualhostnode, _virtualhost, name);
        final HttpRequest request = newManagementRequest(path)
                .DELETE()
                .build();
        executeManagement(request);
    }

    private HttpRequest.Builder newManagementRequest(final String path)
    {
        return HttpRequest.newBuilder(_management.resolve(path)).header(AUTHORIZATION, _authorization);
    }

    private void executeManagement(final HttpRequest httpRequest)
    {
        try
        {
            final HttpResponse<Void> response = _httpClient.send(httpRequest, HttpResponse.BodyHandlers.discarding());
            final int status = response.statusCode();
            final HttpClient.Version version = response.version();
            if (status != HTTP_OK && status != HTTP_CREATED)
            {
                final String msg = String.format("Failed: HTTP error code: %d, Version: %s", status, version);
                throw new RuntimeException(msg);
            }
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
