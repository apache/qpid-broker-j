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

package org.apache.qpid.test.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class QpidJmsClientConnectionBuilder implements ConnectionBuilder
{

    private static final AtomicInteger CLIENTID_COUNTER = new AtomicInteger();
    private String _username;
    private String _password;
    private Map<String, Object> _options;
    private boolean _enableTls;
    private boolean _enableFailover;

    public QpidJmsClientConnectionBuilder()
    {
        _options = new TreeMap<>();
        _options.put("jms.clientID", getNextClientId());
        _options.put("amqp.vhost", "test");
        _username = "guest";
        _password = "guest";
    }

    @Override
    public ConnectionBuilder setPrefetch(final int prefetch)
    {
        _options.put("jms.prefetchPolicy.all", prefetch);
        return this;
    }

    @Override
    public ConnectionBuilder setClientId(final String clientId)
    {
        if (clientId == null)
        {
            _options.remove("jms.clientID");
        }
        else
        {
            _options.put("jms.clientID", clientId);
        }
        return this;
    }

    @Override
    public ConnectionBuilder setUsername(final String username)
    {
        _username = username;
        return this;
    }

    @Override
    public ConnectionBuilder setPassword(final String password)
    {
        _password = password;
        return this;
    }

    @Override
    public ConnectionBuilder setVirtualHost(final String virtualHostName)
    {
        _options.put("amqp.vhost", virtualHostName);
        return this;
    }

    @Override
    public ConnectionBuilder setFailover(final boolean enableFailover)
    {
        _enableFailover = enableFailover;
        return this;
    }

    @Override
    public ConnectionBuilder setFailoverReconnectAttempts(final int reconnectAttempts)
    {
        _options.put("failover.maxReconnectAttempts", reconnectAttempts);
        return this;
    }

    @Override
    public ConnectionBuilder setTls(final boolean enableTls)
    {
        _enableTls = enableTls;
        return this;
    }

    @Override
    public ConnectionBuilder setSyncPublish(final boolean syncPublish)
    {
        _options.put("jms.forceSyncSend", syncPublish);
        return this;
    }

    @Override
    public Connection build() throws NamingException, JMSException
    {
        final Hashtable<Object, Object> initialContextEnvironment = new Hashtable<>();
        final String factoryName;

        final Map<String, Object> options = new TreeMap<>();
        options.putAll(_options);
        if (_enableFailover)
        {
            if (!options.containsKey("failover.maxReconnectAttempts"))
            {
                options.put("failover.maxReconnectAttempts", "2");
            }
            final StringBuilder stem = new StringBuilder("failover:(amqp://localhost:")
                    .append(System.getProperty("test.port"))
                    .append(",amqp://localhost:")
                    .append(System.getProperty("test.port.alt"))
                    .append(")");
            appendOptions(options, stem);

            initialContextEnvironment.put("property.connectionfactory.failover.remoteURI",
                                           stem.toString());
            factoryName = "failover";
        }
        else if (!_enableTls)
        {
            final StringBuilder stem =
                    new StringBuilder("amqp://localhost:").append(System.getProperty("test.port"));

            appendOptions(options, stem);

            initialContextEnvironment.put("property.connectionfactory.default.remoteURI", stem.toString());
            factoryName = "default";
        }
        else
        {

            final StringBuilder stem = new StringBuilder("amqps://localhost:").append(String.valueOf(System.getProperty("test.port.ssl")));
            appendOptions(options, stem);
            initialContextEnvironment.put("connectionfactory.default.ssl", stem.toString());
            factoryName = "default.ssl";
        }
        final ConnectionFactory connectionFactory =
                (ConnectionFactory) new InitialContext(initialContextEnvironment).lookup(factoryName);

        return connectionFactory.createConnection(_username, _password);
    }



    private void appendOptions(final Map<String, Object> actualOptions, final StringBuilder stem)
    {
        boolean first = true;
        for(Map.Entry<String, Object> option : actualOptions.entrySet())
        {
            if(first)
            {
                stem.append('?');
                first = false;
            }
            else
            {
                stem.append('&');
            }
            try
            {
                stem.append(option.getKey()).append('=').append(URLEncoder.encode(String.valueOf(option.getValue()), "UTF-8"));
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private String getNextClientId()
    {
        return "builderClientId-" + CLIENTID_COUNTER.getAndIncrement();
    }
}
