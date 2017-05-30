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

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.url.URLSyntaxException;

public class QpidJmsClient0xConnectionBuilder implements ConnectionBuilder
{
    private String _clientId = "clientid";
    private String _username = "guest";
    private String _password = "guest";
    private String _virtualHost;
    private boolean _enableTls;
    private boolean _enableFailover;
    private final Map<String, Object> _options = new TreeMap<>();
    private int _reconnectAttempts = 20;

    @Override
    public ConnectionBuilder setPrefetch(final int prefetch)
    {
        _options.put("maxprefetch", prefetch);
        return this;
    }

    @Override
    public ConnectionBuilder setClientId(final String clientId)
    {
        _clientId = clientId;
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
        _virtualHost = virtualHostName;
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
        _reconnectAttempts = reconnectAttempts;
        return this;
    }

    @Override
    public ConnectionBuilder setTls(final boolean enableTls)
    {
        _enableTls = enableTls;
        return this;
    }

    @Override
    public Connection build() throws JMSException, NamingException, URLSyntaxException
    {
        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        contextProperties.put(Context.PROVIDER_URL, System.getProperty(Context.PROVIDER_URL));
        InitialContext initialContext = null;
        ConnectionFactory connectionFactory;
        try
        {
            initialContext = new InitialContext(contextProperties);
            String jndiName = "default";
            if (_enableFailover)
            {
                jndiName = "failover";
            }

            if (_enableTls)
            {
                jndiName += ".ssl";
            }
            connectionFactory = (ConnectionFactory) initialContext.lookup(jndiName);
        }

        finally
        {
            if (initialContext != null)
            {
                initialContext.close();
            }
        }
        AMQConnectionURL curl =
                new AMQConnectionURL(((AMQConnectionFactory) connectionFactory).getConnectionURLString());

        if (_virtualHost != null)
        {
            curl.setVirtualHost("/" + _virtualHost);
        }

        for (Map.Entry<String, Object> entry: _options.entrySet())
        {
            curl.setOption(entry.getKey(), String.valueOf(entry.getValue()));
        }

        if (_enableFailover)
        {
            curl.setFailoverOption("cyclecount", String.valueOf(_reconnectAttempts));
        }

        curl.setClientName(_clientId);

        curl = new AMQConnectionURL(curl.toString());
        connectionFactory = new AMQConnectionFactory(curl);


        return connectionFactory.createConnection(_username, _password);
    }
}
