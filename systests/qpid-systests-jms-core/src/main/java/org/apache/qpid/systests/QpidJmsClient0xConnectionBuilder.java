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

package org.apache.qpid.systests;

import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class QpidJmsClient0xConnectionBuilder implements ConnectionBuilder
{
    private String _clientId = "clientid";
    private String _username = USERNAME;
    private String _password = PASSWORD;
    private String _virtualHost;
    private boolean _enableTls;
    private boolean _enableFailover;
    private final Map<String, Object> _options = new TreeMap<>();
    private int _reconnectAttempts = 20;
    private String _host = "localhost";
    private int _port;
    private int _sslPort;

    @Override
    public ConnectionBuilder setHost(final String host)
    {
        _host = host;
        return this;
    }

    @Override
    public ConnectionBuilder setPort(final int port)
    {
        _port = port;
        return this;
    }

    @Override
    public ConnectionBuilder setSslPort(final int port)
    {
        _sslPort = port;
        return this;
    }

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
    public ConnectionBuilder setSyncPublish(final boolean syncPublish)
    {
        if (syncPublish)
        {
            _options.put("sync_publish", "all");
        }
        else
        {
            _options.remove("sync_publish");
        }
        return this;
    }

    @Override
    public ConnectionBuilder setOptions(final Map<String, String> options)
    {
        _options.putAll(options);
        return this;
    }

    @Override
    public ConnectionBuilder setPopulateJMSXUserID(final boolean populateJMSXUserID)
    {
        _options.put("populateJMSXUserID", String.valueOf(populateJMSXUserID));
        return this;
    }

    @Override
    public ConnectionBuilder setMessageRedelivery(final boolean redelivery)
    {
        if (redelivery)
        {
            _options.put("rejectbehaviour", "server");
        }
        else
        {
            _options.remove("rejectbehaviour");
        }
        return this;
    }

    @Override
    public Connection build() throws JMSException, NamingException
    {
        return buildConnectionFactory().createConnection(_username, _password);
    }

    @Override
    public ConnectionFactory buildConnectionFactory() throws NamingException
    {
        StringBuilder cUrlBuilder = new StringBuilder("amqp://");
        if (_username != null)
        {
            cUrlBuilder.append(_username);
        }

        if (_username != null || _password != null)
        {
            cUrlBuilder.append(":");
        }

        if (_password != null)
        {
            cUrlBuilder.append(_password);
        }

        if (_username != null || _password != null)
        {
            cUrlBuilder.append("@");
        }

        if (_clientId != null)
        {
            cUrlBuilder.append(_clientId);
        }

        cUrlBuilder.append("/");

        if (_virtualHost != null)
        {
            cUrlBuilder.append(_virtualHost);
        }

        cUrlBuilder.append("?brokerlist='tcp://").append(_host).append(":");
        if (_enableTls)
        {
            cUrlBuilder.append(_sslPort).append("?ssl='true'");
        }
        else
        {
            cUrlBuilder.append(_port);
        }

        if (_enableFailover)
        {
            cUrlBuilder.append(";tcp://").append(_host).append(":");
            if (_enableTls)
            {
                cUrlBuilder.append(System.getProperty("test.port.alt.ssl")).append("?ssl='true'");
            }
            else
            {
                cUrlBuilder.append(System.getProperty("test.port.alt"));
            }
            cUrlBuilder.append("'")
                       .append("&sync_ack='true'&sync_publish='all'&failover='roundrobin?cyclecount='")
                       .append(_reconnectAttempts)
                       .append("''");
        }
        else
        {
            cUrlBuilder.append("'");
        }

        for (Map.Entry<String, Object> entry : _options.entrySet())
        {
            cUrlBuilder.append("&").append(entry.getKey()).append("='").append(entry.getValue()).append("'");
        }

        final Hashtable<Object, Object> initialContextEnvironment = new Hashtable<>();
        initialContextEnvironment.put(Context.INITIAL_CONTEXT_FACTORY,
                                      "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        final String factoryName = "connectionFactory";
        initialContextEnvironment.put("connectionfactory." + factoryName, cUrlBuilder.toString());
        InitialContext initialContext = new InitialContext(initialContextEnvironment);
        try
        {
            return (ConnectionFactory) initialContext.lookup(factoryName);
        }
        finally
        {
            initialContext.close();
        }
    }

    String getBrokerDetails()
    {
        return "tcp://" + _host + ":" + _port;
    }
}
