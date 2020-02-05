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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class QpidJmsClientConnectionBuilder implements ConnectionBuilder
{
    private static final AtomicInteger CLIENTID_COUNTER = new AtomicInteger();
    private String _host;
    private int _port;
    private Map<String, Object> _options;
    private boolean _enableTls;
    private boolean _enableFailover;
    private final List<Integer> _failoverPorts = new ArrayList<>();
    private String _transport = "amqp";

    QpidJmsClientConnectionBuilder()
    {
        _options = new TreeMap<>();
        _options.put("jms.clientID", getNextClientId());
        _options.put("jms.username", USERNAME);
        _options.put("jms.password", PASSWORD);
        _options.put("transport.enabledProtocols", "TLSv1.2");
        _options.put("transport.disabledProtocols", "SSLv2Hello,SSLv3,TLSv1,TLSv1.1");
        _host = "localhost";
    }

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
    public ConnectionBuilder addFailoverPort(final int port)
    {
        _failoverPorts.add(port);
        return this;
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
        if (username == null)
        {
            _options.remove("jms.username");
        }
        else
        {
            _options.put("jms.username", username);
        }
        return this;
    }

    @Override
    public ConnectionBuilder setPassword(final String password)
    {
        if (password == null)
        {
            _options.remove("jms.password");
        }
        else
        {
            _options.put("jms.password", password);
        }
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
    public ConnectionBuilder setFailoverReconnectDelay(final int connectDelay)
    {
        _options.put("failover.reconnectDelay", connectDelay);
        return this;
    }

    @Override
    public ConnectionBuilder setTls(final boolean enableTls)
    {
        _enableTls = enableTls;
        if (enableTls)
        {
            _options.put("transport.storeType", KeyStore.getDefaultType());
        }
        else
        {
            _options.remove("transport.storeType");
        }
        return this;
    }

    @Override
    public ConnectionBuilder setSyncPublish(final boolean syncPublish)
    {
        _options.put("jms.forceSyncSend", syncPublish);
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
        _options.put("jms.populateJMSXUserID", String.valueOf(populateJMSXUserID));
        return this;
    }

    @Override
    public ConnectionBuilder setMessageRedelivery(final boolean redelivery)
    {
        return this;
    }

    @Override
    public ConnectionBuilder setDeserializationPolicyWhiteList(final String whiteList)
    {
        _options.put("jms.deserializationPolicy.whiteList", whiteList);
        return this;
    }

    @Override
    public ConnectionBuilder setDeserializationPolicyBlackList(final String blackList)
    {
        _options.put("jms.deserializationPolicy.blackList", blackList);
        return this;
    }

    @Override
    public ConnectionBuilder setKeyStoreLocation(final String keyStoreLocation)
    {
        _options.put("transport.keyStoreLocation", keyStoreLocation);
        return this;
    }

    @Override
    public ConnectionBuilder setKeyStorePassword(final String keyStorePassword)
    {
        _options.put("transport.keyStorePassword", keyStorePassword);
        return this;
    }

    @Override
    public ConnectionBuilder setTrustStoreLocation(final String trustStoreLocation)
    {
        _options.put("transport.trustStoreLocation", trustStoreLocation);
        return this;
    }

    @Override
    public ConnectionBuilder setTrustStorePassword(final String trustStorePassword)
    {
        _options.put("transport.trustStorePassword", trustStorePassword);
        return this;
    }

    @Override
    public ConnectionBuilder setVerifyHostName(final boolean verifyHostName)
    {
        _options.put("transport.verifyHost", verifyHostName);
        return this;
    }

    @Override
    public ConnectionBuilder setKeyAlias(final String alias)
    {
        _options.put("transport.keyAlias", alias);
        return this;
    }

    @Override
    public ConnectionBuilder setSaslMechanisms(final String... mechanism)
    {
        _options.put("amqp.saslMechanisms", String.join(",", mechanism));
        return this;
    }

    @Override
    public ConnectionBuilder setCompress(final boolean compress)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection build() throws NamingException, JMSException
    {
        return buildConnectionFactory().createConnection();
    }

    @Override
    public ConnectionFactory buildConnectionFactory() throws NamingException
    {
        final Hashtable<Object, Object> initialContextEnvironment = new Hashtable<>();
        initialContextEnvironment.put(Context.INITIAL_CONTEXT_FACTORY,
                                      "org.apache.qpid.jms.jndi.JmsInitialContextFactory");

        final String connectionUrl = buildConnectionURL();

        final String factoryName = "connection";
        initialContextEnvironment.put("connectionfactory." + factoryName, connectionUrl);

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

    @Override
    public String buildConnectionURL()
    {
        final StringBuilder connectionUrlBuilder = new StringBuilder();

        final Map<String, Object> options = new TreeMap<>();
        options.putAll(_options);
        if (_enableFailover)
        {
            if (!options.containsKey("failover.useReconnectBackOff"))
            {
                options.put("failover.useReconnectBackOff", "false");
            }
            if (!options.containsKey("failover.maxReconnectAttempts"))
            {
                options.put("failover.maxReconnectAttempts", "2");
            }

            final Set<String> transportKeys = options.keySet()
                                                     .stream()
                                                     .filter(key -> key.startsWith("amqp.") || key.startsWith(
                                                             "transport."))
                                                     .collect(Collectors.toSet());


            final Map<String, Object> transportOptions = new HashMap<>(options);
            transportOptions.keySet().retainAll(transportKeys);
            options.keySet().removeAll(transportKeys);

            final StringBuilder transportQueryBuilder = new StringBuilder();
            appendOptions(transportOptions, transportQueryBuilder);
            final String transportQuery = transportQueryBuilder.toString();

            final List<Integer> copy = new ArrayList<>(_failoverPorts.size() + 1);
            copy.add(_port);
            copy.addAll(_failoverPorts);

            final String failover = copy.stream()
                                        .map(port -> String.format("amqp://%s:%d%s", _host, port, transportQuery))
                                        .collect(Collectors.joining(",", "failover:(", ")"));
            connectionUrlBuilder.append(failover);
            appendOptions(options, connectionUrlBuilder);
        }
        connectionUrlBuilder.append(_transport);
        if (_enableTls)
        {
            connectionUrlBuilder.append("s");
        }
        connectionUrlBuilder.append("://").append(_host).append(":").append(_port);
        appendOptions(options, connectionUrlBuilder);
        return connectionUrlBuilder.toString();
    }

    @Override
    public ConnectionBuilder setTransport(final String transport)
    {
        _transport = transport;
        return this;
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
