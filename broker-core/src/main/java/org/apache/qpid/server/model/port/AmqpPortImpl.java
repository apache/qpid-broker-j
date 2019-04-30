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
package org.apache.qpid.server.model.port;

import java.io.IOException;
import java.io.StringWriter;
import java.net.SocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSessionContext;
import javax.security.auth.Subject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.logging.messages.PortMessages;
import org.apache.qpid.server.logging.subjects.PortLogSubject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.DefaultVirtualHostAlias;
import org.apache.qpid.server.model.HostNameAlias;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHostAlias;
import org.apache.qpid.server.model.VirtualHostNameAlias;
import org.apache.qpid.server.plugin.ConnectionPropertyEnricher;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.plugin.TransportProviderFactory;
import org.apache.qpid.server.transport.AcceptingTransport;
import org.apache.qpid.server.transport.PortBindFailureException;
import org.apache.qpid.server.transport.TransportProvider;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class AmqpPortImpl extends AbstractPort<AmqpPortImpl> implements AmqpPort<AmqpPortImpl>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpPortImpl.class);


    @ManagedAttributeField
    private boolean _tcpNoDelay;

    @ManagedAttributeField
    private int _maxOpenConnections;

    @ManagedAttributeField
    private int _threadPoolSize;

    @ManagedAttributeField
    private int _numberOfSelectors;

    private final AtomicInteger _connectionCount = new AtomicInteger();
    private final AtomicBoolean _connectionCountWarningGiven = new AtomicBoolean();
    private final AtomicLong _totalConnectionCount = new AtomicLong();

    private final Container<?> _container;
    private final AtomicBoolean _closingOrDeleting = new AtomicBoolean();

    private volatile AcceptingTransport _transport;
    private volatile SSLContext _sslContext;
    private volatile int _connectionWarnCount;
    private volatile long _protocolHandshakeTimeout;
    private volatile int _boundPort = -1;
    private volatile boolean _closeWhenNoRoute;
    private volatile int _sessionCountLimit;
    private volatile int _heartBeatDelay;
    private volatile int _tlsSessionTimeout;
    private volatile int _tlsSessionCacheSize;
    private volatile List<ConnectionPropertyEnricher> _connectionPropertyEnrichers;

    @ManagedObjectFactoryConstructor
    public AmqpPortImpl(Map<String, Object> attributes, Container<?> container)
    {
        super(attributes, container);
        _container = container;
    }

    @Override
    public int getThreadPoolSize()
    {
        return _threadPoolSize;
    }

    @Override
    public int getNumberOfSelectors()
    {
        return _numberOfSelectors;
    }


    @Override
    public SSLContext getSSLContext()
    {
        return _sslContext;
    }

    @Override
    public boolean isTcpNoDelay()
    {
        return _tcpNoDelay;
    }

    @Override
    public int getMaxOpenConnections()
    {
        return _maxOpenConnections;
    }

    @Override
    protected void onCreate()
    {
        super.onCreate();

        final Map<String, Object> nameAliasAttributes = new HashMap<>();
        nameAliasAttributes.put(VirtualHostAlias.NAME, "nameAlias");
        nameAliasAttributes.put(VirtualHostAlias.TYPE, VirtualHostNameAlias.TYPE_NAME);
        nameAliasAttributes.put(VirtualHostAlias.DURABLE, true);

        final Map<String, Object> defaultAliasAttributes = new HashMap<>();
        defaultAliasAttributes.put(VirtualHostAlias.NAME, "defaultAlias");
        defaultAliasAttributes.put(VirtualHostAlias.TYPE, DefaultVirtualHostAlias.TYPE_NAME);
        defaultAliasAttributes.put(VirtualHostAlias.DURABLE, true);

        final Map<String, Object> hostnameAliasAttributes = new HashMap<>();
        hostnameAliasAttributes.put(VirtualHostAlias.NAME, "hostnameAlias");
        hostnameAliasAttributes.put(VirtualHostAlias.TYPE, HostNameAlias.TYPE_NAME);
        hostnameAliasAttributes.put(VirtualHostAlias.DURABLE, true);

        Subject.doAs(getSubjectWithAddedSystemRights(),
                     new PrivilegedAction<Object>()
                     {
                         @Override
                         public Object run()
                         {
                             createChild(VirtualHostAlias.class, nameAliasAttributes);
                             createChild(VirtualHostAlias.class, defaultAliasAttributes);
                             createChild(VirtualHostAlias.class, hostnameAliasAttributes);
                             return null;
                         }
                     });
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        _protocolHandshakeTimeout = getContextValue(Long.class, AmqpPort.PROTOCOL_HANDSHAKE_TIMEOUT);
        _connectionWarnCount = getContextValue(Integer.class, OPEN_CONNECTIONS_WARN_PERCENT);
        _closeWhenNoRoute = getContextValue(Boolean.class, AmqpPort.CLOSE_WHEN_NO_ROUTE);
        _sessionCountLimit = getContextValue(Integer.class, AmqpPort.SESSION_COUNT_LIMIT);
        _heartBeatDelay = getContextValue(Integer.class, AmqpPort.HEART_BEAT_DELAY);
        _tlsSessionTimeout = getContextValue(Integer.class, AmqpPort.TLS_SESSION_TIMEOUT);
        _tlsSessionCacheSize = getContextValue(Integer.class, AmqpPort.TLS_SESSION_CACHE_SIZE);

        @SuppressWarnings("unchecked")
        List<String> configurationPropertyEnrichers = getContextValue(List.class, AmqpPort.CONNECTION_PROPERTY_ENRICHERS);
        List<ConnectionPropertyEnricher> enrichers = new ArrayList<>(configurationPropertyEnrichers.size());
        final Map<String, ConnectionPropertyEnricher> enrichersByType =
                new QpidServiceLoader().getInstancesByType(ConnectionPropertyEnricher.class);
        for(String enricherName : configurationPropertyEnrichers)
        {
            ConnectionPropertyEnricher enricher = enrichersByType.get(enricherName);
            if(enricher != null)
            {
                enrichers.add(enricher);
            }
            else
            {
                LOGGER.warn("Ignoring unknown Connection Property Enricher type: '"+enricherName+"' on port " + this.getName());
            }
        }
        _connectionPropertyEnrichers = Collections.unmodifiableList(enrichers);
    }

    @Override
    protected State onActivate()
    {
        if(getAncestor(SystemConfig.class).isManagementMode())
        {
            return State.QUIESCED;
        }
        else
        {
            Collection<Transport> transports = getTransports();

            TransportProvider transportProvider = null;
            final HashSet<Transport> transportSet = new HashSet<>(transports);
            for (TransportProviderFactory tpf : (new QpidServiceLoader()).instancesOf(TransportProviderFactory.class))
            {
                if (tpf.getSupportedTransports().contains(transports))
                {
                    transportProvider = tpf.getTransportProvider(transportSet);
                }
            }

            if (transportProvider == null)
            {
                throw new IllegalConfigurationException(
                        "No transport providers found which can satisfy the requirement to support the transports: "
                        + transports
                );
            }

            if (transports.contains(Transport.SSL) || transports.contains(Transport.WSS))
            {
                _sslContext = createSslContext();
            }
            Protocol defaultSupportedProtocolReply = getDefaultAmqpSupportedReply();
            try
            {
                _transport = transportProvider.createTransport(transportSet,
                                                               _sslContext,
                                                               this,
                                                               getProtocols(),
                                                               defaultSupportedProtocolReply);

                _transport.start();
                _boundPort = _transport.getAcceptingPort();
                for (Transport transport : getTransports())
                {
                    _container.getEventLogger()
                            .message(BrokerMessages.LISTENING(String.valueOf(transport),
                                                              _transport.getAcceptingPort()));
                }
                return State.ACTIVE;
            }
            catch (PortBindFailureException e)
            {
                _container.getEventLogger().message(PortMessages.BIND_FAILED(getType().toUpperCase(), getPort()));
                throw e;
            }
        }
    }

    @Override
    protected boolean updateSSLContext()
    {
        final Set<Transport> transports = getTransports();
        if (transports.contains(Transport.SSL) || transports.contains(Transport.WSS))
        {
            _sslContext = createSslContext();
            return _transport.updatesSSLContext();
        }
        return false;
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        _closingOrDeleting.set(true);
        return Futures.immediateFuture(null);
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        closeTransport();
        return Futures.immediateFuture(null);
    }

    @Override
    protected ListenableFuture<Void> beforeDelete()
    {
        _closingOrDeleting.set(true);
        return super.beforeDelete();
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        closeTransport();
        return super.onDelete();
    }

    private void closeTransport()
    {
        if (_transport != null)
        {
            for(Transport transport : getTransports())
            {
                _container.getEventLogger().message(BrokerMessages.SHUTTING_DOWN(String.valueOf(transport), _transport.getAcceptingPort()));
            }

            _transport.close();
        }
    }

    @Override
    public int getNetworkBufferSize()
    {
        return _container.getNetworkBufferSize();
    }

    @Override
    public List<ConnectionPropertyEnricher> getConnectionPropertyEnrichers()
    {
        return _connectionPropertyEnrichers;
    }

    @Override
    public int getBoundPort()
    {
        return _boundPort;
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        Collection<VirtualHostAlias> aliases = getChildren(VirtualHostAlias.class);
        if (aliases.size() == 0)
        {
            LOGGER.warn("{} has no virtualhost aliases defined.  No AMQP connections will be possible"
                        + " through this port until at least one alias is added.", this);
        }

        validateThreadPoolSettings(this);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        AmqpPort changed = (AmqpPort) proxyForValidation;
        if (changedAttributes.contains(THREAD_POOL_SIZE) || changedAttributes.contains(NUMBER_OF_SELECTORS))
        {
            validateThreadPoolSettings(changed);
        }
    }

    private void validateThreadPoolSettings(final AmqpPort changed)
    {
        if (changed.getThreadPoolSize() < 1)
        {
            throw new IllegalConfigurationException(String.format("Thread pool size %d on Port %s must be greater than zero.", changed.getThreadPoolSize(), getName()));
        }
        if (changed.getNumberOfSelectors() < 1)
        {
            throw new IllegalConfigurationException(String.format("Number of Selectors %d on Port %s must be greater than zero.", changed.getNumberOfSelectors(), getName()));
        }
        if (changed.getThreadPoolSize() <= changed.getNumberOfSelectors())
        {
            throw new IllegalConfigurationException(String.format("Number of Selectors %d on Port %s must be greater than the thread pool size %d.", changed.getNumberOfSelectors(), getName(), changed.getThreadPoolSize()));
        }
    }

    private SSLContext createSslContext()
    {
        KeyStore keyStore = getKeyStore();
        Collection<TrustStore> trustStores = getTrustStores();

        boolean needClientCert = (Boolean)getAttribute(NEED_CLIENT_AUTH) || (Boolean)getAttribute(WANT_CLIENT_AUTH);
        if (needClientCert && trustStores.isEmpty())
        {
            throw new IllegalConfigurationException("Client certificate authentication is enabled on AMQP port '"
                    + this.getName() + "' but no trust store defined");
        }

        SSLContext sslContext = SSLUtil.createSslContext(keyStore, trustStores, getName());
        SSLSessionContext serverSessionContext = sslContext.getServerSessionContext();
        if (getTLSSessionCacheSize() > 0)
        {
            serverSessionContext.setSessionCacheSize(getTLSSessionCacheSize());
        }
        if (getTLSSessionTimeout() > 0)
        {
            serverSessionContext.setSessionTimeout(getTLSSessionTimeout());
        }

        return sslContext;
    }

    private Protocol getDefaultAmqpSupportedReply()
    {
        String defaultAmqpSupportedReply = getContextKeys(false).contains(AmqpPort.PROPERTY_DEFAULT_SUPPORTED_PROTOCOL_REPLY) ?
                getContextValue(String.class, AmqpPort.PROPERTY_DEFAULT_SUPPORTED_PROTOCOL_REPLY) : null;
        Protocol protocol = null;
        if (defaultAmqpSupportedReply != null && defaultAmqpSupportedReply.length() != 0)
        {
            try
            {
                protocol = Protocol.valueOf("AMQP_" + defaultAmqpSupportedReply.substring(1));
            }
            catch(IllegalArgumentException e)
            {
                LOGGER.warn("The configured default reply ({}) is not a valid value for a protocol.  This value will be ignored", defaultAmqpSupportedReply);
            }
        }
        final Set<Protocol> protocolSet = getProtocols();
        if(protocol != null && !protocolSet.contains(protocol))
        {
            LOGGER.warn("The configured default reply ({}) to an unsupported protocol version initiation is not"
                         + " supported on this port.  Only the following versions are supported: {}",
                         defaultAmqpSupportedReply, protocolSet);

            protocol = null;
        }
        return protocol;
    }

    public static Set<Protocol> getInstalledProtocols()
    {
        Set<Protocol> protocols = new HashSet<>();
        for(ProtocolEngineCreator installedEngine : (new QpidServiceLoader()).instancesOf(ProtocolEngineCreator.class))
        {
            protocols.add(installedEngine.getVersion());
        }
        return protocols;
    }

    @SuppressWarnings("unused")
    public static Collection<String> getAllAvailableProtocolCombinations()
    {
        Set<Protocol> protocols = getInstalledProtocols();

        Set<Set<String>> last = new HashSet<>();
        for(Protocol protocol : protocols)
        {
            last.add(Collections.singleton(protocol.name()));
        }

        Set<Set<String>> protocolCombinations = new HashSet<>(last);
        for(int i = 1; i < protocols.size(); i++)
        {
            Set<Set<String>> current = new HashSet<>();
            for(Set<String> set : last)
            {
                for(Protocol p : protocols)
                {
                    if(!set.contains(p.name()))
                    {
                        Set<String> potential = new HashSet<>(set);
                        potential.add(p.name());
                        current.add(potential);
                    }
                }
            }
            protocolCombinations.addAll(current);
            last = current;
        }
        Set<String> combinationsAsString = new HashSet<>(protocolCombinations.size());
        ObjectMapper mapper = new ObjectMapper();
        for(Set<String> combination : protocolCombinations)
        {
            try(StringWriter writer = new StringWriter())
            {
                mapper.writeValue(writer, combination);
                combinationsAsString.add(writer.toString());
            }
            catch (IOException e)
            {
                throw new IllegalArgumentException("Unexpected IO Exception generating JSON string", e);
            }
        }
        return Collections.unmodifiableSet(combinationsAsString);
    }

    @SuppressWarnings("unused")
    public static Collection<String> getAllAvailableTransportCombinations()
    {
        Set<Set<Transport>> combinations = new HashSet<>();

        for(TransportProviderFactory providerFactory : (new QpidServiceLoader()).instancesOf(TransportProviderFactory.class))
        {
            combinations.addAll(providerFactory.getSupportedTransports());
        }

        Set<String> combinationsAsString = new HashSet<>(combinations.size());
        ObjectMapper mapper = new ObjectMapper();
        for(Set<Transport> combination : combinations)
        {
            try(StringWriter writer = new StringWriter())
            {
                mapper.writeValue(writer, combination);
                combinationsAsString.add(writer.toString());
            }
            catch (IOException e)
            {
                throw new IllegalArgumentException("Unexpected IO Exception generating JSON string", e);
            }
        }
        return Collections.unmodifiableSet(combinationsAsString);
    }


    public static String getInstalledProtocolsAsString()
    {
        Set<Protocol> installedProtocols = getInstalledProtocols();
        ObjectMapper mapper = new ObjectMapper();

        try(StringWriter output = new StringWriter())
        {
            mapper.writeValue(output, installedProtocols);
            return output.toString();
        }
        catch (IOException e)
        {
            throw new ServerScopedRuntimeException(e);
        }
    }

    @Override
    public int incrementConnectionCount()
    {
        int openConnections = _connectionCount.incrementAndGet();
        _totalConnectionCount.incrementAndGet();
        int maxOpenConnections = getMaxOpenConnections();
        if(maxOpenConnections > 0
           && openConnections > (maxOpenConnections * _connectionWarnCount) / 100
           && _connectionCountWarningGiven.compareAndSet(false, true))
        {
            _container.getEventLogger().message(new PortLogSubject(this),
                                                PortMessages.CONNECTION_COUNT_WARN(openConnections,
                                                                                _connectionWarnCount,
                                                                                maxOpenConnections));
        }
        return openConnections;
    }

    @Override
    public int decrementConnectionCount()
    {
        int openConnections = _connectionCount.decrementAndGet();
        int maxOpenConnections = getMaxOpenConnections();

        if(maxOpenConnections > 0
           && openConnections < (maxOpenConnections * square(_connectionWarnCount)) / 10000)
        {
           _connectionCountWarningGiven.compareAndSet(true,false);
        }


        return openConnections;
    }

    private static int square(int val)
    {
        return val * val;
    }

    @Override
    public boolean canAcceptNewConnection(final SocketAddress remoteSocketAddress)
    {
        String addressString = remoteSocketAddress.toString();
        if (_closingOrDeleting.get())
        {
            _container.getEventLogger().message(new PortLogSubject(this),
                                                PortMessages.CONNECTION_REJECTED_CLOSED(addressString));
            return false;
        }
        else if (_maxOpenConnections > 0 && _connectionCount.get() >= _maxOpenConnections)
        {
            _container.getEventLogger().message(new PortLogSubject(this),
                                                PortMessages.CONNECTION_REJECTED_TOO_MANY(addressString,
                                                                                       _maxOpenConnections));
            return false;
        }
        else
        {
            return true;
        }
    }

    @Override
    public int getConnectionCount()
    {
        return _connectionCount.get();
    }

    @Override
    public long getTotalConnectionCount()
    {
        return _totalConnectionCount.get();
    }

    @Override
    public long getProtocolHandshakeTimeout()
    {
        return _protocolHandshakeTimeout;
    }

    @Override
    public boolean getCloseWhenNoRoute()
    {
        return _closeWhenNoRoute;
    }

    @Override
    public int getSessionCountLimit()
    {
        return _sessionCountLimit;
    }

    @Override
    public int getHeartbeatDelay()
    {
        return _heartBeatDelay;
    }

    @Override
    public int getTLSSessionTimeout()
    {
        return _tlsSessionTimeout;
    }

    @Override
    public int getTLSSessionCacheSize()
    {
        return _tlsSessionCacheSize;
    }
}
