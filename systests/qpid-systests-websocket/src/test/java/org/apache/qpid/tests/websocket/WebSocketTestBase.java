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
 */

package org.apache.qpid.tests.websocket;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener.DefaultSystemLauncherListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostAlias;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.QpidUnitTestExtension;
import org.apache.qpid.test.utils.tls.AltNameType;
import org.apache.qpid.test.utils.tls.AlternativeName;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;

/**
 * Starts an isolated complete broker per invocation. Managed operations run with the launcher's
 * system subject; messaging traverses real network connections and protocol engines.
 */
@ExtendWith(QpidUnitTestExtension.class)
abstract class WebSocketTestBase
{
    protected static final int TIMEOUT_MILLIS = 10000;
    protected static final String VIRTUAL_HOST = "localhost";

    @TempDir
    private Path _workDirectory;

    protected Broker<?> _broker;
    protected QueueManagingVirtualHost<?> _virtualHost;
    protected SSLContext _clientContext;
    private SystemLauncher _launcher;
    private Subject _systemSubject;
    private TlsResource _tls;
    private KeyStore<?> _keyStore;
    private TrustManager[] _clientTrustManagers;

    @BeforeEach
    public void startBroker() throws Exception
    {
        _tls = new TlsResource();
        _launcher = new SystemLauncher(new DefaultSystemLauncherListener()
        {
            @Override
            public void onContainerResolve(final SystemConfig<?> systemConfig)
            {
                _broker = systemConfig.getContainer(Broker.class);
            }
        });
        _launcher.startup(Map.of(SystemConfig.TYPE, "JSON",
                SystemConfig.INITIAL_CONFIGURATION_LOCATION, "classpath:config-websocket-tests.json",
                SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, false,
                SystemConfig.CONTEXT, Map.of("qpid.work_dir", _workDirectory.toString(),
                        "qpid.port.protocol_handshake_timeout", TIMEOUT_MILLIS)));
        _systemSubject = new Subject(true, Set.of(_launcher.getSystemPrincipal()), Set.of(), Set.of());

        final KeyCertificatePair certificate = TlsResourceBuilder.createSelfSigned("CN=localhost",
                new AlternativeName(AltNameType.DNS_NAME, "localhost"),
                new AlternativeName(AltNameType.DNS_NAME, "selected.example"));
        final Path store = _tls.createKeyStore(new PrivateKeyEntry("server", certificate));
        _keyStore = admin(() -> _broker.createChild(KeyStore.class, Map.of(ConfiguredObject.NAME, "server",
                ConfiguredObject.TYPE, "FileKeyStore", FileKeyStore.STORE_URL, store.toUri().toString(),
                FileKeyStore.PASSWORD, _tls.getSecret())));
        final java.security.KeyStore trustStore = TlsResourceHelper.createKeyStore(_tls.getKeyStoreType(),
                _tls.getSecretAsCharacters(), new CertificateEntry("server", certificate.certificate()));
        final TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(trustStore);
        _clientTrustManagers = trust.getTrustManagers();
        _clientContext = createClientContext(null);
        _virtualHost = createVirtualHost(VIRTUAL_HOST);
    }

    @AfterEach
    public void stopBroker()
    {
        try
        {
            if (_launcher != null)
            {
                _launcher.shutdown();
            }
        }
        finally
        {
            if (_tls != null)
            {
                _tls.close();
            }
        }
    }

    protected final SSLContext createClientContext(final KeyManager[] keyManagers) throws GeneralSecurityException
    {
        final SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers, _clientTrustManagers, null);
        return context;
    }

    protected final <T> T admin(final Callable<T> action) throws Exception
    {
        return SubjectExecutionContext.withSubject(_systemSubject, action);
    }

    protected final QueueManagingVirtualHost<?> createVirtualHost(final String name) throws Exception
    {
        return admin(() ->
        {
            final VirtualHostNode<?> node = _broker.createChild(VirtualHostNode.class,
                    Map.of(ConfiguredObject.NAME, name, ConfiguredObject.TYPE, "Memory"));
            return (QueueManagingVirtualHost<?>) node.createChild(VirtualHost.class,
                    Map.of(ConfiguredObject.NAME, name, ConfiguredObject.TYPE, "Memory",
                            QueueManagingVirtualHost.CONNECTION_THREAD_POOL_SIZE, 2,
                            QueueManagingVirtualHost.NUMBER_OF_SELECTORS, 1));
        });
    }

    protected final AmqpPort<?> createPort(final Transport transport) throws Exception
    {
        return createPort(transport, "plain", Map.of());
    }

    protected final AmqpPort<?> createPort(final Transport transport,
                                           final String authentication,
                                           final Map<String, Object> context) throws Exception
    {
        return createPort(transport, authentication, context, Map.of());
    }

    protected final AmqpPort<?> createPort(final Transport transport,
                                           final String authentication,
                                           final Map<String, Object> context,
                                           final Map<String, Object> portAttributes) throws Exception
    {
        return admin(() ->
        {
            final Map<String, Object> attributes = new HashMap<>();
            attributes.put(ConfiguredObject.NAME, "websocket-" + _broker.getPorts().size());
            attributes.put(ConfiguredObject.TYPE, "AMQP");
            attributes.put(Port.PORT, 0);
            attributes.put(Port.BINDING_ADDRESS, "127.0.0.1");
            attributes.put(Port.TRANSPORTS, Set.of(transport));
            attributes.put(Port.PROTOCOLS, Set.of(Protocol.AMQP_1_0));
            attributes.put(Port.AUTHENTICATION_PROVIDER, authentication);
            attributes.put(AmqpPort.THREAD_POOL_SIZE, 2);
            attributes.put(AmqpPort.NUMBER_OF_SELECTORS, 1);
            final Map<String, Object> portContext = new HashMap<>(context);
            portContext.putIfAbsent(AmqpPort.FINAL_WRITE_TIMEOUT, 500);
            portContext.putIfAbsent(org.apache.qpid.server.model.Connection.CLOSE_RESPONSE_TIMEOUT, 250);
            attributes.put(ConfiguredObject.CONTEXT, portContext);
            if (transport == Transport.WSS)
            {
                attributes.put(Port.KEY_STORE, _keyStore);
            }
            attributes.putAll(portAttributes);
            final AmqpPort<?> port = (AmqpPort<?>) _broker.createChild(Port.class, attributes);
            port.createChild(VirtualHostAlias.class, Map.of(ConfiguredObject.NAME, "name",
                    ConfiguredObject.TYPE, "nameAlias"));
            return port;
        });
    }

    protected final JmsConnectionFactory connectionFactory(final AmqpPort<?> port, final String id)
    {
        final String scheme;
        final Transport transport = port.getTransports().iterator().next();
        scheme = switch (transport)
        {
            case TCP -> "amqp";
            case WS -> "amqpws";
            case WSS -> "amqpwss";
            default -> throw new IllegalArgumentException("Unsupported test transport: " + transport);
        };
        final JmsConnectionFactory factory = new JmsConnectionFactory(scheme + "://localhost:" + port.getBoundPort() +
                "/?amqp.vhost=" + VIRTUAL_HOST + "&amqp.saslMechanisms=PLAIN");
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setClientID(id);
        factory.setSslContext(_clientContext);
        factory.setConnectTimeout(TIMEOUT_MILLIS);
        factory.setRequestTimeout(TIMEOUT_MILLIS);
        factory.setSendTimeout(TIMEOUT_MILLIS);
        factory.setCloseTimeout(TIMEOUT_MILLIS);
        factory.setForceSyncSend(true);
        return factory;
    }

    protected final Connection connect(final AmqpPort<?> port, final String id) throws JMSException
    {
        final Connection connection = connectionFactory(port, id).createConnection();
        try
        {
            connection.start();
            return connection;
        }
        catch (JMSException | RuntimeException e)
        {
            connection.close();
            throw e;
        }
    }

    protected final AbstractAMQPConnection<?, ?> brokerConnection(final String id) throws Exception
    {
        await(() -> _broker.getVirtualHostNodes().stream()
                .map(VirtualHostNode::getVirtualHost).filter(Objects::nonNull)
                .anyMatch(v -> v.getConnections().stream().anyMatch(c -> id.equals(c.getClientId()))),
                "Connection was not registered: " + id);
        final AbstractAMQPConnection<?, ?> connection = _broker.getVirtualHostNodes().stream()
                .map(VirtualHostNode::getVirtualHost).filter(Objects::nonNull)
                .flatMap(v -> v.getConnections().stream()).filter(c -> id.equals(c.getClientId()))
                .map(c -> (AbstractAMQPConnection<?, ?>) c).findFirst().orElse(null);
        assertNotNull(connection);
        return connection;
    }

    protected final Queue<?> createQueue(final String name) throws Exception
    {
        return admin(() -> _virtualHost.createChild(Queue.class, Map.of(ConfiguredObject.NAME, name)));
    }

    protected static void await(final BooleanSupplier condition, final String message) throws InterruptedException
    {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline)
        {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), message);
    }
}
