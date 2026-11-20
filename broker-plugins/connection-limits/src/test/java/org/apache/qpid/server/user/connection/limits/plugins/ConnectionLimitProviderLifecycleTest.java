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
package org.apache.qpid.server.user.connection.limits.plugins;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ResourceLimitMessages;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerImpl;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostConnectionLimitProvider;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.security.limit.ConnectionLimitException;
import org.apache.qpid.server.security.limit.ConnectionLimitProvider;
import org.apache.qpid.server.security.limit.ConnectionLimiterService;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhostnode.TestVirtualHostNode;
import org.apache.qpid.test.utils.UnitTestBase;

class ConnectionLimitProviderLifecycleTest extends UnitTestBase
{
    private static final List<Map<String, Object>> BLOCKING_RULES = List.of(Map.of("identity", "ALL", "blocked", true));

    private final Map<UUID, ConfiguredObjectRecord> _records = new LinkedHashMap<>();

    @TempDir
    private Path _temporaryDirectory;

    private TaskExecutor _taskExecutor;
    private BrokerImpl _broker;
    private TestVirtualHostNode _node;

    @BeforeEach
    void setUp()
    {
        initialize(CurrentThreadTaskExecutor.newStartedInstance());
    }

    private void initialize(final TaskExecutor taskExecutor)
    {
        _records.clear();
        _taskExecutor = taskExecutor;
        final SystemConfig<?> systemConfig =
                BrokerTestHelper.mockWithSystemPrincipal(SystemConfig.class, mock(Principal.class));
        when(systemConfig.getTaskExecutor()).thenReturn(_taskExecutor);
        when(systemConfig.getChildExecutor()).thenReturn(_taskExecutor);
        when(systemConfig.getModel()).thenReturn(BrokerModel.getInstance());
        when(systemConfig.getEventLogger()).thenReturn(new EventLogger());
        when(systemConfig.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));

        _broker = new BrokerImpl(Map.of(ConfiguredObject.NAME, getTestName(),
                Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION), systemConfig);
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        _node = new TestVirtualHostNode(_broker, Map.of(ConfiguredObject.NAME, getTestName()), store);
        _node.registerWithParents();

        final ConfiguredObjectRecord hostRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                VirtualHost.class.getSimpleName(),
                Map.of(ConfiguredObject.NAME, getTestName(),
                        ConfiguredObject.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE,
                        VirtualHost.MODEL_VERSION, BrokerModel.MODEL_VERSION),
                Map.of("VirtualHostNode", _node.getId()));
        _records.put(hostRecord.getId(), hostRecord);
        doAnswer(invocation ->
        {
            final ConfiguredObjectRecordHandler handler = invocation.getArgument(0);
            List.copyOf(_records.values()).forEach(handler::handle);
            return false;
        }).when(store).openConfigurationStore(any(ConfiguredObjectRecordHandler.class),
                any(ConfiguredObjectRecord[].class));
        doAnswer(invocation ->
        {
            final ConfiguredObjectRecordHandler handler = invocation.getArgument(0);
            List.copyOf(_records.values()).forEach(handler::handle);
            return null;
        }).when(store).reload(any(ConfiguredObjectRecordHandler.class));
        doAnswer(invocation ->
        {
            final ConfiguredObjectRecord[] records = (ConfiguredObjectRecord[]) invocation.getRawArguments()[1];
            for (final ConfiguredObjectRecord record : records)
            {
                _records.put(record.getId(), record);
            }
            return null;
        }).when(store).update(anyBoolean(), any(ConfiguredObjectRecord[].class));
        when(store.remove(any(ConfiguredObjectRecord[].class))).thenAnswer(invocation ->
        {
            final ConfiguredObjectRecord[] records = (ConfiguredObjectRecord[]) invocation.getRawArguments()[0];
            final UUID[] removed = new UUID[records.length];
            for (int i = 0; i < records.length; i++)
            {
                removed[i] = records[i].getId();
                _records.remove(removed[i]);
            }
            return removed;
        });
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            if (_broker != null)
            {
                _broker.close();
            }
        }
        finally
        {
            _taskExecutor.stop();
        }
    }

    @Test
    void brokerStartupEnforcesRecoveredProvider()
    {
        final RuleBasedBrokerConnectionLimitProviderImpl provider = recoverBrokerProvider(BLOCKING_RULES, State.ACTIVE);

        _broker.open();

        assertEquals(State.ACTIVE, _broker.getState());
        assertEquals(State.ACTIVE, provider.getState());
        assertEquals(State.ACTIVE, _node.getVirtualHost().getState());
        assertConnectionRejected();
    }

    @Test
    void lateActivationFailureBlocksConnections()
    {
        final RuleBasedBrokerConnectionLimitProviderImpl provider =
                recoverBrokerProvider(List.of(Map.of("identity", "ALL")), State.QUIESCED);
        _broker.open();
        assertEquals(State.QUIESCED, provider.getState());
        assertConnectionAccepted();

        assertThrows(IllegalArgumentException.class, provider::start);

        assertEquals(State.ERRORED, provider.getState());
        assertConnectionRejected();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void recoveryPreservesCountersBetweenActivationNotifications(final boolean frequencyLimited)
    {
        final RuleBasedBrokerConnectionLimitProviderImpl provider =
                recoverBrokerProvider(List.of(Map.of("identity", "ALL")), State.QUIESCED);
        _broker.open();
        assertThrows(IllegalArgumentException.class, provider::start);
        assertEquals(State.ERRORED, provider.getState());
        assertEquals(State.ACTIVE, provider.getDesiredState());

        final Map<String, Object> rule = frequencyLimited
                ? Map.of("identity", "ALL", "frequencyLimit", 1, "frequencyPeriod", 3_600_000L)
                : Map.of("identity", "ALL", "countLimit", 1);
        provider.setAttributes(Map.of("rules", List.of(rule)));

        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        final AMQPConnection<?> connection = newConnection();
        final AtomicBoolean connectionRegistered = new AtomicBoolean();
        final ConfigurationChangeListener listener = new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
            {
                if (newState == State.ACTIVE && connectionRegistered.compareAndSet(false, true))
                {
                    host.registerConnection(connection);
                    if (frequencyLimited)
                    {
                        host.deregisterConnection(connection);
                    }
                    assertConnectionRejected();
                }
            }
        };
        provider.addChangeListener(listener);
        try
        {
            provider.start();

            assertEquals(State.ACTIVE, provider.getState());
            assertTrue(connectionRegistered.get());
            assertConnectionRejected();
        }
        finally
        {
            provider.removeChangeListener(listener);
            if (!frequencyLimited)
            {
                host.deregisterConnection(connection);
            }
        }
        if (!frequencyLimited)
        {
            assertConnectionAccepted();
        }
    }

    @Test
    void restartAppliesBrokerRulesChangedWhileStopped()
    {
        final RuleBasedBrokerConnectionLimitProviderImpl provider = recoverBrokerProvider(List.of(), State.ACTIVE);
        _broker.open();
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        assertConnectionAccepted();

        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.STOPPED));
        provider.setAttributes(Map.of("rules", BLOCKING_RULES));
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.ACTIVE));

        assertEquals(State.ACTIVE, host.getState());
        assertConnectionRejected();
    }

    @Test
    void providerCreatedAfterRestartEnforced()
    {
        _broker.open();
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.STOPPED));
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.ACTIVE));
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "host-limits",
                ConfiguredObject.TYPE, "RuleBased",
                "rules", BLOCKING_RULES);
        final VirtualHostConnectionLimitProvider<?> provider = host
                .createChild(VirtualHostConnectionLimitProvider.class, attributes);

        assertEquals(State.ACTIVE, provider.getState());
        assertConnectionRejected();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void recoveredProviderEditsAfterRestart(final boolean initiallyBlocking)
    {
        _broker.open();
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "host-limits",
                ConfiguredObject.TYPE, "RuleBased",
                "rules", initiallyBlocking ? BLOCKING_RULES : List.of());
        host.createChild(VirtualHostConnectionLimitProvider.class, attributes);
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.STOPPED));
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.ACTIVE));

        final VirtualHostConnectionLimitProvider<?> provider = host
                .getChildren(VirtualHostConnectionLimitProvider.class).iterator().next();
        assertEquals(State.ACTIVE, provider.getState());
        if (initiallyBlocking)
        {
            assertConnectionRejected();
        }
        else
        {
            assertConnectionAccepted();
        }

        provider.setAttributes(Map.of("rules", initiallyBlocking ? List.of() : BLOCKING_RULES));

        if (initiallyBlocking)
        {
            assertConnectionAccepted();
        }
        else
        {
            assertConnectionRejected();
        }
    }

    @Test
    void restartRestoresServiceLimiter()
    {
        final ConnectionLimiterService service = mock(ConnectionLimiterService.class);
        when(service.register(any(AMQPConnection.class))).thenThrow(new ConnectionLimitException("Service rejects"));
        try (final MockedConstruction<QpidServiceLoader> ignored = mockConstruction(QpidServiceLoader.class,
                withSettings().defaultAnswer(CALLS_REAL_METHODS), (loader, context) ->
                        doReturn(List.of(service)).when(loader).instancesOf(ConnectionLimiterService.class)))
        {
            _broker.open();
            final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
            assertConnectionRejected();

            host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.STOPPED));
            host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.ACTIVE));

            assertEquals(State.ACTIVE, host.getState());
            assertConnectionRejected();
        }
    }

    @Test
    void restartRestoresFileReloadAndProviderRemoval() throws Exception
    {
        final Path file = _temporaryDirectory.resolve("limits.clt");
        Files.writeString(file, "CLT ALL connection_limit=10");
        _broker.open();
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "file-limits",
                ConfiguredObject.TYPE, "ConnectionLimitFile",
                "path", file.toUri().toString());

        host.createChild(VirtualHostConnectionLimitProvider.class,attributes);
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.STOPPED));
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.ACTIVE));
        assertConnectionAccepted();

        final VirtualHostConnectionLimitProvider<?> provider = host
                .getChildren(VirtualHostConnectionLimitProvider.class).iterator().next();
        Files.writeString(file, "CLT ALL BLOCK");
        ((FileBasedConnectionLimitProvider<?>) provider).reload();
        assertConnectionRejected();

        provider.delete();
        assertConnectionAccepted();
    }

    @Test
    void restartRestoresCounterReset()
    {
        _broker.open();
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        final Map<String, Object> rules = Map.of("identity", "ALL",
                "frequencyLimit", 1,
                "frequencyPeriod", 3_600_000L);
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "host-limits",
                ConfiguredObject.TYPE, "RuleBased",
                "rules", List.of(rules));
        host.createChild(VirtualHostConnectionLimitProvider.class, attributes);
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.STOPPED));
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.ACTIVE));
        assertConnectionAccepted();
        assertConnectionRejected();

        final RuleBasedConnectionLimitProvider<?> provider = (RuleBasedConnectionLimitProvider<?>) host
                .getChildren(VirtualHostConnectionLimitProvider.class).iterator().next();
        provider.resetCounters();

        assertConnectionAccepted();
    }

    @Test
    void restartWithSeparateConfigurationThreads()
    {
        _broker.close();
        _taskExecutor.stop();
        final TaskExecutor taskExecutor = new TaskExecutorImpl();
        taskExecutor.start();
        initialize(taskExecutor);
        final RuleBasedBrokerConnectionLimitProviderImpl provider = recoverBrokerProvider(List.of(), State.ACTIVE);
        _broker.open();
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        assertConnectionAccepted();

        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.STOPPED));
        provider.setAttributes(Map.of("rules", BLOCKING_RULES));
        host.setAttributes(Map.of(ConfiguredObject.DESIRED_STATE, State.ACTIVE));
        assertConnectionRejected();

        provider.setAttributes(Map.of("rules", List.of()));
        assertConnectionAccepted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ruleViolationRejectionLogged(final boolean frequencyLimited)
    {
        final Map<String, Object> rule = frequencyLimited
                ? Map.of("identity", "ALL", "frequencyLimit", 1, "frequencyPeriod", 3_600_000L)
                : Map.of("identity", "ALL", "countLimit", 1);
        startRuleProvider(true, List.of(rule));
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        final AMQPConnection<?> connection = newConnection();
        host.registerConnection(connection);
        try
        {
            final EventLogger eventLogger = mock(EventLogger.class);
            final AMQPConnection<?> rejectedConnection = newConnection();
            when(rejectedConnection.getEventLogger()).thenReturn(eventLogger);

            final ConnectionLimitException exception = assertThrows(ConnectionLimitException.class, () ->
                    host.registerConnection(rejectedConnection));

            final String reason = frequencyLimited
                    ? "User user breaks connection frequency limit 1 per 3600 s on port amqp"
                    : "User user breaks connection count limit 1 on port amqp";
            assertEquals(reason, exception.getMessage());
            verify(eventLogger).message(ResourceLimitMessages.REJECTED(
                    "Opening", "connection", "user", "Limiter 'broker-limits': " + reason));
        }
        finally
        {
            host.deregisterConnection(connection);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidRuleUpdatePreservesBlockingRules(final boolean brokerProvider)
    {
        final ConnectionLimitProvider<?> provider = startRuleProvider(brokerProvider, BLOCKING_RULES);
        final Object rules = provider.getActualAttributes().get("rules");
        assertConnectionRejected();

        final Map<String, Object> attributes = Map.of("rules", List.of(Map.of("identity", "ALL")));
        assertThrows(IllegalConfigurationException.class, () -> provider.setAttributes(attributes));

        assertEquals(State.ACTIVE, provider.getState());
        assertEquals(rules, provider.getActualAttributes().get("rules"));
        assertConnectionRejected();

        provider.setAttributes(Map.of("rules", List.of()));
        assertConnectionAccepted();
        provider.setAttributes(Map.of("rules", BLOCKING_RULES));
        assertConnectionRejected();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidRuleUpdatePreservesConnectionCount(final boolean brokerProvider)
    {
        final List<Map<String, Object>> attributes = List.of(Map.of("identity", "ALL", "countLimit", 1));
        final ConnectionLimitProvider<?> provider = startRuleProvider(brokerProvider, attributes);
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        final AMQPConnection<?> connection = newConnection();
        host.registerConnection(connection);
        try
        {
            assertConnectionRejected();
            final Map<String, Object> rules = Map.of("rules", List.of(Map.of("identity", "ALL", "countLimit", 10),
                    Map.of("identity", "ALL")));
            assertThrows(IllegalConfigurationException.class, () -> provider.setAttributes(rules));

            assertConnectionRejected();
        }
        finally
        {
            host.deregisterConnection(connection);
        }
        assertConnectionAccepted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidRuleUpdatePreservesFrequencyCount(final boolean brokerProvider)
    {
        final List<Map<String, Object>> attributes = List.of(Map.of("identity", "ALL",
                "frequencyLimit", 1,
                "frequencyPeriod", 3_600_000L));
        final ConnectionLimitProvider<?> provider = startRuleProvider(brokerProvider, attributes);
        assertConnectionAccepted();
        assertConnectionRejected();

        assertThrows(IllegalConfigurationException.class, () ->
                provider.setAttributes(Map.of("rules", List.of(Map.of("identity", "ALL")))));

        assertConnectionRejected();
        provider.setAttributes(Map.of("rules", List.of(Map.of("identity", "ALL", "frequencyLimit", 2,
                "frequencyPeriod", 3_600_000L))));
        assertConnectionAccepted();
    }

    @Test
    void failedFileReloadPreservesCompiledRules() throws Exception
    {
        final Path file = _temporaryDirectory.resolve("limits.clt");
        Files.writeString(file, "CLT ALL BLOCK");
        _broker.open();
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, "file-limits",
                ConfiguredObject.TYPE, "ConnectionLimitFile",
                "path", file.toUri().toString());
        final VirtualHostConnectionLimitProvider<?> provider = _node.getVirtualHost()
                .createChild(VirtualHostConnectionLimitProvider.class, attributes);
        final FileBasedConnectionLimitProvider<?> fileProvider = (FileBasedConnectionLimitProvider<?>) provider;
        assertConnectionRejected();
        Files.writeString(file, "CLT ALL connection_limit=invalid");

        assertThrows(IllegalConfigurationException.class, fileProvider::reload);

        assertEquals(State.ACTIVE, provider.getState());
        assertThrows(ConnectionLimitException.class, () -> provider.getConnectionLimiter().register(newConnection()));
        assertConnectionRejected();

        Files.writeString(file, "CLT ALL connection_limit=10");
        fileProvider.reload();
        assertConnectionAccepted();
    }

    private ConnectionLimitProvider<?> startRuleProvider(final boolean brokerProvider,
                                                         final List<Map<String, Object>> rules)
    {
        if (brokerProvider)
        {
            final RuleBasedBrokerConnectionLimitProviderImpl provider = recoverBrokerProvider(rules, State.ACTIVE);
            _broker.open();
            return provider;
        }
        _broker.open();
        return _node.getVirtualHost().createChild(VirtualHostConnectionLimitProvider.class,
                Map.of(ConfiguredObject.NAME, "host-limits", ConfiguredObject.TYPE, "RuleBased", "rules", rules));
    }

    private RuleBasedBrokerConnectionLimitProviderImpl recoverBrokerProvider(final List<Map<String, Object>> rules,
                                                                             final State desiredState)
    {
        final RuleBasedBrokerConnectionLimitProviderImpl provider = new RuleBasedBrokerConnectionLimitProviderImpl(
                Map.of(ConfiguredObject.NAME, "broker-limits", "rules", rules,
                        ConfiguredObject.DESIRED_STATE, desiredState), _broker);
        provider.registerWithParents();
        return provider;
    }

    private void assertConnectionAccepted()
    {
        final QueueManagingVirtualHost<?> host = _node.getVirtualHost();
        final AMQPConnection<?> connection = newConnection();
        host.registerConnection(connection);
        host.deregisterConnection(connection);
    }

    private void assertConnectionRejected()
    {
        assertThrows(ConnectionLimitException.class,
                () -> _node.getVirtualHost().registerConnection(newConnection()));
    }

    private AMQPConnection<?> newConnection()
    {
        final AMQPConnection<?> connection = mock(AMQPConnection.class);
        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getName()).thenReturn("amqp");
        doReturn(port).when(connection).getPort();
        when(connection.getAuthorizedPrincipal()).thenReturn(() -> "user");
        when(connection.getPrincipal()).thenReturn("user");
        when(connection.getEventLogger()).thenReturn(_broker.getEventLogger());
        return connection;
    }
}
