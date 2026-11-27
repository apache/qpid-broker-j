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
package org.apache.qpid.server.virtualhost;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.messages.ResourceLimitMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerConnectionLimitProvider;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostConnectionLimitProvider;
import org.apache.qpid.server.security.limit.ConnectionLimitException;
import org.apache.qpid.server.security.limit.ConnectionLimitProvider;
import org.apache.qpid.server.security.limit.ConnectionLimiter;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

class VirtualHostConnectionLimiterTest extends UnitTestBase
{
    private Broker<?> _broker;
    private VirtualHost<?> _virtualHost;
    private SystemConfig<?> _systemConfig;
    private VirtualHostConnectionLimiter _limiter;
    private EventLogger _eventLogger;

    @BeforeEach
    void setUp()
    {
        _broker = mock(Broker.class);
        _virtualHost = mock(VirtualHost.class);
        _systemConfig = mock(SystemConfig.class);
        _eventLogger = mock(EventLogger.class);
        doReturn(_systemConfig).when(_broker).getParent();
        doReturn(Broker.class).when(_broker).getCategoryClass();
        doReturn(VirtualHost.class).when(_virtualHost).getCategoryClass();
        _limiter = new VirtualHostConnectionLimiter(_virtualHost, _broker);
    }

    @AfterEach
    void tearDown()
    {
        _limiter.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void activeProviderEnforced(final boolean brokerProvider)
    {
        createProvider(brokerProvider, State.ACTIVE, ConnectionLimiter.blockEveryone());
        _limiter.open();
        _limiter.activate();

        assertConnectionRejected();
    }

    @ParameterizedTest
    @CsvSource({
            "false, UNINITIALIZED, initializing, RL-1004, rejected_initializing",
            "true, UNINITIALIZED, initializing, RL-1004, rejected_initializing",
            "false, ERRORED, errored, RL-1005, rejected_errored",
            "true, ERRORED, errored, RL-1005, rejected_errored"
    })
    void providerLifecycleRejectionLogged(final boolean brokerProvider,
                                          final State state,
                                          final String description,
                                          final String eventCode,
                                          final String logHierarchy)
    {
        final ConnectionLimitProvider<?> provider = createProvider(brokerProvider, state, ConnectionLimiter.noLimits());
        when(provider.getName()).thenReturn("limits");
        when(_virtualHost.getName()).thenReturn("test-host");
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();

        final ConnectionLimitException exception = assertConnectionRejected();

        final String reason = String.format("Connection limit provider 'limits' (%s) is %s (state %s); " +
                "new connections to virtual host 'test-host' are rejected",
                brokerProvider ? "BrokerConnectionLimitProvider" : "VirtualHostConnectionLimitProvider",
                description, state);
        assertEquals(reason, exception.getMessage());
        final ArgumentCaptor<LogMessage> captor = ArgumentCaptor.forClass(LogMessage.class);
        verify(_eventLogger).message(captor.capture());
        assertEquals(eventCode + " : Rejected : Opening connection by user : " + reason,
                captor.getValue().toString());
        assertEquals("qpid.message.resourcelimit." + logHierarchy, captor.getValue().getLogHierarchy());
        verify(provider, never()).getConnectionLimiter();

        when(provider.getState()).thenReturn(State.ACTIVE);
        listener.stateChanged(provider, state, State.ACTIVE);

        assertConnectionAccepted();
        verifyNoMoreInteractions(_eventLogger);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void initializingProviderBlocksUntilActivation(final boolean brokerProvider)
    {
        final ConnectionLimitProvider<?> provider = createProvider(brokerProvider, State.UNINITIALIZED,
                ConnectionLimiter.noLimits());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();

        assertConnectionRejected();
        verify(provider, never()).getConnectionLimiter();

        when(provider.getState()).thenReturn(State.ACTIVE);
        listener.stateChanged(provider, State.UNINITIALIZED, State.ACTIVE);

        assertConnectionAccepted();
        verify(provider).getConnectionLimiter();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void delayedBlockingProviderEnforced(final boolean brokerProvider)
    {
        final ConnectionLimitProvider<?> provider = createProvider(brokerProvider, State.UNINITIALIZED,
                ConnectionLimiter.blockEveryone());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();

        when(provider.getState()).thenReturn(State.ACTIVE);
        listener.stateChanged(provider, State.UNINITIALIZED, State.ACTIVE);

        assertConnectionRejected();
        verify(provider).getConnectionLimiter();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void activationFailureWithoutNotificationRemainsBlocked(final boolean brokerProvider)
    {
        final ConnectionLimitProvider<?> provider = createProvider(brokerProvider, State.UNINITIALIZED,
                ConnectionLimiter.noLimits());
        _limiter.open();
        _limiter.activate();

        when(provider.getState()).thenReturn(State.ERRORED);

        final ConnectionLimitException exception = assertConnectionRejected();
        assertTrue(exception.getMessage().contains("is errored (state ERRORED)"));
        verify(_eventLogger).message(ResourceLimitMessages
                .REJECTED_ERRORED("Opening", "connection", "user", exception.getMessage()));
        verifyNoMoreInteractions(_eventLogger);
        verify(provider, never()).getConnectionLimiter();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void providerErrorAndRecoveryInvalidateCachedLimiter(final boolean brokerProvider)
    {
        final ConnectionLimitProvider<?> provider = createProvider(brokerProvider, State.ACTIVE, ConnectionLimiter.noLimits());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();
        assertConnectionAccepted();

        when(provider.getState()).thenReturn(State.ERRORED);
        listener.stateChanged(provider, State.ACTIVE, State.ERRORED);
        assertConnectionRejected();

        when(provider.getState()).thenReturn(State.ACTIVE);
        when(provider.getConnectionLimiter()).thenReturn(ConnectionLimiter.blockEveryone());
        listener.stateChanged(provider, State.ERRORED, State.ACTIVE);
        assertConnectionRejected();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void quiescedProviderIgnored(final boolean brokerProvider)
    {
        final ConnectionLimitProvider<?> provider = createProvider(brokerProvider, State.UNINITIALIZED,
                ConnectionLimiter.blockEveryone());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();

        when(provider.getState()).thenReturn(State.QUIESCED);
        listener.stateChanged(provider, State.UNINITIALIZED, State.QUIESCED);

        assertConnectionAccepted();
        verify(provider, never()).getConnectionLimiter();
    }

    @Test
    void managementModeIgnoresProviders()
    {
        when(_systemConfig.isManagementMode()).thenReturn(true);
        final ConnectionLimitProvider<?> provider = createProvider(true, State.UNINITIALIZED, ConnectionLimiter.blockEveryone());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();
        assertConnectionAccepted();

        when(provider.getState()).thenReturn(State.ERRORED);
        listener.stateChanged(provider, State.UNINITIALIZED, State.ERRORED);

        assertConnectionAccepted();
        verify(provider, never()).getConnectionLimiter();
    }

    @Test
    void reopenUsesCurrentBrokerRules()
    {
        final ConnectionLimitProvider<?> provider = createProvider(true, State.ACTIVE, ConnectionLimiter.noLimits());
        _limiter.open();
        _limiter.activate();
        assertConnectionAccepted();
        _limiter.close();

        when(provider.getConnectionLimiter()).thenReturn(ConnectionLimiter.blockEveryone());
        _limiter.open();
        _limiter.activate();

        assertConnectionRejected();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeDetachesListenersFromProvidersNoLongerInChildren(final boolean brokerProvider)
    {
        final ConnectionLimitProvider<?> provider = createProvider(brokerProvider, State.ACTIVE, ConnectionLimiter.noLimits());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();
        doReturn(List.of()).when(_broker).getChildren(BrokerConnectionLimitProvider.class);
        doReturn(List.of()).when(_virtualHost).getChildren(VirtualHostConnectionLimitProvider.class);

        _limiter.close();

        verify(provider).removeChangeListener(listener);
    }

    @Test
    void closedLimiterIgnoresProviderNotifications()
    {
        final ConnectionLimitProvider<?> provider = createProvider(true, State.ACTIVE, ConnectionLimiter.blockEveryone());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();
        _limiter.close();

        listener.bulkChangeStart(provider);
        listener.bulkChangeEnd(provider);

        assertConnectionAccepted();
    }

    @Test
    void reopenedLimiterIgnoresOldProviderListener()
    {
        final ConnectionLimitProvider<?> provider = createProvider(true, State.ACTIVE, ConnectionLimiter.noLimits());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);
        _limiter.activate();
        _limiter.close();
        _limiter.open();
        _limiter.activate();

        when(provider.getConnectionLimiter()).thenReturn(ConnectionLimiter.blockEveryone());
        when(provider.getState()).thenReturn(State.ERRORED);
        listener.stateChanged(provider, State.ACTIVE, State.ERRORED);
        assertConnectionAccepted();

        when(provider.getState()).thenReturn(State.ACTIVE);
        listener.stateChanged(provider, State.ERRORED, State.ACTIVE);
        listener.bulkChangeStart(provider);
        listener.bulkChangeEnd(provider);

        assertConnectionAccepted();
        verify(provider, times(2)).getConnectionLimiter();
    }

    @Test
    void repeatedReopenDoesNotRetainOldProviders() throws Exception
    {
        for (int i = 0; i < 3; i++)
        {
            final ConnectionLimitProvider<?> provider = createProvider(false, State.ACTIVE, ConnectionLimiter.noLimits());
            _limiter.open();
            final ConfigurationChangeListener listener = getProviderListener(provider);
            _limiter.activate();
            assertEquals(1, mapSize("_connectionLimitProviders"));

            _limiter.close();

            assertEquals(0, mapSize("_connectionLimitProviders"));
            assertEquals(0, mapSize("_providerListeners"));
            verify(provider).removeChangeListener(listener);
        }
    }

    @Test
    void openIsIdempotent()
    {
        final ConnectionLimitProvider<?> provider = createProvider(true, State.ACTIVE, ConnectionLimiter.noLimits());
        _limiter.open();
        _limiter.open();
        _limiter.activate();

        verify(_virtualHost, times(1)).addChangeListener(any());
        verify(_broker, times(1)).addChangeListener(any());
        getProviderListener(provider);
        assertConnectionAccepted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeIsIdempotent(final boolean brokerProvider)
    {
        final ConnectionLimitProvider<?> provider = createProvider(brokerProvider, State.ACTIVE, ConnectionLimiter.noLimits());
        _limiter.open();
        final ConfigurationChangeListener listener = getProviderListener(provider);

        _limiter.close();
        _limiter.close();

        verify(_virtualHost, times(1)).removeChangeListener(any());
        verify(_broker, times(1)).removeChangeListener(any());
        verify(provider).removeChangeListener(listener);
    }

    @Test
    void closeBeforeOpenDoesNotDetachListeners()
    {
        _limiter.close();

        verify(_virtualHost, never()).removeChangeListener(any());
        verify(_broker, never()).removeChangeListener(any());
    }

    @Test
    void closedLimiterIgnoresParentNotifications()
    {
        _limiter.open();
        final ArgumentCaptor<ConfigurationChangeListener> captor = ArgumentCaptor.captor();
        verify(_broker).addChangeListener(captor.capture());
        _limiter.close();
        final ConnectionLimitProvider<?> provider = createProvider(true, State.ACTIVE, ConnectionLimiter.blockEveryone());

        captor.getValue().childAdded(_broker, provider);

        verify(provider, never()).addChangeListener(any());
        assertConnectionAccepted();
    }

    private int mapSize(final String name) throws Exception
    {
        final Field field = VirtualHostConnectionLimiter.class.getDeclaredField(name);
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(_limiter)).size();
    }

    private ConnectionLimitProvider<?> createProvider(final boolean brokerProvider,
                                                      final State state,
                                                      final ConnectionLimiter limiter)
    {
        final ConnectionLimitProvider<?> provider;
        if (brokerProvider)
        {
            final BrokerConnectionLimitProvider<?> brokerLimitProvider = mock();
            doReturn(BrokerConnectionLimitProvider.class).when(brokerLimitProvider).getCategoryClass();
            doReturn(List.of(brokerLimitProvider)).when(_broker).getChildren(BrokerConnectionLimitProvider.class);
            provider = brokerLimitProvider;
        }
        else
        {
            final VirtualHostConnectionLimitProvider<?> hostLimitProvider = mock();
            doReturn(VirtualHostConnectionLimitProvider.class).when(hostLimitProvider).getCategoryClass();
            doReturn(List.of(hostLimitProvider))
                    .when(_virtualHost).getChildren(VirtualHostConnectionLimitProvider.class);
            provider = hostLimitProvider;
        }
        when(provider.getState()).thenReturn(state);
        when(provider.getConnectionLimiter()).thenReturn(limiter);
        return provider;
    }

    private ConfigurationChangeListener getProviderListener(final ConnectionLimitProvider<?> provider)
    {
        final ArgumentCaptor<ConfigurationChangeListener> captor = ArgumentCaptor.captor();
        verify(provider).addChangeListener(captor.capture());
        return captor.getValue();
    }

    private void assertConnectionAccepted()
    {
        _limiter.register(mock(AMQPConnection.class)).free();
    }

    private ConnectionLimitException assertConnectionRejected()
    {
        final AMQPConnection<?> connection = mock();
        when(connection.getEventLogger()).thenReturn(_eventLogger);
        when(connection.getPrincipal()).thenReturn("user");
        return assertThrows(ConnectionLimitException.class, () -> _limiter.register(connection));
    }
}
