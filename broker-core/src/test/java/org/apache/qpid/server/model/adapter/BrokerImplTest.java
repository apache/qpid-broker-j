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
package org.apache.qpid.server.model.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerImpl;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.PreferenceFactory;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.SimpleAuthenticationManager;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhostnode.TestVirtualHostNode;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes"})
public class BrokerImplTest extends UnitTestBase
{
    private TaskExecutorImpl _taskExecutor;
    private SystemConfig _systemConfig;
    private BrokerImpl _brokerImpl;
    private PreferenceStore _preferenceStore;

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        _preferenceStore = mock(PreferenceStore.class);
        _systemConfig = BrokerTestHelper.mockWithSystemPrincipal(SystemConfig.class, mock(Principal.class));
        when(_systemConfig.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_systemConfig.getChildExecutor()).thenReturn(_taskExecutor);
        when(_systemConfig.getModel()).thenReturn(BrokerModel.getInstance());
        when(_systemConfig.getEventLogger()).thenReturn(new EventLogger());
        when(_systemConfig.getCategoryClass()).thenReturn(SystemConfig.class);
        when(_systemConfig.createPreferenceStore()).thenReturn(_preferenceStore);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_brokerImpl != null)
        {
            _brokerImpl.close();
        }

        if (_taskExecutor != null)
        {
            _taskExecutor.stopImmediately();
        }
    }

    @Test
    public void testAssignTargetSizes_NoQueueDepth()
    {
        doAssignTargetSizeTest(new long[] {0, 0}, 1024 * 1024 * 1024);
    }

    @Test
    public void testAssignTargetSizes_OneQueue()
    {
        doAssignTargetSizeTest(new long[] {37}, 1024 * 1024 * 1024);
    }

    @Test
    public void testAssignTargetSizes_ThreeQueues()
    {
        doAssignTargetSizeTest(new long[] {37, 47, 0}, 1024 * 1024 * 1024);
    }

    @Test
    public void testAssignTargetSizes_QueuesOversize()
    {
        final int flowToDiskThreshold = 1024 * 1024 * 1024;
        doAssignTargetSizeTest(new long[] {flowToDiskThreshold / 2, flowToDiskThreshold / 2 , 1024}, flowToDiskThreshold);
    }

    @Test
    public void testAssignTargetSizesWithHighQueueDepthAndMemoryLimit()
    {
        final long flowToDiskThreshold = 3L * 1024 * 1024 * 1024;
        doAssignTargetSizeTest(new long[] {4L * 1024 * 1024 * 1024, 0, 0 , 0}, flowToDiskThreshold);
    }

    @Test
    public void testNetworkBufferSize()
    {
        final Map<String, Object> attributes = Map.of(Broker.NAME, "Broker",
                Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION,
                Broker.DURABLE, true,
                Broker.CONTEXT, Map.of(Broker.NETWORK_BUFFER_SIZE, Broker.MINIMUM_NETWORK_BUFFER_SIZE - 1));

        // testing successful case is not possible because of the static nature of QpidByteBuffer which *should* be unrelated

        // testing unsuccessful case
        _brokerImpl = new BrokerImpl(attributes, _systemConfig);
        _brokerImpl.open();
        assertEquals(State.ERRORED, _brokerImpl.getState(),
                "Broker open should fail with network buffer size less then minimum");

        assertEquals(Broker.DEFAULT_NETWORK_BUFFER_SIZE, (long) _brokerImpl.getNetworkBufferSize(),
                "Unexpected buffer size");
    }

    @Test
    public void testPurgeUser()
    {
        final String testUsername = "testUser";
        final String testPassword = "testPassword";

        // setup broker
        final Map<String, Object> brokerAttributes = Map.of("name", "Broker",
                Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION,
                Broker.DURABLE, true);
        _brokerImpl = new BrokerImpl(brokerAttributes, _systemConfig);
        _brokerImpl.open();

        // setup auth provider with testuser
        final Map<String, Object> authProviderAttributes = Map.of(ConfiguredObject.NAME, "testAuthProvider",
                ConfiguredObject.TYPE, "Simple");
        final SimpleAuthenticationManager authenticationProvider = new SimpleAuthenticationManager(authProviderAttributes, _brokerImpl);
        authenticationProvider.create();
        authenticationProvider.addUser(testUsername, testPassword);

        // setup preference owned by testuser
        final UUID preferenceId = randomUUID();
        final Map<String, Object> preferenceAttributes = Map.of(Preference.ID_ATTRIBUTE, preferenceId,
                Preference.NAME_ATTRIBUTE, "testPref",
                Preference.TYPE_ATTRIBUTE, "X-testPrefType",
                Preference.VALUE_ATTRIBUTE, Collections.EMPTY_MAP);
        final Subject testUserSubject = new Subject();
        testUserSubject.getPrincipals()
                       .add(new AuthenticatedPrincipal(new UsernamePrincipal(testUsername, authenticationProvider)));
        testUserSubject.setReadOnly();
        final Collection<Preference> preferences =
                Set.of(PreferenceFactory.fromAttributes(_brokerImpl, preferenceAttributes));
        Subject.doAs(testUserSubject, (PrivilegedAction<Void>) () ->
        {
            try
            {
                _brokerImpl.getUserPreferences().updateOrAppend(preferences).get(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException | ExecutionException | TimeoutException e)
            {
                e.printStackTrace();
                fail("Failed to put preference:");
            }
            return null;
        });

        // test pre-conditions
        final Collection<Preference> preferencesBeforePurge = getPreferencesAs(testUserSubject);
        assertEquals(1, (long) preferencesBeforePurge.size(),
                "Unexpected number of preferences before userPurge");
        assertEquals(preferenceId, preferencesBeforePurge.iterator().next().getId(),
                "Unexpected preference before userPurge");

        assertTrue(authenticationProvider.getUsers().containsKey(testUsername),
                "User was not valid before userPurge");

        _brokerImpl.purgeUser(authenticationProvider, testUsername);

        // test post-conditions
        final Collection<Preference> preferencesAfterPurge = getPreferencesAs(testUserSubject);
        assertEquals(Set.of(), preferencesAfterPurge, "Preferences were not deleted during userPurge");
        assertEquals(Collections.EMPTY_MAP, authenticationProvider.getUsers(),
                "User was not deleted from authentication Provider");
        verify(_preferenceStore).replace(Set.of(preferenceId), Set.of());
    }

    @Test
    public void resetStatistics()
    {
        final Map<String, Object> brokerAttributes = Map.of("name", "Broker",
                Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION,
                Broker.DURABLE, true);
        _brokerImpl = new BrokerImpl(brokerAttributes, _systemConfig);
        _brokerImpl.open();

        _brokerImpl.registerMessageDelivered(100L);
        _brokerImpl.registerMessageReceived(100L);
        _brokerImpl.registerTransactedMessageDelivered();
        _brokerImpl.registerTransactedMessageReceived();

        final Map<String, Object> statisticsBeforeReset = _brokerImpl.getStatistics();
        assertEquals(100L, statisticsBeforeReset.get("bytesIn"));
        assertEquals(100L, statisticsBeforeReset.get("bytesOut"));
        assertEquals(1L, statisticsBeforeReset.get("messagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("messagesOut"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesIn"));
        assertEquals(1L, statisticsBeforeReset.get("transactedMessagesOut"));

        _brokerImpl.resetStatistics();

        final Map<String, Object> statisticsAfterReset = _brokerImpl.getStatistics();
        assertEquals(0L, statisticsAfterReset.get("bytesIn"));
        assertEquals(0L, statisticsAfterReset.get("bytesOut"));
        assertEquals(0L, statisticsAfterReset.get("messagesIn"));
        assertEquals(0L, statisticsAfterReset.get("messagesOut"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesIn"));
        assertEquals(0L, statisticsAfterReset.get("transactedMessagesOut"));
    }

    private Collection<Preference> getPreferencesAs(final Subject testUserSubject)
    {
        return Subject.doAs(testUserSubject, (PrivilegedAction<Collection<Preference>>) () ->
        {
            Collection<Preference> preferences = null;
            try
            {
                preferences = _brokerImpl.getUserPreferences().getPreferences().get(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException | ExecutionException | TimeoutException e)
            {
                e.printStackTrace();
                fail("Failed to put preference:");
            }
            return preferences;
        });
    }

    private void doAssignTargetSizeTest(final long[] virtualHostQueueSizes, final long flowToDiskThreshold)
    {
        final Map<String, Object> attributes = Map.of("name", "Broker",
                Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION,
                Broker.DURABLE, true,
                "context", Map.of(Broker.BROKER_FLOW_TO_DISK_THRESHOLD, flowToDiskThreshold));
        _brokerImpl = new BrokerImpl(attributes, _systemConfig);
        _brokerImpl.open();
        assertEquals(State.ACTIVE, _brokerImpl.getState(), "Unexpected broker state");

        for (int i = 0; i < virtualHostQueueSizes.length; i++)
        {
            createVhnWithVh(_brokerImpl, i, virtualHostQueueSizes[i]);
        }

        long totalAssignedTargetSize = 0;
        for (final VirtualHostNode<?> vhn : _brokerImpl.getVirtualHostNodes())
        {
            final VirtualHost<?> virtualHost = vhn.getVirtualHost();
            if (virtualHost instanceof QueueManagingVirtualHost)
            {
                final long targetSize = ((QueueManagingVirtualHost<?>)virtualHost).getTargetSize();
                assertTrue(targetSize > 0, "A virtualhost's target size cannot be zero");
                totalAssignedTargetSize += targetSize;
            }
        }

        final long diff = Math.abs(flowToDiskThreshold - totalAssignedTargetSize);
        final long tolerance = _brokerImpl.getVirtualHostNodes().size() * 2L;
        assertTrue(diff < tolerance,
                String.format("Assigned target size not within expected tolerance. Diff %d Tolerance %d", diff, tolerance));
    }

    private void createVhnWithVh(final BrokerImpl brokerImpl, final int nameSuffix, final long totalQueueSize)
    {
        final Map<String, Object> vhnAttributes = Map.of(VirtualHostNode.TYPE, TestVirtualHostNode.VIRTUAL_HOST_NODE_TYPE,
                VirtualHostNode.NAME, "testVhn" + nameSuffix);
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        final TestVirtualHostNode vhn = new TestVirtualHostNode(brokerImpl, vhnAttributes, store);
        vhn.create();

        final Map<String, Object> vhAttributes = Map.of(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE,
                VirtualHost.NAME, "testVh" + nameSuffix);
        final TestMemoryVirtualHost vh = new TestMemoryVirtualHost(vhAttributes, vhn)
        {
            @Override
            public long getTotalDepthOfQueuesBytes()
            {
                return totalQueueSize;
            }
        };
        vh.create();
    }
}
