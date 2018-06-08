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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

public class BrokerImplTest extends UnitTestBase
{
    private TaskExecutorImpl _taskExecutor;
    private SystemConfig _systemConfig;
    private BrokerImpl _brokerImpl;
    private PreferenceStore _preferenceStore;

    @Before
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

    @After
    public void tearDown() throws Exception
    {
        try
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
        finally
        {
        }
    }

    @Test
    public void testAssignTargetSizes_NoQueueDepth() throws Exception
    {
        doAssignTargetSizeTest(new long[] {0, 0}, 1024 * 1024 * 1024);
    }

    @Test
    public void testAssignTargetSizes_OneQueue() throws Exception
    {
        doAssignTargetSizeTest(new long[] {37}, 1024 * 1024 * 1024);
    }

    @Test
    public void testAssignTargetSizes_ThreeQueues() throws Exception
    {
        doAssignTargetSizeTest(new long[] {37, 47, 0}, 1024 * 1024 * 1024);
    }

    @Test
    public void testAssignTargetSizes_QueuesOversize() throws Exception
    {
        int flowToDiskThreshold = 1024 * 1024 * 1024;
        doAssignTargetSizeTest(new long[] {flowToDiskThreshold / 2, flowToDiskThreshold / 2 , 1024},
                               flowToDiskThreshold);
    }

    @Test
    public void testAssignTargetSizesWithHighQueueDepthAndMemoryLimit() throws Exception
    {
        long flowToDiskThreshold = 3L * 1024 * 1024 * 1024;
        doAssignTargetSizeTest(new long[] {4L * 1024 * 1024 * 1024, 0, 0 , 0}, flowToDiskThreshold);
    }

    @Test
    public void testNetworkBufferSize()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Broker.NAME, "Broker");
        attributes.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        attributes.put(Broker.DURABLE, true);

        // testing successful case is not possible because of the static nature of QpidByteBuffer which *should* be unrelated

        // testing unsuccessful case
        attributes.put(Broker.CONTEXT, Collections.singletonMap(Broker.NETWORK_BUFFER_SIZE, Broker.MINIMUM_NETWORK_BUFFER_SIZE - 1));
        _brokerImpl = new BrokerImpl(attributes, _systemConfig);
        _brokerImpl.open();
        assertEquals("Broker open should fail with network buffer size less then minimum",
                            State.ERRORED,
                            _brokerImpl.getState());

        assertEquals("Unexpected buffer size",
                            (long) Broker.DEFAULT_NETWORK_BUFFER_SIZE,
                            (long) _brokerImpl.getNetworkBufferSize());

    }

    @Test
    public void testPurgeUser() throws Exception
    {
        final String testUsername = "testUser";
        final String testPassword = "testPassword";

        // setup broker
        Map<String, Object> brokerAttributes = new HashMap<>();
        brokerAttributes.put("name", "Broker");
        brokerAttributes.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        brokerAttributes.put(Broker.DURABLE, true);
        _brokerImpl = new BrokerImpl(brokerAttributes, _systemConfig);
        _brokerImpl.open();

        // setup auth provider with testuser
        final Map<String, Object> authProviderAttributes = new HashMap<>();
        authProviderAttributes.put(ConfiguredObject.NAME, "testAuthProvider");
        authProviderAttributes.put(ConfiguredObject.TYPE, "Simple");
        SimpleAuthenticationManager authenticationProvider = new SimpleAuthenticationManager(authProviderAttributes, _brokerImpl);
        authenticationProvider.create();
        authenticationProvider.addUser(testUsername, testPassword);

        // setup preference owned by testuser
        final Map<String, Object> preferenceAttributes = new HashMap<>();
        UUID preferenceId = UUID.randomUUID();
        preferenceAttributes.put(Preference.ID_ATTRIBUTE, preferenceId);
        preferenceAttributes.put(Preference.NAME_ATTRIBUTE, "testPref");
        preferenceAttributes.put(Preference.TYPE_ATTRIBUTE, "X-testPrefType");
        preferenceAttributes.put(Preference.VALUE_ATTRIBUTE, Collections.EMPTY_MAP);
        Subject testUserSubject = new Subject();
        testUserSubject.getPrincipals()
                       .add(new AuthenticatedPrincipal(new UsernamePrincipal(testUsername, authenticationProvider)));
        testUserSubject.setReadOnly();
        final Collection<Preference> preferences =
                Collections.singleton(PreferenceFactory.fromAttributes(_brokerImpl, preferenceAttributes));
        Subject.doAs(testUserSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
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
            }
        });

        // test pre-conditions
        Collection<Preference> preferencesBeforePurge = getPreferencesAs(testUserSubject);
        assertEquals("Unexpected number of preferences before userPurge",
                            (long) 1,
                            (long) preferencesBeforePurge.size());
        assertEquals("Unexpected preference before userPurge",
                            preferenceId,
                            preferencesBeforePurge.iterator().next().getId());

        assertTrue("User was not valid before userPurge",
                          authenticationProvider.getUsers().containsKey(testUsername));

        _brokerImpl.purgeUser(authenticationProvider, testUsername);

        // test post-conditions
        Collection<Preference> preferencesAfterPurge = getPreferencesAs(testUserSubject);
        assertEquals("Preferences were not deleted during userPurge",
                            Collections.EMPTY_SET,
                            preferencesAfterPurge);
        assertEquals("User was not deleted from authentication Provider",
                            Collections.EMPTY_MAP,
                            authenticationProvider.getUsers());
        verify(_preferenceStore).replace(Collections.singleton(preferenceId), Collections.EMPTY_SET);
    }

    private Collection<Preference> getPreferencesAs(final Subject testUserSubject)
    {
        return Subject.doAs(testUserSubject, new PrivilegedAction<Collection<Preference>>()
            {
                @Override
                public Collection<Preference> run()
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
                }
            });
    }

    private void doAssignTargetSizeTest(final long[] virtualHostQueueSizes, final long flowToDiskThreshold)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "Broker");
        attributes.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        attributes.put(Broker.DURABLE, true);
        attributes.put("context", Collections.singletonMap(Broker.BROKER_FLOW_TO_DISK_THRESHOLD, flowToDiskThreshold));
        _brokerImpl = new BrokerImpl(attributes, _systemConfig);
        _brokerImpl.open();
        assertEquals("Unexpected broker state", State.ACTIVE, _brokerImpl.getState());

        for(int i=0; i < virtualHostQueueSizes.length; i++)
        {
            createVhnWithVh(_brokerImpl, i, virtualHostQueueSizes[i]);
        }

        long totalAssignedTargetSize = 0;
        for(VirtualHostNode<?> vhn : _brokerImpl.getVirtualHostNodes())
        {
            VirtualHost<?> virtualHost = vhn.getVirtualHost();
            if(virtualHost instanceof QueueManagingVirtualHost)
            {
                long targetSize = ((QueueManagingVirtualHost<?>)virtualHost).getTargetSize();
                assertTrue("A virtualhost's target size cannot be zero", targetSize > 0);
                totalAssignedTargetSize += targetSize;
            }
        }

        long diff = Math.abs(flowToDiskThreshold - totalAssignedTargetSize);
        long tolerance = _brokerImpl.getVirtualHostNodes().size() * 2;
        assertTrue(String.format("Assigned target size not within expected tolerance. Diff %d Tolerance %d", diff, tolerance),
                          diff < tolerance);
    }

    private void createVhnWithVh(final BrokerImpl brokerImpl, int nameSuffix, final long totalQueueSize)
    {
        final Map<String, Object> vhnAttributes = new HashMap<>();
        vhnAttributes.put(VirtualHostNode.TYPE, TestVirtualHostNode.VIRTUAL_HOST_NODE_TYPE);
        vhnAttributes.put(VirtualHostNode.NAME, "testVhn" + nameSuffix);

        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        TestVirtualHostNode vhn = new TestVirtualHostNode(brokerImpl, vhnAttributes, store);
        vhn.create();


        final Map<String, Object> vhAttributes = new HashMap<>();
        vhAttributes.put(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        vhAttributes.put(VirtualHost.NAME, "testVh" + nameSuffix);
        TestMemoryVirtualHost vh = new TestMemoryVirtualHost(vhAttributes, vhn)
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
