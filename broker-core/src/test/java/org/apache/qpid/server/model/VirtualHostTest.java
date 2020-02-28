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
package org.apache.qpid.server.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.store.StoreConfigurationChangeListener;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.ConnectionPrincipalStatistics;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.NoopConnectionEstablishmentPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostTest extends UnitTestBase
{
    private final AccessControl _mockAccessControl = BrokerTestHelper.createAccessControlMock();
    private Broker _broker;
    private TaskExecutor _taskExecutor;
    private VirtualHostNode _virtualHostNode;
    private DurableConfigurationStore _configStore;
    private QueueManagingVirtualHost<?> _virtualHost;
    private StoreConfigurationChangeListener _storeConfigurationChangeListener;
    private PreferenceStore _preferenceStore;

    @Before
    public void setUp() throws Exception
    {

        _broker = BrokerTestHelper.createBrokerMock();

        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);


        Principal systemPrincipal = ((SystemPrincipalSource)_broker).getSystemPrincipal();
        _virtualHostNode = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class, systemPrincipal, _mockAccessControl);
        when(_virtualHostNode.getParent()).thenReturn(_broker);
        when(_virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(_virtualHostNode.isDurable()).thenReturn(true);

        _configStore = mock(DurableConfigurationStore.class);
        _storeConfigurationChangeListener = new StoreConfigurationChangeListener(_configStore);

        when(_virtualHostNode.getConfigurationStore()).thenReturn(_configStore);


        // Virtualhost needs the EventLogger from the SystemContext.
        when(_virtualHostNode.getParent()).thenReturn(_broker);

        ConfiguredObjectFactory objectFactory = _broker.getObjectFactory();
        when(_virtualHostNode.getModel()).thenReturn(objectFactory.getModel());
        when(_virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);
        _preferenceStore = mock(PreferenceStore.class);
        when(_virtualHostNode.createPreferenceStore()).thenReturn(_preferenceStore);
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            try
            {
                _taskExecutor.stopImmediately();
            }
            finally
            {
                if (_virtualHost != null)
                {
                    _virtualHost.close();
                }
            }
        }
        finally
        {
        }
    }

    @Test
    public void testNewVirtualHost()
    {
        String virtualHostName = getTestName();
        QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        assertNotNull("Unexpected id", virtualHost.getId());
        assertEquals("Unexpected name", virtualHostName, virtualHost.getName());
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        verify(_configStore).update(eq(true), matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    @Test
    public void testDeleteVirtualHost()
    {
        VirtualHost<?> virtualHost = createVirtualHost(getTestName());
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        virtualHost.delete();

        assertEquals("Unexpected state", State.DELETED, virtualHost.getState());
        verify(_configStore).remove(matchesRecord(virtualHost.getId(), virtualHost.getType()));
        verify(_preferenceStore).onDelete();
    }

    @Test
    public void testStopAndStartVirtualHost()
    {
        String virtualHostName = getTestName();

        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals("Unexpected state", State.STOPPED, virtualHost.getState());
        verify(_preferenceStore).close();

        ((AbstractConfiguredObject<?>)virtualHost).start();
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        verify(_configStore, times(1)).update(eq(true), matchesRecord(virtualHost.getId(), virtualHost.getType()));
        verify(_configStore, times(2)).update(eq(false), matchesRecord(virtualHost.getId(), virtualHost.getType()));
        verify(_preferenceStore, times(2)).openAndLoad(any(PreferenceStoreUpdater.class));
    }

    @Test
    public void testRestartingVirtualHostRecoversChildren()
    {
        String virtualHostName = getTestName();

        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());
        final ConfiguredObjectRecord virtualHostCor = virtualHost.asObjectRecord();

        // Give virtualhost a queue and an exchange
        Queue queue = virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, "myQueue"));
        final ConfiguredObjectRecord queueCor = queue.asObjectRecord();

        Map<String, Object> exchangeArgs = new HashMap<>();
        exchangeArgs.put(Exchange.NAME, "myExchange");
        exchangeArgs.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        Exchange exchange = virtualHost.createChild(Exchange.class, exchangeArgs);
        final ConfiguredObjectRecord exchangeCor = exchange.asObjectRecord();

        assertEquals("Unexpected number of queues before stop",
                            (long) 1,
                            (long) virtualHost.getChildren(Queue.class).size());

        assertEquals("Unexpected number of exchanges before stop",
                            (long) 5,
                            (long) virtualHost.getChildren(Exchange.class).size());

        final List<ConfiguredObjectRecord> allObjects = new ArrayList<>();
        allObjects.add(virtualHostCor);
        allObjects.add(queueCor);
        for(Exchange e : virtualHost.getChildren(Exchange.class))
        {
            allObjects.add(e.asObjectRecord());
        }


        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals("Unexpected state", State.STOPPED, virtualHost.getState());
        assertEquals("Unexpected number of queues after stop",
                            (long) 0,
                            (long) virtualHost.getChildren(Queue.class).size());
        assertEquals("Unexpected number of exchanges after stop",
                            (long) 0,
                            (long) virtualHost.getChildren(Exchange.class).size());

        // Setup an answer that will return the configured object records
        doAnswer(new Answer()
        {
            final Iterator<ConfiguredObjectRecord> corIterator = allObjects.iterator();

            @Override
            public Object answer(final InvocationOnMock invocation)
            {
                ConfiguredObjectRecordHandler handler = (ConfiguredObjectRecordHandler) invocation.getArguments()[0];
                boolean handlerContinue = true;
                while(corIterator.hasNext())
                {
                    handler.handle(corIterator.next());
                }

                return null;
            }
        }).when(_configStore).reload(any(ConfiguredObjectRecordHandler.class));

        ((AbstractConfiguredObject<?>)virtualHost).start();
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        assertEquals("Unexpected number of queues after restart",
                            (long) 1,
                            (long) virtualHost.getChildren(Queue.class).size());
        assertEquals("Unexpected number of exchanges after restart",
                            (long) 5,
                            (long) virtualHost.getChildren(Exchange.class).size());
    }


    @Test
    public void testModifyDurableChildAfterRestartingVirtualHost()
    {
        String virtualHostName = getTestName();

        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        final ConfiguredObjectRecord virtualHostCor = virtualHost.asObjectRecord();

        // Give virtualhost a queue and an exchange
        Queue queue = virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, "myQueue"));
        final ConfiguredObjectRecord queueCor = queue.asObjectRecord();

        final List<ConfiguredObjectRecord> allObjects = new ArrayList<>();
        allObjects.add(virtualHostCor);
        allObjects.add(queueCor);

        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals("Unexpected state", State.STOPPED, virtualHost.getState());

        // Setup an answer that will return the configured object records
        doAnswer(new Answer()
        {
            final Iterator<ConfiguredObjectRecord> corIterator = allObjects.iterator();

            @Override
            public Object answer(final InvocationOnMock invocation)
            {
                ConfiguredObjectRecordHandler handler = (ConfiguredObjectRecordHandler) invocation.getArguments()[0];
                while (corIterator.hasNext())
                {
                    handler.handle(corIterator.next());
                }

                return null;
            }
        }).when(_configStore).reload(any(ConfiguredObjectRecordHandler.class));

        ((AbstractConfiguredObject<?>)virtualHost).start();
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());
        final Collection<Queue> queues = virtualHost.getChildren(Queue.class);
        assertEquals("Unexpected number of queues after restart", (long) 1, (long) queues.size());

        final Queue recoveredQueue = queues.iterator().next();
        recoveredQueue.setAttributes(Collections.singletonMap(ConfiguredObject.DESCRIPTION, "testDescription"));
        final ConfiguredObjectRecord recoveredQueueCor = queue.asObjectRecord();

        verify(_configStore).update(eq(false), matchesRecord(recoveredQueueCor.getId(), recoveredQueueCor.getType()));
    }


    @Test
    public void testStopVirtualHost_ClosesConnections()
    {
        String virtualHostName = getTestName();

        QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        assertEquals("Unexpected number of connections before connection registered",
                            (long) 0,
                            virtualHost.getConnectionCount());

        AMQPConnection modelConnection = getMockConnection();
        virtualHost.registerConnection(modelConnection, new NoopConnectionEstablishmentPolicy());

        assertEquals("Unexpected number of connections after connection registered",
                            (long) 1,
                            virtualHost.getConnectionCount());

        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals("Unexpected state", State.STOPPED, virtualHost.getState());

        assertEquals("Unexpected number of connections after virtualhost stopped",
                            (long) 0,
                            virtualHost.getConnectionCount());

        verify(modelConnection).closeAsync();
    }

    @Test
    public void testDeleteVirtualHost_ClosesConnections()
    {
        String virtualHostName = getTestName();

        QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        assertEquals("Unexpected number of connections before connection registered",
                            (long) 0,
                            virtualHost.getConnectionCount());

        AMQPConnection modelConnection = getMockConnection();
        virtualHost.registerConnection(modelConnection, new NoopConnectionEstablishmentPolicy());

        assertEquals("Unexpected number of connections after connection registered",
                            (long) 1,
                            virtualHost.getConnectionCount());

        virtualHost.delete();
        assertEquals("Unexpected state", State.DELETED, virtualHost.getState());

        assertEquals("Unexpected number of connections after virtualhost deleted",
                            (long) 0,
                            virtualHost.getConnectionCount());

        verify(modelConnection).closeAsync();
    }

    @Test
    public void testCreateDurableQueue()
    {
        String virtualHostName = getTestName();
        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        String queueName = "myQueue";
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(Queue.NAME, queueName);
        arguments.put(Queue.DURABLE, Boolean.TRUE);

        Queue queue = virtualHost.createChild(Queue.class, arguments);
        assertNotNull(queue.getId());
        assertEquals(queueName, queue.getName());

        verify(_configStore).update(eq(true), matchesRecord(queue.getId(), queue.getType()));
    }

    @Test
    public void testCreateNonDurableQueue()
    {
        String virtualHostName = getTestName();
        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        String queueName = "myQueue";
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(Queue.NAME, queueName);
        arguments.put(Queue.DURABLE, Boolean.FALSE);

        Queue queue = virtualHost.createChild(Queue.class, arguments);
        assertNotNull(queue.getId());
        assertEquals(queueName, queue.getName());

        verify(_configStore, never()).create(matchesRecord(queue.getId(), queue.getType()));
    }

    // ***************  VH Access Control Tests  ***************

    @Test
    public void testUpdateDeniedByACL()
    {

        String virtualHostName = getTestName();
        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(eq(null), eq(Operation.UPDATE), eq(virtualHost), any(Map.class))).thenReturn(Result.DENIED);


        assertNull(virtualHost.getDescription());

        try
        {
            virtualHost.setAttributes(Collections.<String, Object>singletonMap(VirtualHost.DESCRIPTION, "My description"));
            fail("Exception not thrown");
        }
        catch (AccessControlException ace)
        {
            // PASS
        }

        verify(_configStore, never()).update(eq(false), matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    @Test
    public void testStopDeniedByACL()
    {
        String virtualHostName = getTestName();
        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(eq(null),
                                          eq(Operation.UPDATE),
                                          same(virtualHost),
                                          any(Map.class))).thenReturn(Result.DENIED);

        try
        {
            ((AbstractConfiguredObject<?>)virtualHost).stop();
            fail("Exception not thrown");
        }
        catch (AccessControlException ace)
        {
            // PASS
        }

        verify(_configStore, never()).update(eq(false), matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    @Test
    public void testDeleteDeniedByACL()
    {
        String virtualHostName = getTestName();
        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(eq(null),
                                          eq(Operation.DELETE),
                                          same(virtualHost),
                                          any(Map.class))).thenReturn(Result.DENIED);

        try
        {
            virtualHost.delete();
            fail("Exception not thrown");
        }
        catch (AccessControlException ace)
        {
            // PASS
        }

        verify(_configStore, never()).remove(matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    @Test
    public void testExistingConnectionBlocking()
    {
        VirtualHost<?> host = createVirtualHost(getTestName());
        AMQPConnection connection = getMockConnection();
        host.registerConnection(connection, new NoopConnectionEstablishmentPolicy());
        ((EventListener) host).event(Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
        verify(connection).block();
    }

    @Test
    public void testCreateValidation()
    {
        try
        {
            createVirtualHost(getTestName(), Collections.singletonMap(QueueManagingVirtualHost.NUMBER_OF_SELECTORS,
                                                                      "-1"));
            fail("Exception not thrown for negative number of selectors");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
        try
        {
            createVirtualHost(getTestName(), Collections.singletonMap(QueueManagingVirtualHost.CONNECTION_THREAD_POOL_SIZE, "-1"));
            fail("Exception not thrown for negative connection thread pool size");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
        try
        {
            createVirtualHost(getTestName(), Collections.<String, Object>singletonMap(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, QueueManagingVirtualHost.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE));
            fail("Exception not thrown for number of selectors equal to connection thread pool size");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testChangeValidation()
    {
        QueueManagingVirtualHost<?> virtualHost = createVirtualHost(getTestName());
        try
        {
            virtualHost.setAttributes(Collections.<String, Object>singletonMap(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, "-1"));
            fail("Exception not thrown for negative number of selectors");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
        try
        {
            virtualHost.setAttributes(Collections.singletonMap(QueueManagingVirtualHost.CONNECTION_THREAD_POOL_SIZE,
                                                               "-1"));
            fail("Exception not thrown for negative connection thread pool size");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
        try
        {
            virtualHost.setAttributes(Collections.singletonMap(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, QueueManagingVirtualHost.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE));
            fail("Exception not thrown for number of selectors equal to connection thread pool size");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testRegisterConnection()
    {
        QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        AMQPConnection<?> connection = getMockConnection();

        assertEquals("unexpected number of connections before test", (long) 0, vhost.getConnectionCount());
        vhost.registerConnection(connection, new NoopConnectionEstablishmentPolicy());
        assertEquals("unexpected number of connections after registerConnection",
                            (long) 1,
                            vhost.getConnectionCount());
        assertEquals("unexpected connection object", Collections.singleton(connection), vhost.getConnections());
    }

    @Test
    public void testRegisterConnectionCausesUpdateOfAuthenticatedPrincipalConnectionCountAndFrequency()
    {
        final NoopConnectionEstablishmentPolicy policy = new NoopConnectionEstablishmentPolicy();
        final QueueManagingVirtualHost<?> vhost = createVirtualHost(getTestName());
        final Principal principal = mockAuthenticatedPrincipal(getTestName());
        final Principal principal2 = mockAuthenticatedPrincipal(getTestName() + "_2");
        final AMQPConnection<?> connection1 = mockAmqpConnection(principal);
        final AMQPConnection<?> connection2 = mockAmqpConnection(principal);
        final AMQPConnection<?> connection3 = mockAmqpConnection(principal2);

        vhost.registerConnection(connection1, policy);
        verify(connection1).registered(argThat(new ConnectionPrincipalStatisticsArgumentMatcher(1, 1)));

        vhost.registerConnection(connection2, policy);
        verify(connection2).registered(argThat(new ConnectionPrincipalStatisticsArgumentMatcher(2, 2)));

        vhost.registerConnection(connection3, policy);
        verify(connection3).registered(argThat(new ConnectionPrincipalStatisticsArgumentMatcher(1, 1)));
    }

    @Test
    public void testDeregisterConnectionAffectsAuthenticatedPrincipalConnectionCountAndFrequency()
    {
        final Principal principal = mockAuthenticatedPrincipal(getTestName());
        final NoopConnectionEstablishmentPolicy policy = new NoopConnectionEstablishmentPolicy();
        final QueueManagingVirtualHost<?> vhost = createVirtualHost(getTestName());

        final AMQPConnection<?> connection1 = mockAmqpConnection(principal);
        final AMQPConnection<?> connection2 = mockAmqpConnection(principal);

        vhost.registerConnection(connection1, policy);
        verify(connection1).registered(argThat(new ConnectionPrincipalStatisticsArgumentMatcher(1, 1)));

        vhost.deregisterConnection(connection1);
        vhost.registerConnection(connection2, policy);

        verify(connection2).registered(argThat(new ConnectionPrincipalStatisticsArgumentMatcher(1, 2)));
    }

    @Test
    public void testStopVirtualhostClosesConnections()
    {
        QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        AMQPConnection<?> connection = getMockConnection();

        vhost.registerConnection(connection, new NoopConnectionEstablishmentPolicy());
        assertEquals("unexpected number of connections after registerConnection",
                            (long) 1,
                            vhost.getConnectionCount());
        assertEquals("unexpected connection object", Collections.singleton(connection), vhost.getConnections());
        ((AbstractConfiguredObject<?>)vhost).stop();
        verify(connection).stopConnection();
        verify(connection).closeAsync();
    }

    @Test
    public void testRegisterConnectionOnStoppedVirtualhost()
    {
        QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        AMQPConnection<?> connection = getMockConnection();

        ((AbstractConfiguredObject<?>)vhost).stop();
        try
        {
            vhost.registerConnection(connection, new NoopConnectionEstablishmentPolicy());
            fail("exception not thrown");
        }
        catch (VirtualHostUnavailableException e)
        {
            // pass
        }
        assertEquals("unexpected number of connections", (long) 0, vhost.getConnectionCount());
        ((AbstractConfiguredObject<?>)vhost).start();
        vhost.registerConnection(connection, new NoopConnectionEstablishmentPolicy());
        assertEquals("unexpected number of connections", (long) 1, vhost.getConnectionCount());
    }

    @Test
    public void testAddValidAutoCreationPolicies()
    {
        NodeAutoCreationPolicy[] policies = new NodeAutoCreationPolicy[] {
                new NodeAutoCreationPolicy()
                {
                    @Override
                    public String getPattern()
                    {
                        return "fooQ*";
                    }

                    @Override
                    public boolean isCreatedOnPublish()
                    {
                        return true;
                    }

                    @Override
                    public boolean isCreatedOnConsume()
                    {
                        return true;
                    }

                    @Override
                    public String getNodeType()
                    {
                        return "Queue";
                    }

                    @Override
                    public Map<String, Object> getAttributes()
                    {
                        return Collections.emptyMap();
                    }
                },
                new NodeAutoCreationPolicy()
                {
                    @Override
                    public String getPattern()
                    {
                        return "barE*";
                    }

                    @Override
                    public boolean isCreatedOnPublish()
                    {
                        return true;
                    }

                    @Override
                    public boolean isCreatedOnConsume()
                    {
                        return false;
                    }

                    @Override
                    public String getNodeType()
                    {
                        return "Exchange";
                    }

                    @Override
                    public Map<String, Object> getAttributes()
                    {
                        return Collections.singletonMap(Exchange.TYPE, "amq.fanout");
                    }
                }
        };

        QueueManagingVirtualHost<?> vhost = createVirtualHost("host");


        Map<String, Object> newAttributes = Collections.singletonMap(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES,
                                                                     Arrays.asList(policies));

        vhost.setAttributes(newAttributes);

        List<NodeAutoCreationPolicy> retrievedPoliciesList = vhost.getNodeAutoCreationPolicies();
        assertEquals("Retrieved node policies list has incorrect size",
                            (long) 2,
                            (long) retrievedPoliciesList.size());
        NodeAutoCreationPolicy firstPolicy =  retrievedPoliciesList.get(0);
        NodeAutoCreationPolicy secondPolicy = retrievedPoliciesList.get(1);
        assertEquals("fooQ*", firstPolicy.getPattern());
        assertEquals("barE*", secondPolicy.getPattern());
        assertEquals(true, firstPolicy.isCreatedOnConsume());
        assertEquals(false, secondPolicy.isCreatedOnConsume());
    }

    @Test
    public void testAddInvalidAutoCreationPolicies()
    {
        NodeAutoCreationPolicy[] policies = new NodeAutoCreationPolicy[] {
                new NodeAutoCreationPolicy()
                {
                    @Override
                    public String getPattern()
                    {
                        return null;
                    }

                    @Override
                    public boolean isCreatedOnPublish()
                    {
                        return true;
                    }

                    @Override
                    public boolean isCreatedOnConsume()
                    {
                        return true;
                    }

                    @Override
                    public String getNodeType()
                    {
                        return "Queue";
                    }

                    @Override
                    public Map<String, Object> getAttributes()
                    {
                        return Collections.emptyMap();
                    }
                }
        };

        QueueManagingVirtualHost<?> vhost = createVirtualHost("host");
        Map<String, Object> newAttributes = Collections.singletonMap(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES,
                                                                     Arrays.asList(policies));

        try
        {
            vhost.setAttributes(newAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // PASS
        }

        List<NodeAutoCreationPolicy> retrievedPoliciesList = vhost.getNodeAutoCreationPolicies();
        assertTrue("Retrieved node policies is not empty", ((List) retrievedPoliciesList).isEmpty());
    }

    private AMQPConnection<?> getMockConnection()
    {
        return mockAmqpConnection(mockAuthenticatedPrincipal(getTestName()));
    }

    private AMQPConnection<?> mockAmqpConnection(final Principal principal)
    {
        AMQPConnection<?> connection = mock(AMQPConnection.class);
        when(connection.getAuthorizedPrincipal()).thenReturn(principal);
        final Subject subject =
                new Subject(true, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());
        when(connection.getSubject()).thenReturn(subject);
        final ListenableFuture<Void> listenableFuture = Futures.immediateFuture(null);
        when(connection.closeAsync()).thenReturn(listenableFuture);
        when(connection.getCreatedTime()).thenReturn(new Date());
        return connection;
    }

    private Principal mockAuthenticatedPrincipal(final String principalName)
    {
        final Principal principal = mock(Principal.class);
        when(principal.getName()).thenReturn(principalName);
        return new AuthenticatedPrincipal(principal);
    }

    private QueueManagingVirtualHost<?> createVirtualHost(final String virtualHostName)
    {
        return createVirtualHost(virtualHostName, Collections.<String, Object>emptyMap());
    }

    private QueueManagingVirtualHost<?> createVirtualHost(final String virtualHostName, Map<String,Object> attributes)
    {
        Map<String, Object> vhAttributes = new HashMap<>();
        vhAttributes.put(VirtualHost.NAME, virtualHostName);
        vhAttributes.put(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        vhAttributes.putAll(attributes);

        TestMemoryVirtualHost host = new TestMemoryVirtualHost(vhAttributes, _virtualHostNode);
        host.addChangeListener(_storeConfigurationChangeListener);
        host.create();
        // Fire the child added event on the node
        _storeConfigurationChangeListener.childAdded(_virtualHostNode,host);
        _virtualHost = host;
        when(_virtualHostNode.getVirtualHost()).thenReturn(_virtualHost);
        return host;
    }

    private static ConfiguredObjectRecord matchesRecord(UUID id, String type)
    {
        return argThat(new MinimalConfiguredObjectRecordMatcher(id, type));
    }

    private static class MinimalConfiguredObjectRecordMatcher implements ArgumentMatcher<ConfiguredObjectRecord>
    {
        private final UUID _id;
        private final String _type;

        private MinimalConfiguredObjectRecordMatcher(UUID id, String type)
        {
            _id = id;
            _type = type;
        }

        @Override
        public boolean matches(ConfiguredObjectRecord rhs)
        {
            return (_id.equals(rhs.getId()) || _type.equals(rhs.getType()));
        }
    }

    private static class ConnectionPrincipalStatisticsArgumentMatcher implements ArgumentMatcher<ConnectionPrincipalStatistics>
    {

        private final int _expectedConnectionCount;
        private final int _expectedConnectionFrequency;

        ConnectionPrincipalStatisticsArgumentMatcher(final int expectedConnectionCount,
                                                     final int expectedConnectionFrequency)
        {
            _expectedConnectionCount = expectedConnectionCount;
            _expectedConnectionFrequency = expectedConnectionFrequency;
        }

        @Override
        public boolean matches(final ConnectionPrincipalStatistics connectionPrincipalStatistics)
        {
            return connectionPrincipalStatistics.getConnectionFrequency() == _expectedConnectionFrequency
                    && connectionPrincipalStatistics.getConnectionCount() == _expectedConnectionCount;
        }
    }
}
