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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class VirtualHostTest extends UnitTestBase
{
    private final AccessControl<?> _mockAccessControl = BrokerTestHelper.createAccessControlMock();
    private TaskExecutor _taskExecutor;
    private VirtualHostNode _virtualHostNode;
    private DurableConfigurationStore _configStore;
    private StoreConfigurationChangeListener _storeConfigurationChangeListener;
    private PreferenceStore _preferenceStore;

    @BeforeEach
    public void setUp() throws Exception
    {
        final Broker broker = BrokerTestHelper.createBrokerMock();

        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        when(broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(broker.getChildExecutor()).thenReturn(_taskExecutor);

        final Principal systemPrincipal = ((SystemPrincipalSource) broker).getSystemPrincipal();
        _virtualHostNode = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class, systemPrincipal, _mockAccessControl);
        when(_virtualHostNode.getParent()).thenReturn(broker);
        when(_virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(_virtualHostNode.isDurable()).thenReturn(true);

        _configStore = mock(DurableConfigurationStore.class);
        _storeConfigurationChangeListener = new StoreConfigurationChangeListener(_configStore);

        when(_virtualHostNode.getConfigurationStore()).thenReturn(_configStore);

        // Virtualhost needs the EventLogger from the SystemContext.
        when(_virtualHostNode.getParent()).thenReturn(broker);

        final ConfiguredObjectFactory objectFactory = broker.getObjectFactory();
        when(_virtualHostNode.getModel()).thenReturn(objectFactory.getModel());
        when(_virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);
        _preferenceStore = mock(PreferenceStore.class);
        when(_virtualHostNode.createPreferenceStore()).thenReturn(_preferenceStore);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _taskExecutor.stopImmediately();
    }

    @Test
    public void testNewVirtualHost()
    {
        final String virtualHostName = getTestName();
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        assertNotNull(virtualHost.getId(), "Unexpected id");
        assertEquals(virtualHostName, virtualHost.getName(), "Unexpected name");
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");

        verify(_configStore).update(eq(true), matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    @Test
    public void testDeleteVirtualHost()
    {
        final VirtualHost<?> virtualHost = createVirtualHost(getTestName());
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");

        virtualHost.delete();

        assertEquals(State.DELETED, virtualHost.getState(), "Unexpected state");
        verify(_configStore).remove(matchesRecord(virtualHost.getId(), virtualHost.getType()));
        verify(_preferenceStore).onDelete();
    }

    @Test
    public void testStopAndStartVirtualHost()
    {
        final String virtualHostName = getTestName();

        final VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");

        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals(State.STOPPED, virtualHost.getState(), "Unexpected state");
        verify(_preferenceStore).close();

        ((AbstractConfiguredObject<?>)virtualHost).start();
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");

        verify(_configStore, times(1)).update(eq(true), matchesRecord(virtualHost.getId(), virtualHost.getType()));
        verify(_configStore, times(2)).update(eq(false), matchesRecord(virtualHost.getId(), virtualHost.getType()));
        verify(_preferenceStore, times(2)).openAndLoad(any(PreferenceStoreUpdater.class));
    }

    @Test
    public void testRestartingVirtualHostRecoversChildren()
    {
        final String virtualHostName = getTestName();

        final VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");
        final ConfiguredObjectRecord virtualHostCor = virtualHost.asObjectRecord();

        // Give virtualhost a queue and an exchange
        final Queue queue = virtualHost.createChild(Queue.class, Map.of(Queue.NAME, "myQueue"));
        final ConfiguredObjectRecord queueCor = queue.asObjectRecord();

        final Map<String, Object> exchangeArgs = Map.of(Exchange.NAME, "myExchange",
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        final Exchange exchange = virtualHost.createChild(Exchange.class, exchangeArgs);
        final ConfiguredObjectRecord exchangeCor = exchange.asObjectRecord();

        assertEquals(1, (long) virtualHost.getChildren(Queue.class).size(),
                "Unexpected number of queues before stop");

        assertEquals(5, (long) virtualHost.getChildren(Exchange.class).size(),
                "Unexpected number of exchanges before stop");

        final List<ConfiguredObjectRecord> allObjects = new ArrayList<>();
        allObjects.add(virtualHostCor);
        allObjects.add(queueCor);
        for (final Exchange e : virtualHost.getChildren(Exchange.class))
        {
            allObjects.add(e.asObjectRecord());
        }

        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals(State.STOPPED, virtualHost.getState(), "Unexpected state");
        assertEquals(0, (long) virtualHost.getChildren(Queue.class).size(),
                "Unexpected number of queues after stop");
        assertEquals(0, (long) virtualHost.getChildren(Exchange.class).size(),
                "Unexpected number of exchanges after stop");

        // Setup an answer that will return the configured object records
        doAnswer(new Answer()
        {
            final Iterator<ConfiguredObjectRecord> corIterator = allObjects.iterator();

            @Override
            public Object answer(final InvocationOnMock invocation)
            {
                final ConfiguredObjectRecordHandler handler = (ConfiguredObjectRecordHandler) invocation.getArguments()[0];
                while (corIterator.hasNext())
                {
                    handler.handle(corIterator.next());
                }
                return null;
            }
        }).when(_configStore).reload(any(ConfiguredObjectRecordHandler.class));

        ((AbstractConfiguredObject<?>)virtualHost).start();
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");

        assertEquals(1, (long) virtualHost.getChildren(Queue.class).size(),
                "Unexpected number of queues after restart");
        assertEquals(5, (long) virtualHost.getChildren(Exchange.class).size(),
                "Unexpected number of exchanges after restart");
    }


    @Test
    public void testModifyDurableChildAfterRestartingVirtualHost()
    {
        final String virtualHostName = getTestName();
        final VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        final ConfiguredObjectRecord virtualHostCor = virtualHost.asObjectRecord();

        // Give virtualhost a queue and an exchange
        final Queue queue = virtualHost.createChild(Queue.class, Map.of(Queue.NAME, "myQueue"));
        final ConfiguredObjectRecord queueCor = queue.asObjectRecord();

        final List<ConfiguredObjectRecord> allObjects = new ArrayList<>();
        allObjects.add(virtualHostCor);
        allObjects.add(queueCor);

        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals(State.STOPPED, virtualHost.getState(), "Unexpected state");

        // Setup an answer that will return the configured object records
        doAnswer(new Answer()
        {
            final Iterator<ConfiguredObjectRecord> corIterator = allObjects.iterator();

            @Override
            public Object answer(final InvocationOnMock invocation)
            {
                final ConfiguredObjectRecordHandler handler = (ConfiguredObjectRecordHandler) invocation.getArguments()[0];
                while (corIterator.hasNext())
                {
                    handler.handle(corIterator.next());
                }
                return null;
            }
        }).when(_configStore).reload(any(ConfiguredObjectRecordHandler.class));

        ((AbstractConfiguredObject<?>)virtualHost).start();
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");
        final Collection<Queue> queues = virtualHost.getChildren(Queue.class);
        assertEquals(1, (long) queues.size(), "Unexpected number of queues after restart");

        final Queue recoveredQueue = queues.iterator().next();
        recoveredQueue.setAttributes(Map.of(ConfiguredObject.DESCRIPTION, "testDescription"));
        final ConfiguredObjectRecord recoveredQueueCor = queue.asObjectRecord();

        verify(_configStore).update(eq(false), matchesRecord(recoveredQueueCor.getId(), recoveredQueueCor.getType()));
    }

    @Test
    public void testStopVirtualHost_ClosesConnections()
    {
        final String virtualHostName = getTestName();
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");

        assertEquals(0, virtualHost.getConnectionCount(),
                "Unexpected number of connections before connection registered");

        final AMQPConnection modelConnection = getMockConnection();
        virtualHost.registerConnection(modelConnection);

        assertEquals(1, virtualHost.getConnectionCount(),
                "Unexpected number of connections after connection registered");

        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals(State.STOPPED, virtualHost.getState(), "Unexpected state");

        assertEquals(0, virtualHost.getConnectionCount(),
                "Unexpected number of connections after virtualhost stopped");

        verify(modelConnection).closeAsync();
    }

    @Test
    public void testDeleteVirtualHost_ClosesConnections()
    {
        final String virtualHostName = getTestName();
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected state");

        assertEquals(0, virtualHost.getConnectionCount(),
                "Unexpected number of connections before connection registered");

        final AMQPConnection modelConnection = getMockConnection();
        virtualHost.registerConnection(modelConnection);

        assertEquals(1, virtualHost.getConnectionCount(),
                "Unexpected number of connections after connection registered");

        virtualHost.delete();
        assertEquals(State.DELETED, virtualHost.getState(), "Unexpected state");

        assertEquals(0, virtualHost.getConnectionCount(),
                "Unexpected number of connections after virtualhost deleted");

        verify(modelConnection).closeAsync();
    }

    @Test
    public void testCreateDurableQueue()
    {
        final String virtualHostName = getTestName();
        final VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        final String queueName = "myQueue";
        final Map<String, Object> arguments = Map.of(Queue.NAME, queueName,
                Queue.DURABLE, Boolean.TRUE);
        final Queue queue = virtualHost.createChild(Queue.class, arguments);
        assertNotNull(queue.getId());
        assertEquals(queueName, queue.getName());

        verify(_configStore).update(eq(true), matchesRecord(queue.getId(), queue.getType()));
    }

    @Test
    public void testCreateNonDurableQueue()
    {
        final String virtualHostName = getTestName();
        final VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        final String queueName = "myQueue";
        final Map<String, Object> arguments = Map.of(Queue.NAME, queueName,
                Queue.DURABLE, Boolean.FALSE);
        final Queue queue = virtualHost.createChild(Queue.class, arguments);
        assertNotNull(queue.getId());
        assertEquals(queueName, queue.getName());

        verify(_configStore, never()).create(matchesRecord(queue.getId(), queue.getType()));
    }

    // ***************  VH Access Control Tests  ***************

    @Test
    public void testUpdateDeniedByACL()
    {
        final String virtualHostName = getTestName();
        final VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(eq(null), eq(Operation.UPDATE), eq(virtualHost), any(Map.class))).thenReturn(Result.DENIED);

        assertNull(virtualHost.getDescription());

        assertThrows(AccessControlException.class,
                () -> virtualHost.setAttributes(Map.of(VirtualHost.DESCRIPTION, "My description")),
                "Exception not thrown");
        verify(_configStore, never()).update(eq(false), matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    @Test
    public void testStopDeniedByACL()
    {
        final String virtualHostName = getTestName();
        final VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(eq(null), eq(Operation.UPDATE), same(virtualHost), any(Map.class)))
                .thenReturn(Result.DENIED);
        assertThrows(AccessControlException.class,
                () -> ((AbstractConfiguredObject<?>)virtualHost).stop(),
                "Exception not thrown");
        verify(_configStore, never()).update(eq(false), matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    @Test
    public void testDeleteDeniedByACL()
    {
        final String virtualHostName = getTestName();
        final VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(eq(null), eq(Operation.DELETE), same(virtualHost), any(Map.class)))
                .thenReturn(Result.DENIED);
        assertThrows(AccessControlException.class, virtualHost::delete, "Exception not thrown");
        verify(_configStore, never()).remove(matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    @Test
    public void testExistingConnectionBlocking()
    {
        final VirtualHost<?> host = createVirtualHost(getTestName());
        final AMQPConnection connection = getMockConnection();
        host.registerConnection(connection);
        ((EventListener) host).event(Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
        verify(connection).block();
    }

    @Test
    public void testCreateValidation()
    {
        assertThrows(IllegalConfigurationException.class,
                () -> createVirtualHost(getTestName(), Map.of(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, "-1")),
                "Exception not thrown for negative number of selectors");
        assertThrows(IllegalConfigurationException.class,
                () -> createVirtualHost(getTestName(), Map.of(QueueManagingVirtualHost.CONNECTION_THREAD_POOL_SIZE, "-1")),
                "Exception not thrown for negative connection thread pool size");
        assertThrows(IllegalConfigurationException.class,
                () -> createVirtualHost(getTestName(), Map.of(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, QueueManagingVirtualHost.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE)),
                "Exception not thrown for number of selectors equal to connection thread pool size");
    }

    @Test
    public void testChangeValidation()
    {
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHost(getTestName());
        assertThrows(IllegalConfigurationException.class,
                () -> virtualHost.setAttributes(Map.of(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, "-1")),
                "Exception not thrown for negative number of selectors");
        assertThrows(IllegalConfigurationException.class,
                () -> virtualHost.setAttributes(Map.of(QueueManagingVirtualHost.CONNECTION_THREAD_POOL_SIZE, "-1")),
                "Exception not thrown for negative connection thread pool size");
        assertThrows(IllegalConfigurationException.class,
                () -> virtualHost.setAttributes(Map.of(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, QueueManagingVirtualHost.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE)),
                "Exception not thrown for number of selectors equal to connection thread pool size");
    }

    @Test
    public void testRegisterConnection()
    {
        final QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        final AMQPConnection<?> connection = getMockConnection();

        assertEquals(0, vhost.getConnectionCount(), "unexpected number of connections before test");
        vhost.registerConnection(connection);
        assertEquals(1, vhost.getConnectionCount(), "unexpected number of connections after registerConnection");
        assertEquals(Set.of(connection), vhost.getConnections(), "unexpected connection object");
    }

    @Test
    public void testStopVirtualhostClosesConnections()
    {
        final QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        final AMQPConnection<?> connection = getMockConnection();

        vhost.registerConnection(connection);
        assertEquals(1, vhost.getConnectionCount(),
                     "unexpected number of connections after registerConnection");
        assertEquals(Set.of(connection), vhost.getConnections(), "unexpected connection object");
        ((AbstractConfiguredObject<?>)vhost).stop();
        verify(connection).stopConnection();
        verify(connection).closeAsync();
    }

    @Test
    public void testRegisterConnectionOnStoppedVirtualhost()
    {
        final QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        final AMQPConnection<?> connection = getMockConnection();

        ((AbstractConfiguredObject<?>)vhost).stop();
        assertThrows(VirtualHostUnavailableException.class, () -> vhost.registerConnection(connection),
                "Exception not thrown");
        assertEquals(0, vhost.getConnectionCount(), "unexpected number of connections");
        ((AbstractConfiguredObject<?>)vhost).start();
        vhost.registerConnection(connection);
        assertEquals(1, vhost.getConnectionCount(), "unexpected number of connections");
    }

    @Test
    public void testAddValidAutoCreationPolicies()
    {
        final NodeAutoCreationPolicy[] policies = new NodeAutoCreationPolicy[] {
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
                        return Map.of();
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
                        return Map.of(Exchange.TYPE, "amq.fanout");
                    }
                }
        };
        final String virtualHostName = getTestName();
        final QueueManagingVirtualHost<?> vhost = createVirtualHost(virtualHostName);
        final Map<String, Object> newAttributes = Map
                .of(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES, List.of(policies));

        vhost.setAttributes(newAttributes);

        final List<NodeAutoCreationPolicy> retrievedPoliciesList = vhost.getNodeAutoCreationPolicies();
        assertEquals(2, (long) retrievedPoliciesList.size(),
                "Retrieved node policies list has incorrect size");
        final NodeAutoCreationPolicy firstPolicy =  retrievedPoliciesList.get(0);
        final NodeAutoCreationPolicy secondPolicy = retrievedPoliciesList.get(1);
        assertEquals("fooQ*", firstPolicy.getPattern());
        assertEquals("barE*", secondPolicy.getPattern());
        assertTrue(firstPolicy.isCreatedOnConsume());
        assertFalse(secondPolicy.isCreatedOnConsume());
    }

    @Test
    public void testAddInvalidAutoCreationPolicies()
    {
        final NodeAutoCreationPolicy[] policies = new NodeAutoCreationPolicy[] {
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
                        return Map.of();
                    }
                }
        };

        final QueueManagingVirtualHost<?> vhost = createVirtualHost("host");
        final Map<String, Object> newAttributes = Map
                .of(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES, List.of(policies));

        assertThrows(IllegalArgumentException.class, () -> vhost.setAttributes(newAttributes),
                "Exception not thrown");

        final List<NodeAutoCreationPolicy> retrievedPoliciesList = vhost.getNodeAutoCreationPolicies();
        assertTrue(retrievedPoliciesList.isEmpty(), "Retrieved node policies is not empty");
    }

    private AMQPConnection<?> getMockConnection()
    {
        return mockAmqpConnection(mockAuthenticatedPrincipal(getTestName()));
    }

    private AMQPConnection<?> mockAmqpConnection(final Principal principal)
    {
        final AMQPConnection<?> connection = mock(AMQPConnection.class);
        when(connection.getAuthorizedPrincipal()).thenReturn(principal);
        final Subject subject =
                new Subject(true, Set.of(principal), Set.of(), Set.of());
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
        return createVirtualHost(virtualHostName, Map.of());
    }

    private QueueManagingVirtualHost<?> createVirtualHost(final String virtualHostName, final Map<String,Object> attributes)
    {
        final Map<String, Object> vhAttributes = new HashMap<>();
        vhAttributes.put(VirtualHost.NAME, virtualHostName);
        vhAttributes.put(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        vhAttributes.putAll(attributes);

        final TestMemoryVirtualHost host = new TestMemoryVirtualHost(vhAttributes, _virtualHostNode);
        host.addChangeListener(_storeConfigurationChangeListener);
        host.create();
        // Fire the child added event on the node
        _storeConfigurationChangeListener.childAdded(_virtualHostNode,host);
        when(_virtualHostNode.getVirtualHost()).thenReturn(host);
        return host;
    }

    private static ConfiguredObjectRecord matchesRecord(final UUID id, final String type)
    {
        return argThat(new MinimalConfiguredObjectRecordMatcher(id, type));
    }

    private static class MinimalConfiguredObjectRecordMatcher implements ArgumentMatcher<ConfiguredObjectRecord>
    {
        private final UUID _id;
        private final String _type;

        private MinimalConfiguredObjectRecordMatcher(final UUID id, final String type)
        {
            _id = id;
            _type = type;
        }

        @Override
        public boolean matches(final ConfiguredObjectRecord rhs)
        {
            return (_id.equals(rhs.getId()) || _type.equals(rhs.getType()));
        }
    }
}
