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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.store.StoreConfigurationChangeListener;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;
import org.apache.qpid.test.utils.QpidTestCase;

public class VirtualHostTest extends QpidTestCase
{
    private final AccessControl _mockAccessControl = BrokerTestHelper.createAccessControlMock();
    private Broker _broker;
    private TaskExecutor _taskExecutor;
    private VirtualHostNode _virtualHostNode;
    private DurableConfigurationStore _configStore;
    private QueueManagingVirtualHost<?> _virtualHost;
    private StoreConfigurationChangeListener _storeConfigurationChangeListener;
    private PreferenceStore _preferenceStore;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();

        _broker = BrokerTestHelper.createBrokerMock();

        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);


        Principal systemPrincipal = ((SystemPrincipalSource)_broker).getSystemPrincipal();
        _virtualHostNode = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class, systemPrincipal, _mockAccessControl);
        when(_virtualHostNode.getParent(Broker.class)).thenReturn(_broker);
        when(_virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(_virtualHostNode.isDurable()).thenReturn(true);

        _configStore = mock(DurableConfigurationStore.class);
        _storeConfigurationChangeListener = new StoreConfigurationChangeListener(_configStore);

        when(_virtualHostNode.getConfigurationStore()).thenReturn(_configStore);


        // Virtualhost needs the EventLogger from the SystemContext.
        when(_virtualHostNode.getParent(Broker.class)).thenReturn(_broker);

        ConfiguredObjectFactory objectFactory = _broker.getObjectFactory();
        when(_virtualHostNode.getModel()).thenReturn(objectFactory.getModel());
        when(_virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);
        _preferenceStore = mock(PreferenceStore.class);
        when(_virtualHostNode.createPreferenceStore()).thenReturn(_preferenceStore);
    }

    @Override
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
            super.tearDown();
        }
    }

    public void testNewVirtualHost()
    {
        String virtualHostName = getName();
        QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        assertNotNull("Unexpected id", virtualHost.getId());
        assertEquals("Unexpected name", virtualHostName, virtualHost.getName());
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        verify(_configStore).update(eq(true), matchesRecord(virtualHost.getId(), virtualHost.getType()));
    }

    public void testDeleteVirtualHost()
    {
        VirtualHost<?> virtualHost = createVirtualHost(getName());
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        virtualHost.delete();

        assertEquals("Unexpected state", State.DELETED, virtualHost.getState());
        verify(_configStore).remove(matchesRecord(virtualHost.getId(), virtualHost.getType()));
        verify(_preferenceStore).onDelete();
    }

    public void testStopAndStartVirtualHost()
    {
        String virtualHostName = getName();

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

    public void testRestartingVirtualHostRecoversChildren()
    {
        String virtualHostName = getName();

        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());
        final ConfiguredObjectRecord virtualHostCor = virtualHost.asObjectRecord();

        // Give virtualhost a queue and an exchange
        Queue queue = virtualHost.createChild(Queue.class, Collections.<String, Object>singletonMap(Queue.NAME, "myQueue"));
        final ConfiguredObjectRecord queueCor = queue.asObjectRecord();

        Map<String, Object> exchangeArgs = new HashMap<>();
        exchangeArgs.put(Exchange.NAME, "myExchange");
        exchangeArgs.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        Exchange exchange = virtualHost.createChild(Exchange.class, exchangeArgs);
        final ConfiguredObjectRecord exchangeCor = exchange.asObjectRecord();

        assertEquals("Unexpected number of queues before stop", 1, virtualHost.getChildren(Queue.class).size());
        assertEquals("Unexpected number of exchanges before stop", 5, virtualHost.getChildren(Exchange.class).size());

        final List<ConfiguredObjectRecord> allObjects = new ArrayList<>();
        allObjects.add(virtualHostCor);
        allObjects.add(queueCor);
        for(Exchange e : virtualHost.getChildren(Exchange.class))
        {
            allObjects.add(e.asObjectRecord());
        }


        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals("Unexpected state", State.STOPPED, virtualHost.getState());
        assertEquals("Unexpected number of queues after stop", 0, virtualHost.getChildren(Queue.class).size());
        assertEquals("Unexpected number of exchanges after stop", 0, virtualHost.getChildren(Exchange.class).size());


        // Setup an answer that will return the configured object records
        doAnswer(new Answer()
        {
            final Iterator<ConfiguredObjectRecord> corIterator = allObjects.iterator();

            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
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

        assertEquals("Unexpected number of queues after restart", 1, virtualHost.getChildren(Queue.class).size());
        assertEquals("Unexpected number of exchanges after restart", 5, virtualHost.getChildren(Exchange.class).size());
    }


    public void testStopVirtualHost_ClosesConnections()
    {
        String virtualHostName = getName();

        QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        AbstractAMQPConnection connection = createMockProtocolConnection(virtualHost);
        assertEquals("Unexpected number of connections before connection registered", 0, virtualHost.getConnectionCount());

        AMQPConnection modelConnection = mock(AMQPConnection.class);
        when(modelConnection.closeAsync()).thenReturn(Futures.immediateFuture(null));
        virtualHost.registerConnection(modelConnection);

        assertEquals("Unexpected number of connections after connection registered", 1, virtualHost.getConnectionCount());

        ((AbstractConfiguredObject<?>)virtualHost).stop();
        assertEquals("Unexpected state", State.STOPPED, virtualHost.getState());

        assertEquals("Unexpected number of connections after virtualhost stopped",
                     0,
                     virtualHost.getConnectionCount());

        verify(modelConnection).closeAsync();
    }

    public void testDeleteVirtualHost_ClosesConnections()
    {
        String virtualHostName = getName();

        QueueManagingVirtualHost<?> virtualHost = createVirtualHost(virtualHostName);
        assertEquals("Unexpected state", State.ACTIVE, virtualHost.getState());

        AbstractAMQPConnection connection = createMockProtocolConnection(virtualHost);
        assertEquals("Unexpected number of connections before connection registered",
                     0,
                     virtualHost.getConnectionCount());

        AMQPConnection modelConnection = mock(AMQPConnection.class);
        when(modelConnection.closeAsync()).thenReturn(Futures.immediateFuture(null));
        virtualHost.registerConnection(modelConnection);

        assertEquals("Unexpected number of connections after connection registered",
                     1,
                     virtualHost.getConnectionCount());

        virtualHost.delete();
        assertEquals("Unexpected state", State.DELETED, virtualHost.getState());

        assertEquals("Unexpected number of connections after virtualhost deleted",
                0,
                virtualHost.getConnectionCount());

        verify(modelConnection).closeAsync();
    }

    public void testCreateDurableQueue()
    {
        String virtualHostName = getName();
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

    public void testCreateNonDurableQueue()
    {
        String virtualHostName = getName();
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

    public void testUpdateDeniedByACL()
    {

        String virtualHostName = getName();
        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(null, Operation.UPDATE, virtualHost, Collections.<String,Object>emptyMap())).thenReturn(Result.DENIED);

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

    public void testStopDeniedByACL()
    {

        String virtualHostName = getName();
        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(null, Operation.UPDATE,
                virtualHost, Collections.<String,Object>emptyMap())).thenReturn(Result.DENIED);

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

    public void testDeleteDeniedByACL()
    {
        String virtualHostName = getName();
        VirtualHost<?> virtualHost = createVirtualHost(virtualHostName);

        when(_mockAccessControl.authorise(null,
                Operation.DELETE, virtualHost, Collections.<String,Object>emptyMap())).thenReturn(Result.DENIED);

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

    public void testExistingConnectionBlocking()
    {
        VirtualHost<?> host = createVirtualHost(getTestName());
        AbstractAMQPConnection connection = mock(AbstractAMQPConnection.class);
        host.registerConnection(connection);
        ((EventListener)host).event(Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
        verify(connection).block();
    }

    public void testCreateValidation() throws Exception
    {
        try
        {
            createVirtualHost(getTestName(), Collections.<String, Object>singletonMap(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, "-1"));
            fail("Exception not thrown for negative number of selectors");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
        try
        {
            createVirtualHost(getTestName(), Collections.<String, Object>singletonMap(QueueManagingVirtualHost.CONNECTION_THREAD_POOL_SIZE, "-1"));
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

    public void testChangeValidation() throws Exception
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
            virtualHost.setAttributes(Collections.<String, Object>singletonMap(QueueManagingVirtualHost.CONNECTION_THREAD_POOL_SIZE,
                                                                               "-1"));
            fail("Exception not thrown for negative connection thread pool size");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
        try
        {
            virtualHost.setAttributes(Collections.<String, Object>singletonMap(QueueManagingVirtualHost.NUMBER_OF_SELECTORS, QueueManagingVirtualHost.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE));
            fail("Exception not thrown for number of selectors equal to connection thread pool size");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    public void testRegisterConnection() throws Exception
    {
        QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        AMQPConnection<?> connection = getMockConnection();

        assertEquals("unexpected number of connections before test", 0, vhost.getConnectionCount());
        vhost.registerConnection(connection);
        assertEquals("unexpected number of connections after registerConnection", 1, vhost.getConnectionCount());
        assertEquals("unexpected connection object", Collections.singleton(connection), vhost.getConnections());
    }

    public void testStopVirtualhostClosesConnections() throws Exception
    {
        QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        AMQPConnection<?> connection = getMockConnection();

        vhost.registerConnection(connection);
        assertEquals("unexpected number of connections after registerConnection", 1, vhost.getConnectionCount());
        assertEquals("unexpected connection object", Collections.singleton(connection), vhost.getConnections());
        ((AbstractConfiguredObject<?>)vhost).stop();
        verify(connection).stopConnection();
        verify(connection).closeAsync();
    }

    public void testRegisterConnectionOnStoppedVirtualhost() throws Exception
    {
        QueueManagingVirtualHost<?> vhost = createVirtualHost("sdf");
        AMQPConnection<?> connection = getMockConnection();

        ((AbstractConfiguredObject<?>)vhost).stop();
        try
        {
            vhost.registerConnection(connection);
            fail("exception not thrown");
        }
        catch (VirtualHostUnavailableException e)
        {
            // pass
        }
        assertEquals("unexpected number of connections", 0, vhost.getConnectionCount());
        ((AbstractConfiguredObject<?>)vhost).start();
        vhost.registerConnection(connection);
        assertEquals("unexpected number of connections", 1, vhost.getConnectionCount());
    }

    private AMQPConnection<?> getMockConnection()
    {
        AMQPConnection<?> connection = mock(AMQPConnection.class);
        final ListenableFuture<Void> listenableFuture = Futures.immediateFuture(null);
        when(connection.closeAsync()).thenReturn(listenableFuture);
        return connection;
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
        return host;
    }

    private AbstractAMQPConnection createMockProtocolConnection(final VirtualHost<?> virtualHost)
    {
        final AbstractAMQPConnection connection = mock(AbstractAMQPConnection.class);
        final List<Action<?>> tasks = new ArrayList<>();
        final ArgumentCaptor<Action> deleteTaskCaptor = ArgumentCaptor.forClass(Action.class);
        Answer answer = new Answer()
        {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
            {
                return tasks.add(deleteTaskCaptor.getValue());
            }
        };
        doAnswer(answer).when(connection).addDeleteTask(deleteTaskCaptor.capture());
        when(connection.getAddressSpace()).thenReturn(virtualHost);
        doAnswer(new Answer()
        {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
            {
                for(Action action : tasks)
                {
                    action.performAction(connection);
                }
                return null;
            }
        }).when(connection).sendConnectionCloseAsync(any(AMQPConnection.CloseReason.class), anyString());
        when(connection.getRemoteAddressString()).thenReturn("peer:1234");
        return connection;
    }

    private static ConfiguredObjectRecord matchesRecord(UUID id, String type)
    {
        return argThat(new MinimalConfiguredObjectRecordMatcher(id, type));
    }

    private static class MinimalConfiguredObjectRecordMatcher extends ArgumentMatcher<ConfiguredObjectRecord>
    {
        private final UUID _id;
        private final String _type;

        private MinimalConfiguredObjectRecordMatcher(UUID id, String type)
        {
            _id = id;
            _type = type;
        }

        @Override
        public boolean matches(Object argument)
        {
            ConfiguredObjectRecord rhs = (ConfiguredObjectRecord) argument;
            return (_id.equals(rhs.getId()) || _type.equals(rhs.getType()));
        }
    }
}
