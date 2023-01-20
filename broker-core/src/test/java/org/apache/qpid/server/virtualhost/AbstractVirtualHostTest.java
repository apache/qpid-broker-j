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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AbstractVirtualHostTest extends UnitTestBase
{
    private TaskExecutor _taskExecutor;
    private VirtualHostNode _node;
    private MessageStore _failingStore;

    @BeforeEach
    public void setUp() throws Exception
    {
        final SystemConfig systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(mock(EventLogger.class));
        when(systemConfig.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
        final AccessControl<?> accessControlMock = BrokerTestHelper.createAccessControlMock();
        final Principal systemPrincipal = mock(Principal.class);
        final Broker broker = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(Broker.class, systemPrincipal,
                accessControlMock);
        when(broker.getParent()).thenReturn(systemConfig);
        when(broker.getModel()).thenReturn(BrokerModel.getInstance());

        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();
        when(broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(broker.getChildExecutor()).thenReturn(_taskExecutor);

        _node = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class,
                systemPrincipal, accessControlMock);
        when(_node.getParent()).thenReturn(broker);
        when(_node.getModel()).thenReturn(BrokerModel.getInstance());
        when(_node.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_node.getChildExecutor()).thenReturn(_taskExecutor);
        when(_node.getConfigurationStore()).thenReturn(mock(DurableConfigurationStore.class));
        when(_node.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(_node.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
        when(_node.getEventLogger()).thenReturn(mock(EventLogger.class));

        _failingStore = mock(MessageStore.class);
        doThrow(new RuntimeException("Cannot open store")).when(_failingStore).openMessageStore(any(ConfiguredObject.class));
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_taskExecutor != null)
        {
            _taskExecutor.stop();
        }
    }

    @Test
    public void testValidateMessageStoreCreationFails()
    {
        final Map<String,Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());

        final AbstractVirtualHost host = new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return _failingStore;
            }
        };

        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                host::validateMessageStoreCreation,
                "Validation on creation should fail");
        assertTrue(thrown.getMessage().startsWith("Cannot open virtual host message store"),
                "Unexpected exception " + thrown.getMessage());
        host.close();
    }

    @Test
    public void testValidateMessageStoreCreationSucceeds()
    {
        final Map<String,Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());
        final MessageStore store = mock(MessageStore.class);
        final AbstractVirtualHost host = new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return store;
            }
        };

        host.validateMessageStoreCreation();
        verify(store).openMessageStore(host);
        verify(store).closeMessageStore();
        host.close();
    }

    @Test
    public void testOpenFails()
    {
        final Map<String,Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());

        final AbstractVirtualHost host = new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return _failingStore;
            }
        };

        host.open();
        assertEquals(State.ERRORED, host.getState(), "Unexpected host state");
        host.close();
    }

    @Test
    public void testOpenSucceeds()
    {
        final Map<String,Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());
        final MessageStore store = mock(MessageStore.class);
        when(store.newMessageStoreReader()).thenReturn(mock(MessageStore.MessageStoreReader.class));

        final AbstractVirtualHost host = new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return  store;
            }

            @Override
            protected void onExceptionInOpen(final RuntimeException e)
            {
                fail("open failed");
            }
        };

        host.open();
        assertEquals(State.ACTIVE, host.getState(), "Unexpected host state");
        verify(store, atLeastOnce()).openMessageStore(host);
        host.close();
    }

    @Test
    public void testDeleteInErrorStateAfterOpen()
    {
        final Map<String,Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());
        final AbstractVirtualHost host = new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return  _failingStore;
            }
        };

        host.open();

        assertEquals(State.ERRORED, host.getState(), "Unexpected state");

        host.delete();
        assertEquals(State.DELETED, host.getState(), "Unexpected state");
    }

    @Test
    public void testActivateInErrorStateAfterOpen()
    {
        final Map<String,Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());
        final MessageStore store = mock(MessageStore.class);
        doThrow(new RuntimeException("Cannot open store")).when(store).openMessageStore(any(ConfiguredObject.class));
        final AbstractVirtualHost host = new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return  store;
            }
        };

        host.open();
        assertEquals(State.ERRORED, host.getState(), "Unexpected state");

        doNothing().when(store).openMessageStore(any(ConfiguredObject.class));
        when(store.newMessageStoreReader()).thenReturn(mock(MessageStore.MessageStoreReader.class));

        host.setAttributes(Collections.<String, Object>singletonMap(VirtualHost.DESIRED_STATE, State.ACTIVE));
        assertEquals(State.ACTIVE, host.getState(), "Unexpected state");
        host.close();
    }

    @Test
    public void testStartInErrorStateAfterOpen()
    {
        final Map<String,Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());
        final MessageStore store = mock(MessageStore.class);
        doThrow(new RuntimeException("Cannot open store")).when(store).openMessageStore(any(ConfiguredObject.class));
        final AbstractVirtualHost host = new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return  store;
            }
        };

        host.open();
        assertEquals(State.ERRORED, host.getState(), "Unexpected state");

        doNothing().when(store).openMessageStore(any(ConfiguredObject.class));
        when(store.newMessageStoreReader()).thenReturn(mock(MessageStore.MessageStoreReader.class));

        host.start();
        assertEquals(State.ACTIVE, host.getState(), "Unexpected state");
        host.close();
    }

    // This indirectly tests QPID-6283
    @Test
    public void testFileSystemCheckWarnsWhenFileSystemDoesNotExist() throws Exception
    {
        final Map<String,Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());
        final MessageStore store = mock(MessageStore.class);
        when(store.newMessageStoreReader()).thenReturn(mock(MessageStore.MessageStoreReader.class));
        final File nonExistingFile = TestFileUtils.createTempFile(this);
        FileUtils.delete(nonExistingFile, false);
        when(store.getStoreLocationAsFile()).thenReturn(nonExistingFile);
        setTestSystemProperty("virtualhost.housekeepingCheckPeriod", "100");

        final AbstractVirtualHost host = new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return  store;
            }
        };

        final String loggerName = AbstractVirtualHost.class.getName();
        assertActionProducesLogMessage(host::open, loggerName, Level.WARN, "Cannot check file system for disk space");
        host.close();
    }

    private void assertActionProducesLogMessage(final Runnable action, final String loggerName,
                                                final Level logLevel, final String message) throws Exception
    {
        final CountDownLatch logMessageReceivedLatch = new CountDownLatch(1);
        final ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.addFilter(new Filter<ILoggingEvent>()
        {
            @Override
            public FilterReply decide(final ILoggingEvent event)
            {
                if (event.getLoggerName().equals(loggerName) && event.getLevel().equals(logLevel) && event.getFormattedMessage().contains(message))
                {
                    logMessageReceivedLatch.countDown();
                }
                return FilterReply.NEUTRAL;
            }
        });
        appender.setContext(rootLogger.getLoggerContext());
        appender.start();
        rootLogger.addAppender(appender);

        action.run();
        assertTrue(logMessageReceivedLatch.await(2, TimeUnit.SECONDS), "Did not receive expected log message");
    }

    @Test
    public void testClearMatchingQueues()
    {
        final List<Queue> queues = new ArrayList<>();
        final Queue<?> queueA = newQueue("queueA");
        queues.add(queueA);
        final Queue<?> topic = newQueue("queue-topic");
        queues.add(topic);
        final Queue<?> queueB = newQueue("queueB");
        queues.add(queueB);

        newVirtualHost(queues).clearMatchingQueues("queue.?");
        Mockito.verify(queueA).clearQueue();
        Mockito.verify(queueB).clearQueue();
        Mockito.verify(topic, Mockito.never()).clearQueue();
    }

    @Test
    public void testClearMatchingQueues_any()
    {
        final List<Queue> queues = new ArrayList<>();
        queues.add(newQueue("queueA"));
        queues.add(newQueue("queue-topic"));
        queues.add(newQueue("queueB"));

        newVirtualHost(queues).clearMatchingQueues(".*");
        for (final Queue<?> queue : queues)
        {
            Mockito.verify(queue).clearQueue();
        }
    }

    @Test
    public void testClearMatchingQueues_exception()
    {
        final List<Queue> queues = new ArrayList<>();
        queues.add(newQueue("queueA"));
        queues.add(newQueue("queue-topic"));
        queues.add(newQueue("queueB"));

        final AbstractVirtualHost host = newVirtualHost(queues);
        try
        {
            host.clearMatchingQueues(".*[");
            fail("An exception is expected!");
        }
        catch (RuntimeException e)
        {
            assertNotNull(e.getMessage());
        }
        for (final Queue<?> queue : queues)
        {
            Mockito.verify(queue, Mockito.never()).clearQueue();
        }
    }

    @Test
    public void testClearQueues()
    {
        final List<Queue> queues = new ArrayList<>();
        final Queue<?> queueA = newQueue("queueA");
        queues.add(queueA);
        final Queue<?> topic = newQueue("queue-topic");
        queues.add(topic);
        final Queue<?> queueB = newQueue("queueB");
        queues.add(queueB);

        newVirtualHost(queues).clearQueues(Arrays.asList("queue-topic", queueB.getId().toString()));
        Mockito.verify(queueA, Mockito.never()).clearQueue();
        Mockito.verify(queueB).clearQueue();
        Mockito.verify(topic).clearQueue();
    }

    @Test
    public void testClearQueues_none()
    {
        final List<Queue> queues = new ArrayList<>();
        queues.add(newQueue("queueA"));
        queues.add(newQueue("queue-topic"));
        queues.add(newQueue("queueB"));

        newVirtualHost(queues).clearQueues(Set.of());
        for (final Queue<?> queue : queues)
        {
            Mockito.verify(queue, Mockito.never()).clearQueue();
        }
    }

    private AbstractVirtualHost newVirtualHost(List<Queue> queues)
    {
        final Map<String, Object> attributes = Map.of(AbstractVirtualHost.NAME, getTestName());
        final MessageStore store = mock(MessageStore.class);
        return new AbstractVirtualHost(attributes, _node)
        {
            @Override
            protected MessageStore createMessageStore()
            {
                return store;
            }

            @Override
            public Collection getChildren(Class clazz)
            {
                if (clazz == Queue.class)
                {
                    return queues;
                }
                else
                {
                    return super.getChildren(clazz);
                }

            }
        };
    }

    private Queue<?> newQueue(final String name)
    {
        final Queue<?> queue = Mockito.mock(Queue.class);
        Mockito.doReturn(name).when(queue).getName();

        final UUID uuid = randomUUID();
        Mockito.doReturn(uuid).when(queue).getId();

        Mockito.doReturn(Queue.class).when(queue).getCategoryClass();
        return queue;
    }
}
