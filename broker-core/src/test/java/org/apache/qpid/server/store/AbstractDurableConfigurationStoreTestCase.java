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
package org.apache.qpid.server.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public abstract class AbstractDurableConfigurationStoreTestCase extends UnitTestBase
{
    private static final String EXCHANGE = org.apache.qpid.server.model.Exchange.class.getSimpleName();
    private static final String QUEUE = Queue.class.getSimpleName();
    private static final UUID ANY_UUID = UUID.randomUUID();
    private static final Map ANY_MAP = new HashMap();
    private static final String STANDARD = "standard";

    private String _storePath;
    private String _storeName;

    private ConfiguredObjectRecordHandler _handler;

    private Map<String,Object> _bindingArgs;
    private UUID _queueId;
    private UUID _exchangeId;
    private DurableConfigurationStore _configStore;
    private ConfiguredObjectFactoryImpl _factory;

    private VirtualHostNode<?> _parent;

    private ConfiguredObjectRecord _rootRecord;

    @Before
    public void setUp() throws Exception
    {

        _queueId = UUIDGenerator.generateRandomUUID();
        _exchangeId = UUIDGenerator.generateRandomUUID();
        _factory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        _storeName = getTestName();
        _storePath = TMP_FOLDER + File.separator + _storeName;
        FileUtils.delete(new File(_storePath), true);

        _handler = mock(ConfiguredObjectRecordHandler.class);

        _bindingArgs = new HashMap<>();
        String argKey = AMQPFilterTypes.JMS_SELECTOR.toString();
        String argValue = "some selector expression";
        _bindingArgs.put(argKey, argValue);

        _parent = createVirtualHostNode(_storePath, _factory);

        _configStore = createConfigStore();
        _configStore.init(_parent);
        _configStore.openConfigurationStore(new ConfiguredObjectRecordHandler()
        {

            @Override
            public void handle(final ConfiguredObjectRecord record)
            {
            }

        });
        _rootRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(), VirtualHost.class.getSimpleName(), Collections.<String, Object>singletonMap(ConfiguredObject.NAME, "vhost"));
        _configStore.create(_rootRecord);
    }

    protected VirtualHostNode<?> getVirtualHostNode()
    {
        return _parent;
    }

    protected DurableConfigurationStore getConfigurationStore()
    {
        return _configStore;
    }

    protected abstract VirtualHostNode createVirtualHostNode(String storeLocation, ConfiguredObjectFactory factory);

    @After
    public void tearDown() throws Exception
    {
        try
        {
            closeConfigStore();
        }
        finally
        {
            if (_storePath != null)
            {
                FileUtils.delete(new File(_storePath), true);
            }
        }
    }

    @Test
    public void testCloseIsIdempotent() throws Exception
    {
        _configStore.closeConfigurationStore();
        // Second close should be accepted without exception
        _configStore.closeConfigurationStore();
    }

    @Test
    public void testCreateExchange() throws Exception
    {
        Exchange<?> exchange = createTestExchange();
        _configStore.create(exchange.asObjectRecord());

        reopenStore();
        _configStore.openConfigurationStore(_handler);

        verify(_handler).handle(matchesRecord(_exchangeId, EXCHANGE,
                map( org.apache.qpid.server.model.Exchange.NAME, getTestName(),
                        org.apache.qpid.server.model.Exchange.TYPE, getTestName()+"Type",
                        org.apache.qpid.server.model.Exchange.LIFETIME_POLICY, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS.name())));
    }

    protected Map<String,Object> map(Object... vals)
    {
        Map<String,Object> map = new HashMap<>();
        boolean isValue = false;
        String key = null;
        for(Object obj : vals)
        {
            if(isValue)
            {
                map.put(key,obj);
            }
            else
            {
                key = (String) obj;
            }
            isValue = !isValue;
        }
        return map;
    }

    @Test
    public void testRemoveExchange() throws Exception
    {
        Exchange<?> exchange = createTestExchange();
        _configStore.create(exchange.asObjectRecord());

        _configStore.remove(exchange.asObjectRecord());

        reopenStore();
        verify(_handler, never()).handle(any(ConfiguredObjectRecord.class));
    }

    protected ConfiguredObjectRecord matchesRecord(UUID id, String type, Map<String, Object> attributes)
    {
        return argThat(new ConfiguredObjectMatcher(id, type, attributes, ANY_MAP));
    }

    protected ConfiguredObjectRecord matchesRecord(UUID id, String type, Map<String, Object> attributes, Map<String,UUID> parents)
    {
        return argThat(new ConfiguredObjectMatcher(id, type, attributes, parents));
    }

    private static class ConfiguredObjectMatcher implements ArgumentMatcher<ConfiguredObjectRecord>
    {
        private final Map<String,Object> _matchingMap;
        private final UUID _id;
        private final String _name;
        private final Map<String,UUID> _parents;

        private ConfiguredObjectMatcher(final UUID id, final String type, final Map<String, Object> matchingMap, Map<String,UUID> parents)
        {
            _id = id;
            _name = type;
            _matchingMap = matchingMap;
            _parents = parents;
        }

        @Override
        public boolean matches(final ConfiguredObjectRecord binding)
        {
            Map<String,Object> arg = new HashMap<>(binding.getAttributes());
            arg.remove("createdBy");
            arg.remove("createdTime");
            arg.remove("lastUpdatedTime");
            arg.remove("lastUpdatedBy");
            return (_id == ANY_UUID || _id.equals(binding.getId()))
                   && _name.equals(binding.getType())
                   && (_matchingMap == ANY_MAP || arg.equals(_matchingMap))
                   && (_parents == ANY_MAP || matchesParents(binding));
        }

        private boolean matchesParents(ConfiguredObjectRecord binding)
        {
            Map<String, UUID> bindingParents = binding.getParents();
            if(bindingParents.size() != _parents.size())
            {
                return false;
            }
            for(Map.Entry<String,UUID> entry : _parents.entrySet())
            {
                if(!bindingParents.get(entry.getKey()).equals(entry.getValue()))
                {
                    return false;
                }
            }
            return true;
        }
    }

    @Test
    public void testCreateQueue() throws Exception
    {
        Queue<?> queue = createTestQueue(getTestName(), getTestName() + "Owner", true, null);
        _configStore.create(queue.asObjectRecord());

        reopenStore();
        _configStore.openConfigurationStore(_handler);

        Map<String, Object> queueAttributes = new HashMap<>();
        queueAttributes.put(Queue.NAME, getTestName());
        queueAttributes.put(Queue.OWNER, getTestName() + "Owner");
        queueAttributes.put(Queue.EXCLUSIVE, ExclusivityPolicy.CONTAINER.name());
        queueAttributes.put(Queue.TYPE, STANDARD);
        verify(_handler).handle(matchesRecord(_queueId, QUEUE, queueAttributes));
    }

    @Test
    public void testUpdateQueue() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.TYPE, STANDARD);
        Queue<?> queue = createTestQueue(getTestName(), getTestName() + "Owner", true, attributes);

        _configStore.create(queue.asObjectRecord());

        // update the queue to have exclusive=false
        queue = createTestQueue(getTestName(), getTestName() + "Owner", false, attributes);

        _configStore.update(false, queue.asObjectRecord());

        reopenStore();
        _configStore.openConfigurationStore(_handler);

        Map<String,Object> queueAttributes = new HashMap<>();

        queueAttributes.put(Queue.NAME, getTestName());
        queueAttributes.putAll(attributes);

        verify(_handler).handle(matchesRecord(_queueId, QUEUE, queueAttributes));

    }


    @Test
    public void testRemoveQueue() throws Exception
    {
        Queue<?> queue = createTestQueue(getTestName(), getTestName() + "Owner", true, Collections.emptyMap());
        _configStore.create(queue.asObjectRecord());

        _configStore.remove(queue.asObjectRecord());
        reopenStore();
        verify(_handler, never()).handle(any(ConfiguredObjectRecord.class));
    }

    private Queue<?> createTestQueue(String queueName,
                                     String queueOwner,
                                     boolean exclusive,
                                     final Map<String, Object> arguments) throws StoreException
    {
        Queue queue = BrokerTestHelper.mockWithSystemPrincipal(Queue.class, mock(Principal.class));
        when(queue.getName()).thenReturn(queueName);
        when(queue.isExclusive()).thenReturn(exclusive);
        when(queue.getId()).thenReturn(_queueId);
        when(queue.getType()).thenReturn(STANDARD);
        when(queue.getCategoryClass()).thenReturn(Queue.class);
        when(queue.isDurable()).thenReturn(true);
        TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        when(queue.getTaskExecutor()).thenReturn(taskExecutor);
        when(queue.getChildExecutor()).thenReturn(taskExecutor);

        final QueueManagingVirtualHost vh = mock(QueueManagingVirtualHost.class);
        when(queue.getVirtualHost()).thenReturn(vh);
        final Map<String,Object> attributes = arguments == null ? new LinkedHashMap<>() : new LinkedHashMap<>(arguments);
        attributes.put(Queue.NAME, queueName);
        attributes.put(Queue.TYPE, STANDARD);
        if(exclusive)
        {
            when(queue.getOwner()).thenReturn(queueOwner);

            attributes.put(Queue.OWNER, queueOwner);
            attributes.put(Queue.EXCLUSIVE, ExclusivityPolicy.CONTAINER);
        }
            when(queue.getAvailableAttributes()).thenReturn(attributes.keySet());
            final ArgumentCaptor<String> requestedAttribute = ArgumentCaptor.forClass(String.class);
            when(queue.getAttribute(requestedAttribute.capture())).then(
                    new Answer()
                    {

                        @Override
                        public Object answer(final InvocationOnMock invocation) throws Throwable
                        {
                            String attrName = requestedAttribute.getValue();
                            return attributes.get(attrName);
                        }
                    });

        when(queue.getActualAttributes()).thenReturn(attributes);

        when(queue.getObjectFactory()).thenReturn(_factory);
        when(queue.getModel()).thenReturn(_factory.getModel());
        ConfiguredObjectRecord objectRecord = mock(ConfiguredObjectRecord.class);
        when(objectRecord.getId()).thenReturn(_queueId);
        when(objectRecord.getType()).thenReturn(Queue.class.getSimpleName());
        when(objectRecord.getAttributes()).thenReturn(attributes);
        when(objectRecord.getParents()).thenReturn(Collections.singletonMap(_rootRecord.getType(), _rootRecord.getId()));
        when(queue.asObjectRecord()).thenReturn(objectRecord);
        return queue;
    }

    private Exchange<?> createTestExchange()
    {
        Exchange exchange = mock(Exchange.class);
        Map<String,Object> actualAttributes = new HashMap<>();
        actualAttributes.put("name", getTestName());
        actualAttributes.put("type", getTestName() + "Type");
        actualAttributes.put("lifetimePolicy", LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);
        when(exchange.getName()).thenReturn(getTestName());
        when(exchange.getType()).thenReturn(getTestName() + "Type");
        when(exchange.isAutoDelete()).thenReturn(true);
        when(exchange.getId()).thenReturn(_exchangeId);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);
        when(exchange.isDurable()).thenReturn(true);
        when(exchange.getObjectFactory()).thenReturn(_factory);
        when(exchange.getModel()).thenReturn(_factory.getModel());
        TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        when(exchange.getTaskExecutor()).thenReturn(taskExecutor);
        when(exchange.getChildExecutor()).thenReturn(taskExecutor);

        ConfiguredObjectRecord exchangeRecord = mock(ConfiguredObjectRecord.class);
        when(exchangeRecord.getId()).thenReturn(_exchangeId);
        when(exchangeRecord.getType()).thenReturn(Exchange.class.getSimpleName());
        when(exchangeRecord.getAttributes()).thenReturn(actualAttributes);
        when(exchangeRecord.getParents()).thenReturn(Collections.singletonMap(_rootRecord.getType(), _rootRecord.getId()));
        when(exchange.asObjectRecord()).thenReturn(exchangeRecord);
        when(exchange.getEventLogger()).thenReturn(new EventLogger());
        return exchange;
    }

    private void reopenStore() throws Exception
    {
        closeConfigStore();
        _configStore = createConfigStore();
        _configStore.init(_parent);
    }

    protected abstract DurableConfigurationStore createConfigStore() throws Exception;

    protected void closeConfigStore() throws Exception
    {
        if (_configStore != null)
        {
            _configStore.closeConfigurationStore();
        }
    }
}
