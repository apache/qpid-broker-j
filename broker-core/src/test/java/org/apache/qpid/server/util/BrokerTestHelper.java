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
package org.apache.qpid.server.util;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.PrivilegedAction;
import java.util.*;

import javax.security.auth.Subject;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;
import org.apache.qpid.server.virtualhost.QueueExistsException;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.QpidTestCase;

public class BrokerTestHelper
{

    private static List<VirtualHost> _createdVirtualHosts = new ArrayList<>();

    private static final TaskExecutor TASK_EXECUTOR = new CurrentThreadTaskExecutor();
    private static Runnable _closeVirtualHosts = new Runnable()
    {
        @Override
        public void run()
        {
            for (VirtualHost virtualHost : _createdVirtualHosts)
            {
                virtualHost.close();
            }
            _createdVirtualHosts.clear();
        }
    };

    static
    {
        TASK_EXECUTOR.start();
    }

    public static Broker<?> createBrokerMock()
    {
        ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        EventLogger eventLogger = new EventLogger();

        SystemConfig systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(eventLogger);
        when(systemConfig.getObjectFactory()).thenReturn(objectFactory);
        when(systemConfig.getModel()).thenReturn(objectFactory.getModel());
        when(systemConfig.getCategoryClass()).thenReturn(SystemConfig.class);

        Broker broker = mock(Broker.class);
        when(broker.getConnection_sessionCountLimit()).thenReturn(1);
        when(broker.getConnection_closeWhenNoRoute()).thenReturn(false);
        when(broker.getId()).thenReturn(UUID.randomUUID());
        when(broker.getSecurityManager()).thenReturn(new SecurityManager(broker, false));
        when(broker.getObjectFactory()).thenReturn(objectFactory);
        when(broker.getModel()).thenReturn(objectFactory.getModel());
        when(broker.getModelVersion()).thenReturn(BrokerModel.MODEL_VERSION);
        when(broker.getEventLogger()).thenReturn(eventLogger);
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.getParent(SystemConfig.class)).thenReturn(systemConfig);
        when(broker.getContextValue(eq(Long.class), eq(Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT))).thenReturn(0l);

        when(broker.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(systemConfig.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(broker.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(systemConfig.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(systemConfig.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));

        return broker;
    }

    public static void setUp()
    {
    }

    public static void tearDown()
    {
    }

    public static VirtualHost<?> createVirtualHost(Map<String, Object> attributes)
    {

        Broker<?> broker = createBrokerMock();
        return createVirtualHost(attributes, broker, false);
    }

    private static VirtualHost<?> createVirtualHost(final Map<String, Object> attributes,
                                                        final Broker<?> broker, boolean defaultVHN)
    {
        ConfiguredObjectFactory objectFactory = broker.getObjectFactory();

        VirtualHostNode virtualHostNode = mock(VirtualHostNode.class);
        String virtualHostNodeName = String.format("%s_%s", attributes.get(VirtualHostNode.NAME), "_node");
        when(virtualHostNode.getName()).thenReturn( virtualHostNodeName);
        when(virtualHostNode.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.isDefaultVirtualHostNode()).thenReturn(defaultVHN);

        when(virtualHostNode.getParent(eq(Broker.class))).thenReturn(broker);

        Collection<VirtualHostNode<?>> nodes = broker.getVirtualHostNodes();
        nodes = new ArrayList<>(nodes != null ?  nodes : Collections.<VirtualHostNode<?>>emptyList());
        nodes.add(virtualHostNode);
        when(broker.getVirtualHostNodes()).thenReturn(nodes);

        DurableConfigurationStore dcs = mock(DurableConfigurationStore.class);
        when(virtualHostNode.getConfigurationStore()).thenReturn(dcs);
        when(virtualHostNode.getParent(eq(VirtualHostNode.class))).thenReturn(virtualHostNode);
        when(virtualHostNode.getModel()).thenReturn(objectFactory.getModel());
        when(virtualHostNode.getObjectFactory()).thenReturn(objectFactory);
        when(virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(virtualHostNode.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));

        AbstractVirtualHost
                host = (AbstractVirtualHost) objectFactory.create(VirtualHost.class, attributes, virtualHostNode );
        host.start();
        when(virtualHostNode.getVirtualHost()).thenReturn(host);
        _createdVirtualHosts.add(host);
        QpidTestCase testCase = QpidTestCase.getCurrentInstance();
        testCase.registerTearDown(_closeVirtualHosts);
        return host;
    }

    public static VirtualHost<?> createVirtualHost(String name) throws Exception
    {
        return createVirtualHost(name, createBrokerMock(), false);
    }

    public static VirtualHost<?> createVirtualHost(String name, Broker<?> broker, boolean defaultVHN) throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        attributes.put(org.apache.qpid.server.model.VirtualHost.NAME, name);

        return createVirtualHost(attributes, broker, defaultVHN);
    }

    public static AMQSessionModel<?> createSession(int channelId, AMQPConnection<?> connection)
    {
        @SuppressWarnings("rawtypes")
        AMQSessionModel session = mock(AMQSessionModel.class);
        when(session.getAMQPConnection()).thenReturn(connection);
        when(session.getChannelId()).thenReturn(channelId);
        return session;
    }

    public static AMQSessionModel<?> createSession(int channelId) throws Exception
    {
        AMQPConnection<?> session = createConnection();
        return createSession(channelId, session);
    }

    public static AMQSessionModel<?> createSession() throws Exception
    {
        return createSession(1);
    }

    public static AMQPConnection<?> createConnection() throws Exception
    {
        return createConnection("test");
    }

    public static AMQPConnection<?> createConnection(String hostName) throws Exception
    {
        return mock(AMQPConnection.class);
    }

    public static Exchange<?> createExchange(String hostName, final boolean durable, final EventLogger eventLogger) throws Exception
    {
        Broker broker = mock(Broker.class);
        when(broker.getModel()).thenReturn(BrokerModel.getInstance());
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        SecurityManager securityManager = new SecurityManager(broker, false);
        final VirtualHost<?> virtualHost = mock(VirtualHost.class);
        when(virtualHost.getName()).thenReturn(hostName);
        when(virtualHost.getSecurityManager()).thenReturn(securityManager);
        when(virtualHost.getEventLogger()).thenReturn(eventLogger);
        when(virtualHost.getDurableConfigurationStore()).thenReturn(mock(DurableConfigurationStore.class));
        final ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        when(virtualHost.getObjectFactory()).thenReturn(objectFactory);
        when(virtualHost.getModel()).thenReturn(objectFactory.getModel());
        when(virtualHost.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHost.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        final Map<String,Object> attributes = new HashMap<String, Object>();
        attributes.put(org.apache.qpid.server.model.Exchange.ID, UUIDGenerator.generateExchangeUUID("amp.direct", virtualHost.getName()));
        attributes.put(org.apache.qpid.server.model.Exchange.NAME, "amq.direct");
        attributes.put(org.apache.qpid.server.model.Exchange.TYPE, "direct");
        attributes.put(org.apache.qpid.server.model.Exchange.DURABLE, durable);
        return Subject.doAs(SecurityManager.getSubjectWithAddedSystemRights(), new PrivilegedAction<Exchange<?>>()
        {
            @Override
            public Exchange<?> run()
            {

                return (Exchange<?>) objectFactory.create(Exchange.class, attributes, virtualHost);
            }
        });

    }

    public static Queue<?> createQueue(String queueName, VirtualHost<?> virtualHost)
            throws QueueExistsException
    {
        Map<String,Object> attributes = new HashMap<String, Object>();
        attributes.put(Queue.ID, UUIDGenerator.generateRandomUUID());
        attributes.put(Queue.NAME, queueName);
        Queue<?> queue = virtualHost.createChild(Queue.class, attributes);
        return queue;
    }

}
