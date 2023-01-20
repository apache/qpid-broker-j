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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BrokerTestHelper
{
    private static final Principal PRINCIPAL = () -> "TEST";
    private static final Subject SYSTEM_SUBJECT = new Subject(true, Set.of(PRINCIPAL), Set.of(), Set.of());
    private static final List<VirtualHost> CREATED_VIRTUAL_HOSTS = new ArrayList<>();
    private static final TaskExecutor TASK_EXECUTOR = new CurrentThreadTaskExecutor();

    static
    {
        TASK_EXECUTOR.start();
    }

    private static final Runnable CLOSE_VIRTUAL_HOSTS = () ->
    {
        CREATED_VIRTUAL_HOSTS.forEach(VirtualHost::close);
        CREATED_VIRTUAL_HOSTS.clear();
    };

    private static Broker _broker;

    public static Broker<?> createBrokerMock()
    {
        if (_broker != null)
        {
            return _broker;
        }
        return createBrokerMock(createAccessControlMock(), true);
    }

    public static Broker<?> createNewBrokerMock()
    {
        return createBrokerMock(createAccessControlMock(), false);
    }

    public static AccessControl createAccessControlMock()
    {
        final AccessControl mock = mock(AccessControl.class);
        when(mock.authorise(any(SecurityToken.class), any(Operation.class), any(PermissionedObject.class))).thenReturn(
                Result.DEFER);
        when(mock.authorise(isNull(), any(Operation.class), any(PermissionedObject.class))).thenReturn(
                Result.DEFER);
        when(mock.authorise(any(SecurityToken.class), any(Operation.class), any(PermissionedObject.class), any(Map.class))).thenReturn(Result.DEFER);
        when(mock.authorise(isNull(), any(Operation.class), any(PermissionedObject.class), any(Map.class))).thenReturn(Result.DEFER);
        when(mock.getDefault()).thenReturn(Result.ALLOWED);
        return mock;
    }

    private static Broker<?> createBrokerMock(final AccessControl accessControl, final boolean cacheBroker)
    {
        final ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        final EventLogger eventLogger = new EventLogger();

        final SystemConfig systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(eventLogger);
        when(systemConfig.getObjectFactory()).thenReturn(objectFactory);
        when(systemConfig.getModel()).thenReturn(objectFactory.getModel());
        when(systemConfig.getCategoryClass()).thenReturn(SystemConfig.class);
        when(systemConfig.getTypeClass()).thenReturn(SystemConfig.class);

        final Broker broker = mockWithSystemPrincipalAndAccessControl(Broker.class, PRINCIPAL, accessControl);
        when(broker.getId()).thenReturn(UUID.randomUUID());
        when(broker.getObjectFactory()).thenReturn(objectFactory);
        when(broker.getModel()).thenReturn(objectFactory.getModel());
        when(broker.getModelVersion()).thenReturn(BrokerModel.MODEL_VERSION);
        when(broker.getEventLogger()).thenReturn(eventLogger);
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.getTypeClass()).thenReturn(Broker.class);
        when(broker.getParent()).thenReturn(systemConfig);
        when(broker.getContextValue(eq(Long.class), eq(Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT))).thenReturn(0L);
        when(broker.getFlowToDiskThreshold()).thenReturn(Long.MAX_VALUE);
        when(broker.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(broker.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(systemConfig.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(systemConfig.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(systemConfig.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
        when(systemConfig.getChildren(Broker.class)).thenReturn(Set.of(broker));

        if (cacheBroker)
        {
            _broker = broker;
        }

        return broker;
    }

    public static QueueManagingVirtualHost<?> createVirtualHost(final Map<String, Object> attributes,
                                                                final UnitTestBase testBase)
    {
        final Broker<?> broker = createBrokerMock(createAccessControlMock(), true);
        return createVirtualHost(attributes, broker, false, createAccessControlMock(), testBase);
    }

    private static QueueManagingVirtualHost<?> createVirtualHost(final Map<String, Object> attributes,
                                                                 final Broker<?> broker,
                                                                 final boolean defaultVHN,
                                                                 final AccessControl accessControl,
                                                                 final UnitTestBase testBase)
    {
        final ConfiguredObjectFactory objectFactory = broker.getObjectFactory();
        final String virtualHostNodeName = String.format("%s_%s", attributes.get(VirtualHostNode.NAME), "_node");
        final VirtualHostNode virtualHostNode =
                createVirtualHostNodeMock(virtualHostNodeName, defaultVHN, accessControl, broker);

        final AbstractVirtualHost host = (AbstractVirtualHost) objectFactory
                .create(VirtualHost.class, attributes, virtualHostNode );
        host.start();
        when(virtualHostNode.getVirtualHost()).thenReturn(host);
        CREATED_VIRTUAL_HOSTS.add(host);

        testBase.registerAfterAllTearDown(CLOSE_VIRTUAL_HOSTS);
        return host;
    }

    public static VirtualHostNode createVirtualHostNodeMock(final String virtualHostNodeName,
                                                            final boolean defaultVHN,
                                                            final AccessControl accessControl,
                                                            final Broker<?> broker)
    {
        final ConfiguredObjectFactory objectFactory = broker.getObjectFactory();

        final VirtualHostNode virtualHostNode = mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class,
                                                                                        PRINCIPAL, accessControl);
        when(virtualHostNode.getName()).thenReturn( virtualHostNodeName);
        when(virtualHostNode.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.isDefaultVirtualHostNode()).thenReturn(defaultVHN);

        when(virtualHostNode.getParent()).thenReturn(broker);

        Collection<VirtualHostNode<?>> nodes = broker.getVirtualHostNodes();
        nodes = new ArrayList<>(nodes != null ?  nodes : List.of());
        nodes.add(virtualHostNode);
        when(broker.getVirtualHostNodes()).thenReturn(nodes);

        final DurableConfigurationStore dcs = mock(DurableConfigurationStore.class);
        when(virtualHostNode.getConfigurationStore()).thenReturn(dcs);
        when(virtualHostNode.getModel()).thenReturn(objectFactory.getModel());
        when(virtualHostNode.getObjectFactory()).thenReturn(objectFactory);
        when(virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(virtualHostNode.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
        return virtualHostNode;
    }

    public static QueueManagingVirtualHost<?> createVirtualHost(final String name,
                                                                final UnitTestBase testBase) throws Exception
    {
        return createVirtualHost(name, createBrokerMock(createAccessControlMock(), true), false, createAccessControlMock(),
                                 testBase);
    }

    public static QueueManagingVirtualHost<?> createVirtualHost(final String name,
                                                                final Broker<?> broker,
                                                                final boolean defaultVHN,
                                                                final UnitTestBase testBase) throws Exception
    {
        return createVirtualHost(name, broker, defaultVHN, createAccessControlMock(), testBase);
    }

    private static QueueManagingVirtualHost<?> createVirtualHost(final String name,
                                                                 final Broker<?> broker,
                                                                 final boolean defaultVHN,
                                                                 final AccessControl accessControl,
                                                                 final UnitTestBase testBase)
    {
        final Map<String,Object> attributes = Map.of(
                org.apache.qpid.server.model.VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE,
                org.apache.qpid.server.model.VirtualHost.NAME, name);

        return createVirtualHost(attributes, broker, defaultVHN, accessControl, testBase);
    }

    public static AMQPSession<?,?> createSession(final int channelId, final AMQPConnection<?> connection)
    {
        final AMQPSession session = mock(AMQPSession.class);
        when(session.getAMQPConnection()).thenReturn(connection);
        when(session.getChannelId()).thenReturn(channelId);
        return session;
    }

    public static AMQPSession<?,?> createSession(final int channelId) throws Exception
    {
        final AMQPConnection<?> session = createConnection();
        return createSession(channelId, session);
    }

    public static AMQPSession<?,?> createSession() throws Exception
    {
        return createSession(1);
    }

    public static AMQPConnection<?> createConnection()
    {
        return createConnection("test");
    }

    public static AMQPConnection<?> createConnection(final String hostName)
    {
        return mock(AMQPConnection.class);
    }

    public static Exchange<?> createExchange(final String hostName, final boolean durable, final EventLogger eventLogger)
    {
        final QueueManagingVirtualHost virtualHost =  mockWithSystemPrincipal(QueueManagingVirtualHost.class, PRINCIPAL);
        when(virtualHost.getName()).thenReturn(hostName);
        when(virtualHost.getEventLogger()).thenReturn(eventLogger);
        when(virtualHost.getDurableConfigurationStore()).thenReturn(mock(DurableConfigurationStore.class));
        final ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        when(virtualHost.getObjectFactory()).thenReturn(objectFactory);
        when(virtualHost.getModel()).thenReturn(objectFactory.getModel());
        when(virtualHost.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHost.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        final Map<String,Object> attributes = Map.of(
                Exchange.ID, UUIDGenerator.generateExchangeUUID("amp.direct", virtualHost.getName()),
                Exchange.NAME, "amq.direct",
                Exchange.TYPE, "direct",
                Exchange.DURABLE, durable);

        return Subject.doAs(SYSTEM_SUBJECT, (PrivilegedAction<Exchange<?>>) () ->
                (Exchange<?>) objectFactory.create(Exchange.class, attributes, virtualHost));

    }

    public static Queue<?> createQueue(final String queueName, final VirtualHost<?> virtualHost)
    {
        final Map<String,Object> attributes = Map.of(Queue.ID, UUIDGenerator.generateRandomUUID(),
                Queue.NAME, queueName);
        return virtualHost.createChild(Queue.class, attributes);
    }

    // The generated classes can't directly inherit from SystemPricipalSource / AccessControlSource as
    // these are package local, and package local visibility is prevented between classes loaded from different
    // class loaders.  Using these "public" interfaces gets around the problem.
    public interface TestableSystemPrincipalSource extends SystemPrincipalSource {}
    public interface TestableAccessControlSource extends AccessControlSource {}

    public static <X extends ConfiguredObject> X mockWithSystemPrincipal(final Class<X> clazz, final Principal principal)
    {
        final X mock = mock(clazz, withSettings().extraInterfaces(TestableSystemPrincipalSource.class));
        when(((SystemPrincipalSource)mock).getSystemPrincipal()).thenReturn(principal);
        return mock;
    }

    public static <X extends ConfiguredObject> X mockWithSystemPrincipalAndAccessControl(final Class<X> clazz,
                                                                                         final Principal principal,
                                                                                         final AccessControl accessControl)
    {
        final X mock = mock(clazz, withSettings().extraInterfaces(TestableSystemPrincipalSource.class,
                TestableAccessControlSource.class));
        when(((SystemPrincipalSource) mock).getSystemPrincipal()).thenReturn(principal);
        when(((AccessControlSource) mock).getAccessControl()).thenReturn(accessControl);

        return mock;
    }

    public static <X extends ConfiguredObject> X mockAsSystemPrincipalSource(final Class<X> clazz)
    {
        return mockWithSystemPrincipal(clazz, PRINCIPAL);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static ServerMessage<?> createMessage(final Long id)
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(String.valueOf(id));

        final ServerMessage<?> message = mock(ServerMessage.class);
        when(message.getMessageNumber()).thenReturn(id);
        when(message.getMessageHeader()).thenReturn(header);
        when(message.checkValid()).thenReturn(true);
        when(message.getSizeIncludingHeader()).thenReturn(100L);
        when(message.getArrivalTime()).thenReturn(System.currentTimeMillis());

        final StoredMessage storedMessage = mock(StoredMessage.class);
        when(message.getStoredMessage()).thenReturn(storedMessage);

        final MessageReference ref = mock(MessageReference.class);
        when(ref.getMessage()).thenReturn(message);

        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);

        return message;
    }
}
