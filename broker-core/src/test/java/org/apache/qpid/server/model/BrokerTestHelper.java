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

import static org.apache.bcel.Constants.ACC_INTERFACE;
import static org.apache.bcel.Constants.ACC_PUBLIC;
import static org.apache.bcel.Constants.ACC_SUPER;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;

import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.ClassGen;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.QpidTestCase;

public class BrokerTestHelper
{

    private static final Principal SYSTEM_PRINCIPAL =
            new Principal()
            {

                @Override
                public String getName()
                {
                    return "TEST";
                }
            };
    private static final Subject SYSTEM_SUBJECT = new Subject(true,
                                                              Collections.singleton(SYSTEM_PRINCIPAL),
                                                              Collections.emptySet(),
                                                              Collections.emptySet());
    private static final Map<Class<? extends ConfiguredObject>, Class<? extends ConfiguredObject>>
            SYSTEM_PRINCIPAL_SOURCE_MOCKS = new HashMap<>();
    private static final Map<Class<? extends ConfiguredObject>, Class<? extends ConfiguredObject>>
            SYSTEM_PRINCIPAL_AND_ACCESS_CONTROL_SOURCE_MOCKS = new HashMap<>();
    private static final AtomicInteger GENERATED_INTERFACE_COUNTER = new AtomicInteger();


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
        return createBrokerMock(createAccessControlMock());
    }

    public static AccessControl createAccessControlMock()
    {
        AccessControl mock = mock(AccessControl.class);
        when(mock.authorise(any(SecurityToken.class), any(Operation.class), any(ConfiguredObject.class))).thenReturn(
                Result.DEFER);
        when(mock.authorise(any(SecurityToken.class), any(Operation.class), any(ConfiguredObject.class), any(Map.class))).thenReturn(Result.DEFER);
        when(mock.getDefault()).thenReturn(Result.ALLOWED);
        return mock;
    }

    private static Broker<?> createBrokerMock(AccessControl accessControl)
    {
        ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        EventLogger eventLogger = new EventLogger();

        SystemConfig systemConfig = mock(SystemConfig.class);
        when(systemConfig.getEventLogger()).thenReturn(eventLogger);
        when(systemConfig.getObjectFactory()).thenReturn(objectFactory);
        when(systemConfig.getModel()).thenReturn(objectFactory.getModel());
        when(systemConfig.getCategoryClass()).thenReturn(SystemConfig.class);

        Broker broker = mockWithSystemPrincipalAndAccessControl(Broker.class, SYSTEM_PRINCIPAL, accessControl);
        when(broker.getConnection_sessionCountLimit()).thenReturn(1);
        when(broker.getConnection_closeWhenNoRoute()).thenReturn(false);
        when(broker.getId()).thenReturn(UUID.randomUUID());
        when(broker.getObjectFactory()).thenReturn(objectFactory);
        when(broker.getModel()).thenReturn(objectFactory.getModel());
        when(broker.getModelVersion()).thenReturn(BrokerModel.MODEL_VERSION);
        when(broker.getEventLogger()).thenReturn(eventLogger);
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.getParent()).thenReturn(systemConfig);
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

    public static QueueManagingVirtualHost<?> createVirtualHost(Map<String, Object> attributes)
    {
        Broker<?> broker = createBrokerMock(createAccessControlMock());
        return createVirtualHost(attributes, broker, false, createAccessControlMock());
    }

    private static QueueManagingVirtualHost<?> createVirtualHost(final Map<String, Object> attributes,
                                                        final Broker<?> broker, boolean defaultVHN, AccessControl accessControl)
    {
        ConfiguredObjectFactory objectFactory = broker.getObjectFactory();

        VirtualHostNode virtualHostNode = mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class, SYSTEM_PRINCIPAL, accessControl);
        String virtualHostNodeName = String.format("%s_%s", attributes.get(VirtualHostNode.NAME), "_node");
        when(virtualHostNode.getName()).thenReturn( virtualHostNodeName);
        when(virtualHostNode.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHostNode.isDefaultVirtualHostNode()).thenReturn(defaultVHN);

        when(virtualHostNode.getParent()).thenReturn(broker);

        Collection<VirtualHostNode<?>> nodes = broker.getVirtualHostNodes();
        nodes = new ArrayList<>(nodes != null ?  nodes : Collections.<VirtualHostNode<?>>emptyList());
        nodes.add(virtualHostNode);
        when(broker.getVirtualHostNodes()).thenReturn(nodes);

        DurableConfigurationStore dcs = mock(DurableConfigurationStore.class);
        when(virtualHostNode.getConfigurationStore()).thenReturn(dcs);
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

    public static QueueManagingVirtualHost<?> createVirtualHost(String name) throws Exception
    {
        return createVirtualHost(name, createBrokerMock(createAccessControlMock()), false, createAccessControlMock());
    }

    public static QueueManagingVirtualHost<?> createVirtualHost(String name, Broker<?> broker, boolean defaultVHN) throws Exception
    {
        return createVirtualHost(name, broker, defaultVHN, createAccessControlMock());
    }

    private static QueueManagingVirtualHost<?> createVirtualHost(String name, Broker<?> broker, boolean defaultVHN, AccessControl accessControl) throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        attributes.put(org.apache.qpid.server.model.VirtualHost.NAME, name);

        return createVirtualHost(attributes, broker, defaultVHN, accessControl);
    }

    public static AMQSessionModel<?,?> createSession(int channelId, AMQPConnection<?> connection)
    {
        @SuppressWarnings("rawtypes")
        AMQSessionModel session = mock(AMQSessionModel.class);
        when(session.getAMQPConnection()).thenReturn(connection);
        when(session.getChannelId()).thenReturn(channelId);
        return session;
    }

    public static AMQSessionModel<?,?> createSession(int channelId) throws Exception
    {
        AMQPConnection<?> session = createConnection();
        return createSession(channelId, session);
    }

    public static AMQSessionModel<?,?> createSession() throws Exception
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
        final QueueManagingVirtualHost virtualHost =  mockWithSystemPrincipal(QueueManagingVirtualHost.class, SYSTEM_PRINCIPAL);
        when(virtualHost.getName()).thenReturn(hostName);
        when(virtualHost.getEventLogger()).thenReturn(eventLogger);
        when(virtualHost.getDurableConfigurationStore()).thenReturn(mock(DurableConfigurationStore.class));
        final ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        when(virtualHost.getObjectFactory()).thenReturn(objectFactory);
        when(virtualHost.getModel()).thenReturn(objectFactory.getModel());
        when(virtualHost.getTaskExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHost.getChildExecutor()).thenReturn(TASK_EXECUTOR);
        when(virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        final Map<String,Object> attributes = new HashMap<String, Object>();
        attributes.put(org.apache.qpid.server.model.Exchange.ID, UUIDGenerator.generateExchangeUUID("amp.direct", virtualHost.getName()));
        attributes.put(org.apache.qpid.server.model.Exchange.NAME, "amq.direct");
        attributes.put(org.apache.qpid.server.model.Exchange.TYPE, "direct");
        attributes.put(org.apache.qpid.server.model.Exchange.DURABLE, durable);

        return Subject.doAs(SYSTEM_SUBJECT, new PrivilegedAction<Exchange<?>>()
        {
            @Override
            public Exchange<?> run()
            {

                return (Exchange<?>) objectFactory.create(Exchange.class, attributes, virtualHost);
            }
        });

    }

    public static Queue<?> createQueue(String queueName, VirtualHost<?> virtualHost)
    {
        Map<String,Object> attributes = new HashMap<String, Object>();
        attributes.put(Queue.ID, UUIDGenerator.generateRandomUUID());
        attributes.put(Queue.NAME, queueName);
        Queue<?> queue = virtualHost.createChild(Queue.class, attributes);
        return queue;
    }

    // The generated classes can't directly inherit from SystemPricipalSource / AccessControlSource as
    // these are package local, and package local visibility is prevented between classes loaded from different
    // class loaders.  Using these "public" interfaces gets around the problem.
    public interface TestableSystemPrincipalSource extends SystemPrincipalSource {}
    public interface TestableAccessControlSource extends AccessControlSource {}

    public static <X extends ConfiguredObject> X mockWithSystemPrincipal(Class<X> clazz, Principal principal)
    {
        synchronized (SYSTEM_PRINCIPAL_SOURCE_MOCKS)
        {
            Class<?> mockedClass = SYSTEM_PRINCIPAL_SOURCE_MOCKS.get(clazz);
            if(mockedClass == null)
            {
                mockedClass = generateTestableInterface(clazz, TestableSystemPrincipalSource.class);
                SYSTEM_PRINCIPAL_SOURCE_MOCKS.put(clazz, (Class<? extends X>) mockedClass);
            }
            X mock = mock((Class<? extends X>)mockedClass);
            when(((SystemPrincipalSource)mock).getSystemPrincipal()).thenReturn(principal);

            return mock;
        }
    }

    public static <X extends ConfiguredObject> X mockWithSystemPrincipalAndAccessControl(Class<X> clazz,
                                                                                         Principal principal,
                                                                                         AccessControl accessControl)
    {
        synchronized (SYSTEM_PRINCIPAL_AND_ACCESS_CONTROL_SOURCE_MOCKS)
        {
            Class<?> mockedClass = SYSTEM_PRINCIPAL_AND_ACCESS_CONTROL_SOURCE_MOCKS.get(clazz);
            if(mockedClass == null)
            {
                mockedClass = generateTestableInterface(clazz, TestableSystemPrincipalSource.class, TestableAccessControlSource.class);
                SYSTEM_PRINCIPAL_AND_ACCESS_CONTROL_SOURCE_MOCKS.put(clazz, (Class<? extends X>) mockedClass);
            }
            X mock = mock((Class<? extends X>)mockedClass);
            when(((SystemPrincipalSource)mock).getSystemPrincipal()).thenReturn(principal);
            when(((AccessControlSource)mock).getAccessControl()).thenReturn(accessControl);

            return mock;
        }
    }


    private static Class<?> generateTestableInterface(final Class<?>... interfaces)
    {
        final String fqcn = BrokerTestHelper.class.getPackage().getName() + ".Testable" + interfaces[0].getSimpleName() + "_" + GENERATED_INTERFACE_COUNTER.incrementAndGet();
        final byte[] classBytes = createSubClassByteCode(fqcn, interfaces);

        try
        {
            final ClassLoader classLoader = new TestableMockDelegatingClassloader(fqcn, classBytes);
            Class<?> clazz = classLoader.loadClass(fqcn);
            return clazz;
        }
        catch (ClassNotFoundException cnfe)
        {
            throw new IllegalArgumentException("Could not resolve dynamically generated class " + fqcn, cnfe);
        }
    }

    private static byte[] createSubClassByteCode(final String className, final Class<?>[] interfaces)
    {
        String[] ifnames = new String[interfaces.length];
        for(int i = 0; i<interfaces.length; i++)
        {
            ifnames[i] = interfaces[i].getName();
        }
        ClassGen classGen = new ClassGen(className,
                                         "java.lang.Object",
                                         "<generated>",
                                         ACC_PUBLIC | ACC_SUPER | ACC_INTERFACE,
                                         ifnames);


        JavaClass javaClass = classGen.getJavaClass();

        try(ByteArrayOutputStream out = new ByteArrayOutputStream())
        {
            javaClass.dump(out);
            return out.toByteArray();
        }
        catch (IOException ioex)
        {
            throw new IllegalStateException("Could not write to a ByteArrayOutputStream - should not happen", ioex);
        }
    }

    private static final class TestableMockDelegatingClassloader extends ClassLoader
    {
        private final String _className;
        private final Class<? extends ConfiguredObject> _clazz;

        private TestableMockDelegatingClassloader(String className, byte[] classBytes)
        {
            super(ConfiguredObject.class.getClassLoader());
            _className = className;
            _clazz = (Class<? extends ConfiguredObject>) defineClass(className, classBytes, 0, classBytes.length);
        }

        @Override
        protected Class<?> findClass(String fqcn) throws ClassNotFoundException
        {
            if (fqcn.equals(_className))
            {
                return _clazz;
            }
            else
            {
                return getParent().loadClass(fqcn);
            }
        }
    }

}
