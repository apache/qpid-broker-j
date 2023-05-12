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
package org.apache.qpid.server.security.access.config;

import static org.apache.qpid.server.security.access.config.LegacyOperation.ACCESS_LOGS;
import static org.apache.qpid.server.security.access.config.ObjectType.BROKER;
import static org.apache.qpid.server.security.access.config.ObjectType.VIRTUALHOST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.*;
import org.apache.qpid.server.queue.QueueConsumer;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class LegacyAccessControlAdapterTest extends UnitTestBase
{
    private static final String TEST_EXCHANGE_TYPE = "testExchangeType";
    private static final String TEST_VIRTUAL_HOST = "testVirtualHost";
    private static final String TEST_EXCHANGE = "testExchange";
    private static final String TEST_QUEUE = "testQueue";
    private static final String TEST_USER = "user";

    private LegacyAccessControl _accessControl;
    private QueueManagingVirtualHost<?> _virtualHost;
    private Broker _broker;
    private VirtualHostNode<?> _virtualHostNode;
    private LegacyAccessControlAdapter _adapter;
    private Model _model;

    @BeforeEach
    public void setUp() throws Exception
    {
        _accessControl = mock(LegacyAccessControl.class);
        _model = BrokerModel.getInstance();
        _broker = mock(Broker.class);
        _virtualHostNode = getMockVirtualHostNode();
        _virtualHost = mock(QueueManagingVirtualHost.class);
        when(_virtualHost.getParent()).thenReturn(_broker);

        when(_virtualHost.getName()).thenReturn(TEST_VIRTUAL_HOST);
        when(_virtualHost.getAttribute(VirtualHost.NAME)).thenReturn(TEST_VIRTUAL_HOST);
        when(_virtualHost.getAttribute(VirtualHost.CREATED_BY)).thenReturn(TEST_USER);
        when(_virtualHost.getModel()).thenReturn(_model);
        doReturn(_virtualHostNode).when(_virtualHost).getParent();
        doReturn(VirtualHost.class).when(_virtualHost).getCategoryClass();

        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getName()).thenReturn("My Broker");
        when(_broker.getAttribute(Broker.NAME)).thenReturn("My Broker");
        when(_broker.getModel()).thenReturn(BrokerModel.getInstance());

        _adapter = new LegacyAccessControlAdapter(_accessControl, BrokerModel.getInstance());
    }

    private VirtualHost getMockVirtualHost()
    {
        final VirtualHost vh = mock(VirtualHost.class);
        when(vh.getCategoryClass()).thenReturn(VirtualHost.class);
        when(vh.getName()).thenReturn(TEST_VIRTUAL_HOST);
        when(vh.getAttribute(VirtualHost.NAME)).thenReturn(TEST_VIRTUAL_HOST);
        when(vh.getParent()).thenReturn(_virtualHostNode);
        when(vh.getModel()).thenReturn(BrokerModel.getInstance());
        return vh;
    }

    private VirtualHostNode getMockVirtualHostNode()
    {
        final VirtualHostNode vhn = mock(VirtualHostNode.class);
        when(vhn.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(vhn.getName()).thenReturn("testVHN");
        when(vhn.getAttribute(ConfiguredObject.NAME)).thenReturn("testVHN");
        when(vhn.getParent()).thenReturn(_broker);
        when(vhn.getModel()).thenReturn(BrokerModel.getInstance());
        when(vhn.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        return vhn;
    }

    @Test
    void authoriseCreateAccessControlProvider()
    {
        final AccessControlProvider accessControlProvider = mock(AccessControlProvider.class);
        when(accessControlProvider.getParent()).thenReturn(_broker);
        when(accessControlProvider.getName()).thenReturn("TEST");
        when(accessControlProvider.getCategoryClass()).thenReturn(AccessControlProvider.class);
        when(accessControlProvider.getAttribute(Queue.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(accessControlProvider);
    }

    @Test
    void authoriseCreateConsumer()
    {
        final Queue queue = mock(Queue.class);
        when(queue.getParent()).thenReturn(_virtualHost);
        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(true);
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.PERMANENT);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getAttribute(Queue.CREATED_BY)).thenReturn(TEST_USER);
        when(queue.getCategoryClass()).thenReturn(Queue.class);

        final Session session = mock(Session.class);
        when(session.getCategoryClass()).thenReturn(Session.class);
        when(session.getAttribute(Session.NAME)).thenReturn("1");

        final QueueConsumer consumer = mock(QueueConsumer.class);
        when(consumer.getAttribute(QueueConsumer.NAME)).thenReturn("1");
        when(consumer.getParent()).thenReturn(queue);
        when(consumer.getCategoryClass()).thenReturn(Consumer.class);

        final ObjectProperties properties = new ObjectProperties();
        properties.put(Property.NAME, TEST_QUEUE);
        properties.put(Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(Property.AUTO_DELETE, false);
        properties.put(Property.TEMPORARY, false);
        properties.put(Property.DURABLE, true);
        properties.put(Property.EXCLUSIVE, false);
        properties.put(Property.CREATED_BY, TEST_USER);

        assertAuthorization(LegacyOperation.CREATE, consumer, LegacyOperation.CONSUME, ObjectType.QUEUE, properties,
                Map.of());
    }

    @Test
    void authoriseUpdatePort()
    {
        final Port mock = mock(Port.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Port.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    void authoriseUpdateUser()
    {
        final AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getName()).thenReturn("testAuthenticationProvider");
        final User mock = mock(User.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(User.class);
        when(mock.getParent()).thenReturn(authenticationProvider);
        final ObjectProperties properties = new ObjectProperties(mock.getName());
        properties.addAttributeNames(Set.of(ConfiguredObject.DESCRIPTION));
        assertUpdateAuthorization(mock,
                LegacyOperation.UPDATE,
                ObjectType.USER,
                properties,
                Map.of(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    void authoriseDeleteVirtualHost()
    {
        final VirtualHostNode vhn = getMockVirtualHostNode();

        final VirtualHost virtualHost = mock(VirtualHost.class);
        when(virtualHost.getName()).thenReturn("test");
        when(virtualHost.getAttribute(ConfiguredObject.NAME)).thenReturn("test");
        when(virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        when(virtualHost.getParent()).thenReturn(vhn);
        final ObjectProperties properties = new ObjectProperties(virtualHost.getName());
        properties.put(Property.VIRTUALHOST_NAME, (String)virtualHost.getAttribute(VirtualHost.NAME));
        assertDeleteAuthorization(virtualHost, LegacyOperation.DELETE, ObjectType.VIRTUALHOST, properties);
    }

    @Test
    void authoriseDeleteKeyStore()
    {
        final KeyStore mock = mock(KeyStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(KeyStore.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    void authoriseDeleteTrustStore()
    {
        final TrustStore mock = mock(TrustStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(TrustStore.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    void authoriseDeleteGroup()
    {
        final GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        final Group mock = mock(Group.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Group.class);
        when(mock.getParent()).thenReturn(groupProvider);
        final ObjectProperties properties = new ObjectProperties(mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.GROUP, properties);
    }

    @Test
    void authoriseDeleteGroupMember()
    {
        final Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getName()).thenReturn("testGroup");
        final GroupMember mock = mock(GroupMember.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupMember.class);
        when(mock.getParent()).thenReturn(group);
        final ObjectProperties properties = new ObjectProperties(mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.UPDATE, ObjectType.GROUP, properties);
    }

    @Test
    void authoriseDeleteUser()
    {
        final AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getName()).thenReturn("testAuthenticationProvider");
        final User mock = mock(User.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(User.class);
        when(mock.getParent()).thenReturn(authenticationProvider);
        final ObjectProperties properties = new ObjectProperties(mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.USER, properties);
    }

    @Test
    void authoriseCreateExchange()
    {
        final VirtualHost vh = getMockVirtualHost();
        final ObjectProperties expectedProperties = createExpectedExchangeObjectProperties();

        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(TEST_EXCHANGE);
        when(exchange.getAttribute(ConfiguredObject.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(exchange.getAttribute(Exchange.DURABLE)).thenReturn(false);
        when(exchange.getAttribute(Exchange.TYPE)).thenReturn(TEST_EXCHANGE_TYPE);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);
        when(exchange.getParent()).thenReturn(vh);

        assertCreateAuthorization(exchange, LegacyOperation.CREATE, ObjectType.EXCHANGE, expectedProperties);
    }

    @Test
    void authoriseCreateQueue()
    {
        final VirtualHost vh = getMockVirtualHost();
        final ObjectProperties expectedProperties = createExpectedQueueObjectProperties();

        final Queue queue = mock(Queue.class);
        when(queue.getName()).thenReturn(TEST_QUEUE);
        when(queue.getAttribute(ConfiguredObject.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getAttribute(ConfiguredObject.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(queue.getAttribute(Queue.OWNER)).thenReturn(null);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queue.getAttribute(Queue.ALTERNATE_BINDING)).thenReturn(null);
        when(queue.getCategoryClass()).thenReturn(Queue.class);
        when(queue.getParent()).thenReturn(vh);

        assertCreateAuthorization(queue, LegacyOperation.CREATE, ObjectType.QUEUE, expectedProperties);
    }

    @Test
    void authoriseDeleteQueue()
    {
        final VirtualHost vh = getMockVirtualHost();
        final ObjectProperties expectedProperties = createExpectedQueueObjectProperties();

        final Queue queueObject = mock(Queue.class);
        when(queueObject.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queueObject.getAttribute(ConfiguredObject.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(queueObject.getAttribute(Queue.OWNER)).thenReturn(null);
        when(queueObject.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queueObject.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queueObject.getParent()).thenReturn(vh);
        when(queueObject.getCategoryClass()).thenReturn(Queue.class);

        assertDeleteAuthorization(queueObject, LegacyOperation.DELETE, ObjectType.QUEUE, expectedProperties);
    }

    @Test
    void authoriseUpdateQueue()
    {
        final VirtualHost vh = getMockVirtualHost();
        final ObjectProperties expectedProperties = createExpectedQueueObjectProperties();
        expectedProperties.addAttributeNames(Set.of(ConfiguredObject.DESCRIPTION));

        final Queue queueObject = mock(Queue.class);
        when(queueObject.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queueObject.getAttribute(ConfiguredObject.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(queueObject.getAttribute(Queue.OWNER)).thenReturn(null);
        when(queueObject.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queueObject.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queueObject.getParent()).thenReturn(vh);
        when(queueObject.getCategoryClass()).thenReturn(Queue.class);

        assertUpdateAuthorization(queueObject, LegacyOperation.UPDATE, ObjectType.QUEUE, expectedProperties,
                Map.of(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    void authoriseUpdateExchange()
    {
        final VirtualHost vh = getMockVirtualHost();
        final ObjectProperties expectedProperties = createExpectedExchangeObjectProperties();
        expectedProperties.addAttributeNames(Set.of(ConfiguredObject.DESCRIPTION));

        final Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(TEST_EXCHANGE);
        when(exchange.getAttribute(Exchange.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(exchange.getAttribute(Exchange.DURABLE)).thenReturn(false);
        when(exchange.getAttribute(Exchange.TYPE)).thenReturn(TEST_EXCHANGE_TYPE);
        when(exchange.getParent()).thenReturn(vh);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);

        assertUpdateAuthorization(exchange, LegacyOperation.UPDATE, ObjectType.EXCHANGE, expectedProperties,
                Map.of(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    void authoriseDeleteExchange()
    {
        final VirtualHost vh = getMockVirtualHost();
        final ObjectProperties expectedProperties = createExpectedExchangeObjectProperties();

        final Exchange exchange = mock(Exchange.class);
        when(exchange.getAttribute(Exchange.NAME)).thenReturn(TEST_EXCHANGE);
        when(exchange.getName()).thenReturn(TEST_EXCHANGE);
        when(exchange.getAttribute(Exchange.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(exchange.getAttribute(Exchange.DURABLE)).thenReturn(false);
        when(exchange.getAttribute(Exchange.TYPE)).thenReturn(TEST_EXCHANGE_TYPE);
        when(exchange.getParent()).thenReturn(vh);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);

        assertDeleteAuthorization(exchange, LegacyOperation.DELETE, ObjectType.EXCHANGE, expectedProperties);
    }

    @Test
    void authoriseCreateVirtualHostNode()
    {
        final VirtualHostNode vhn = getMockVirtualHostNode();
        final ObjectProperties expectedProperties = new ObjectProperties("testVHN");
        expectedProperties.put(Property.CREATED_BY, TEST_USER);
        assertCreateAuthorization(vhn, LegacyOperation.CREATE, ObjectType.VIRTUALHOSTNODE, expectedProperties);
    }

    @Test
    void authoriseCreatePort()
    {
        final Port port = mock(Port.class);
        when(port.getParent()).thenReturn(_broker);
        when(port.getName()).thenReturn("TEST");
        when(port.getCategoryClass()).thenReturn(Port.class);
        when(port.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(port);
    }

    @Test
    void authoriseCreateAuthenticationProvider()
    {
        final AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getParent()).thenReturn(_broker);
        when(authenticationProvider.getName()).thenReturn("TEST");
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildCreateAuthorization(authenticationProvider);
    }

    @Test
    void authoriseCreateGroupProvider()
    {
        final GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getParent()).thenReturn(_broker);
        when(groupProvider.getName()).thenReturn("TEST");
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(groupProvider);
    }

    @Test
    void authoriseCreateKeyStore()
    {
        final KeyStore keyStore = mock(KeyStore.class);
        when(keyStore.getParent()).thenReturn(_broker);
        when(keyStore.getName()).thenReturn("TEST");
        when(keyStore.getCategoryClass()).thenReturn(KeyStore.class);
        when(keyStore.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(keyStore);
    }

    @Test
    void authoriseCreateTrustStore()
    {
        final TrustStore trustStore = mock(TrustStore.class);
        when(trustStore.getParent()).thenReturn(_broker);
        when(trustStore.getName()).thenReturn("TEST");
        when(trustStore.getCategoryClass()).thenReturn(TrustStore.class);
        when(trustStore.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(trustStore);
    }

    @Test
    void authoriseCreateGroup()
    {
        final GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getAttribute(GroupProvider.NAME)).thenReturn("testGroupProvider");
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        when(groupProvider.getModel()).thenReturn(BrokerModel.getInstance());

        final Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getParent()).thenReturn(groupProvider);
        when(group.getAttribute(Group.NAME)).thenReturn("test");
        when(group.getName()).thenReturn("test");

        assertCreateAuthorization(group, LegacyOperation.CREATE, ObjectType.GROUP, new ObjectProperties("test"));
    }

    @Test
    void authoriseCreateGroupMember()
    {
        final Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getAttribute(Group.NAME)).thenReturn("testGroup");
        when(group.getName()).thenReturn("testGroup");
        when(group.getModel()).thenReturn(BrokerModel.getInstance());

        final GroupMember groupMember = mock(GroupMember.class);
        when(groupMember.getCategoryClass()).thenReturn(GroupMember.class);
        when(groupMember.getParent()).thenReturn(group);
        when(groupMember.getAttribute(Group.NAME)).thenReturn("test");
        when(groupMember.getName()).thenReturn("test");

        assertCreateAuthorization(groupMember, LegacyOperation.UPDATE, ObjectType.GROUP, new ObjectProperties("test"));
    }

    @Test
    void authoriseCreateUser()
    {
        final AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getAttribute(AuthenticationProvider.NAME)).thenReturn("testAuthenticationProvider");
        when(authenticationProvider.getModel()).thenReturn(BrokerModel.getInstance());

        final User user = mock(User.class);
        when(user.getCategoryClass()).thenReturn(User.class);
        when(user.getAttribute(User.NAME)).thenReturn("test");
        when(user.getName()).thenReturn("test");
        when(user.getParent()).thenReturn(authenticationProvider);
        when(user.getModel()).thenReturn(BrokerModel.getInstance());

        assertCreateAuthorization(user, LegacyOperation.CREATE, ObjectType.USER, new ObjectProperties("test"));
    }

    @Test
    void authoriseCreateVirtualHost()
    {
        final VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = new ObjectProperties(TEST_VIRTUAL_HOST);
        expectedProperties.put(Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        assertCreateAuthorization(vh, LegacyOperation.CREATE, ObjectType.VIRTUALHOST, expectedProperties);
    }

    @Test
    void authoriseUpdateVirtualHostNode()
    {
        final VirtualHostNode vhn = getMockVirtualHostNode();
        final ObjectProperties expectedProperties = new ObjectProperties(vhn.getName());
        expectedProperties.addAttributeNames(Set.of(ConfiguredObject.DESCRIPTION));
        expectedProperties.put(Property.CREATED_BY, TEST_USER);
        assertUpdateAuthorization(vhn,
                LegacyOperation.UPDATE,
                ObjectType.VIRTUALHOSTNODE,
                expectedProperties,
                Map.of(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    void authoriseUpdateAuthenticationProvider()
    {
        final AuthenticationProvider mock = mock(AuthenticationProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    void authoriseUpdateGroupProvider()
    {
        final GroupProvider mock = mock(GroupProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    void authoriseUpdateAccessControlProvider()
    {
        final AccessControlProvider mock = mock(AccessControlProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AccessControlProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    void authoriseUpdateKeyStore()
    {
        final KeyStore mock = mock(KeyStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(KeyStore.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    void authoriseUpdateTrustStore()
    {
        final TrustStore mock = mock(TrustStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(TrustStore.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    void authoriseUpdateGroup()
    {
        final GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        final Group mock = mock(Group.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Group.class);
        when(mock.getParent()).thenReturn(groupProvider);
        final ObjectProperties properties = new ObjectProperties(mock.getName());
        properties.addAttributeNames(Set.of(ConfiguredObject.DESCRIPTION));
        assertUpdateAuthorization(mock, LegacyOperation.UPDATE, ObjectType.GROUP, properties, Map.of(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    void authoriseUpdateGroupMember()
    {
        final Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getName()).thenReturn("testGroup");
        final GroupMember mock = mock(GroupMember.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupMember.class);
        when(mock.getParent()).thenReturn(group);
        final ObjectProperties properties = new ObjectProperties(mock.getName());
        properties.addAttributeNames(Set.of(ConfiguredObject.DESCRIPTION));
        assertUpdateAuthorization(mock,
                LegacyOperation.UPDATE,
                ObjectType.GROUP,
                properties,
                Map.of(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    void authoriseUpdateVirtualHost()
    {
        final VirtualHostNode vhn = getMockVirtualHostNode();

        final VirtualHost virtualHost = mock(VirtualHost.class);
        when(virtualHost.getName()).thenReturn("test");
        when(virtualHost.getAttribute(ConfiguredObject.NAME)).thenReturn("test");
        when(virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        when(virtualHost.getParent()).thenReturn(vhn);
        final ObjectProperties properties = new ObjectProperties(virtualHost.getName());
        properties.addAttributeNames(Set.of(ConfiguredObject.DESCRIPTION));
        properties.put(Property.VIRTUALHOST_NAME, virtualHost.getName());
        assertUpdateAuthorization(virtualHost, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, properties,
                Map.of(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    void authoriseDeleteVirtualHostNode()
    {
        final VirtualHostNode vhn = getMockVirtualHostNode();
        final ObjectProperties expectedProperties = new ObjectProperties(vhn.getName());
        expectedProperties.put(Property.CREATED_BY, TEST_USER);
        assertDeleteAuthorization(vhn, LegacyOperation.DELETE, ObjectType.VIRTUALHOSTNODE, expectedProperties);
    }

    @Test
    void authoriseDeletePort()
    {
        final Port mock = mock(Port.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Port.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    void authoriseDeleteAuthenticationProvider()
    {
        final AuthenticationProvider mock = mock(AuthenticationProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    void authoriseDeleteGroupProvider()
    {
        final GroupProvider mock = mock(GroupProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    void authoriseDeleteAccessControlProvider()
    {
        final AccessControlProvider mock = mock(AccessControlProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AccessControlProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    void authoriseBrokerLoggerOperations_Create()
    {
        assertBrokerChildCreateAuthorization(newBrokerLogger());
    }

    @Test
    void authoriseBrokerLoggerOperations_Update()
    {
        assertBrokerChildUpdateAuthorization(newBrokerLogger());
    }

    @Test
    void authoriseBrokerLoggerOperations_Delete()
    {
        assertBrokerChildDeleteAuthorization(newBrokerLogger());
    }

    private BrokerLogger newBrokerLogger()
    {
        final BrokerLogger mock = mock(BrokerLogger.class);
        when(mock.getName()).thenReturn("TEST");
        when(mock.getCategoryClass()).thenReturn(BrokerLogger.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        return mock;
    }

    @Test
    void authoriseBrokerLogInclusionRuleOperations_Create()
    {
        assertBrokerChildCreateAuthorization(newInclusionRule());
    }

    @Test
    void authoriseBrokerLogInclusionRuleOperations_Update()
    {
        assertBrokerChildUpdateAuthorization(newInclusionRule());
    }

    @Test
    void authoriseBrokerLogInclusionRuleOperations_Delete()
    {
        assertBrokerChildDeleteAuthorization(newInclusionRule());
    }

    private BrokerLogInclusionRule newInclusionRule()
    {
        final BrokerLogger bl = mock(BrokerLogger.class);
        when(bl.getName()).thenReturn("LOGGER");
        when(bl.getCategoryClass()).thenReturn(BrokerLogger.class);
        when(bl.getParent()).thenReturn(_broker);
        when(bl.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        final BrokerLogInclusionRule mock = mock(BrokerLogInclusionRule.class);
        when(mock.getName()).thenReturn("TEST");
        when(mock.getCategoryClass()).thenReturn(BrokerLogInclusionRule.class);
        when(mock.getParent()).thenReturn(bl);
        when(mock.getModel()).thenReturn(BrokerModel.getInstance());
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        return mock;
    }

    @Test
    void authoriseInvokeVirtualHostDescendantMethod()
    {
        final String methodName = "clearQueue";
        final Queue queue = mock(Queue.class);
        when(queue.getParent()).thenReturn(_virtualHost);
        when(queue.getModel()).thenReturn(_model);
        when(queue.getName()).thenReturn(TEST_QUEUE);
        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(queue.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        final ObjectProperties expectedProperties = new ObjectProperties();
        expectedProperties.put(Property.NAME, TEST_QUEUE);
        expectedProperties.put(Property.METHOD_NAME, methodName);
        expectedProperties.put(Property.VIRTUALHOST_NAME, _virtualHost.getName());
        expectedProperties.put(Property.COMPONENT, "VirtualHost.Queue");
        expectedProperties.put(Property.CREATED_BY, TEST_USER);

        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                same(ObjectType.QUEUE),
                any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        final Result result = _adapter.authoriseMethod(queue, methodName, Collections.emptyMap());
        assertEquals(Result.ALLOWED, result, "Unexpected authorise result");

        verify(_accessControl).authorise(LegacyOperation.INVOKE, ObjectType.QUEUE, expectedProperties);
        verify(_accessControl, never()).authorise(eq(LegacyOperation.PURGE), eq(ObjectType.QUEUE), any(ObjectProperties.class));
    }

    @Test
    void authoriseInvokeBrokerDescendantMethod()
    {
        final String methodName = "getStatistics";
        final VirtualHostNode<?> virtualHostNode = _virtualHostNode;

        final ObjectProperties expectedProperties = new ObjectProperties();
        expectedProperties.put(Property.NAME, virtualHostNode.getName());
        expectedProperties.put(Property.METHOD_NAME, methodName);
        expectedProperties.put(Property.COMPONENT, "Broker.VirtualHostNode");
        expectedProperties.put(Property.CREATED_BY, TEST_USER);

        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                same(ObjectType.VIRTUALHOSTNODE),
                any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        final Result result = _adapter.authoriseMethod(virtualHostNode, methodName, Collections.emptyMap());
        assertEquals(Result.ALLOWED, result, "Unexpected authorise result");

        verify(_accessControl).authorise(LegacyOperation.INVOKE, ObjectType.VIRTUALHOSTNODE, expectedProperties);
    }

    @Test
    void authorisePurge()
    {
        final Queue queue = mock(Queue.class);
        when(queue.getParent()).thenReturn(_virtualHost);
        when(queue.getModel()).thenReturn(_model);
        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);

        final ObjectProperties properties = createExpectedQueueObjectProperties();

        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                any(ObjectType.class),
                any(ObjectProperties.class))).thenReturn(Result.DENIED);

        when(_accessControl.authorise(same(LegacyOperation.PURGE),
                same(ObjectType.QUEUE),
                any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        final Result result = _adapter.authoriseMethod(queue, "clearQueue", Collections.emptyMap());
        assertEquals(Result.ALLOWED, result, "Unexpected authorise result");

        verify(_accessControl).authorise(LegacyOperation.PURGE, ObjectType.QUEUE, properties);

    }

    @Test
    void authoriseLogsAccessOnBroker()
    {
        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                same(ObjectType.BROKER),
                any(ObjectProperties.class))).thenReturn(Result.DENIED);
        when(_accessControl.authorise(same(LegacyOperation.ACCESS_LOGS),
                same(ObjectType.BROKER),
                any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        final ConfiguredObject logger = mock(BrokerLogger.class);
        when(logger.getCategoryClass()).thenReturn(BrokerLogger.class);
        when(logger.getModel()).thenReturn(_model);
        when(logger.getParent()).thenReturn(_broker);

        final Result result = _adapter.authoriseMethod(logger, "getFile", Map.of("fileName", "qpid.log"));
        assertEquals(Result.ALLOWED, result, "Unexpected authorise result");

        verify(_accessControl).authorise(ACCESS_LOGS, BROKER, new ObjectProperties());
    }

    @Test
    void authoriseLogsAccessOnVirtualHost()
    {
        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                same(ObjectType.VIRTUALHOST),
                any(ObjectProperties.class))).thenReturn(Result.DENIED);
        when(_accessControl.authorise(same(LegacyOperation.ACCESS_LOGS),
                same(ObjectType.VIRTUALHOST),
                any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        final ConfiguredObject logger = mock(VirtualHostLogger.class);
        when(logger.getCategoryClass()).thenReturn(VirtualHostLogger.class);
        when(logger.getParent()).thenReturn(_virtualHost);
        when(logger.getModel()).thenReturn(_model);

        final Result result = _adapter.authoriseMethod(logger, "getFile", Map.of("fileName", "qpid.log"));
        assertEquals(Result.ALLOWED, result, "Unexpected authorise result");

        final ObjectProperties expectedObjectProperties = new ObjectProperties(_virtualHost.getName());
        verify(_accessControl).authorise(ACCESS_LOGS, VIRTUALHOST, expectedObjectProperties);
    }

    @Test
    void authoriseMethod()
    {
        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                any(ObjectType.class),
                any(ObjectProperties.class))).thenReturn(Result.DENIED);

        when(_accessControl.authorise(same(LegacyOperation.UPDATE),
                same(ObjectType.METHOD),
                any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        final ObjectProperties properties = new ObjectProperties("deleteMessages");
        properties.put(Property.COMPONENT, "VirtualHost.Queue");
        properties.put(Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);

        final Queue queue = mock(Queue.class);
        when(queue.getParent()).thenReturn(_virtualHost);
        when(queue.getVirtualHost()).thenReturn(_virtualHost);
        when(queue.getModel()).thenReturn(_model);

        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);

        final Result result = _adapter.authoriseMethod(queue, "deleteMessages", Map.of());
        assertEquals(Result.ALLOWED, result, "Unexpected authorise result");

        verify(_accessControl).authorise(LegacyOperation.UPDATE, ObjectType.METHOD, properties);
    }

    @Test
    void accessManagement()
    {
        _adapter.authoriseAction(_broker, "manage", Collections.emptyMap());
        verify(_accessControl).authorise(LegacyOperation.ACCESS, ObjectType.MANAGEMENT, new ObjectProperties());
    }

    @Test
    void authorisePublish()
    {
        final String routingKey = "routingKey";
        final String exchangeName = "exchangeName";
        final ObjectProperties properties = new ObjectProperties(exchangeName);
        properties.put(Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(Property.ROUTING_KEY, routingKey);
        properties.put(Property.DURABLE, true);
        properties.put(Property.AUTO_DELETE, false);
        properties.put(Property.TEMPORARY, false);

        final Exchange exchange = mock(Exchange.class);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);
        when(exchange.getAddressSpace()).thenReturn(_virtualHost);
        when(exchange.getName()).thenReturn(exchangeName);
        when(exchange.getLifetimePolicy()).thenReturn(LifetimePolicy.PERMANENT);
        when(exchange.isDurable()).thenReturn(true);
        final Map<String, Object> args = Map.of("routingKey",routingKey);
        _adapter.authoriseAction(exchange, "publish", args);

        verify(_accessControl).authorise(LegacyOperation.PUBLISH, ObjectType.EXCHANGE, properties);
    }

    @Test
    void authoriseCreateConnection()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.put(Property.NAME, TEST_VIRTUAL_HOST);
        properties.put(Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(Property.CREATED_BY, TEST_USER);

        _adapter.authoriseAction(_virtualHost, "connect", Collections.emptyMap());

        verify(_accessControl).authorise(LegacyOperation.ACCESS, ObjectType.VIRTUALHOST, properties);
    }

    private ObjectProperties createExpectedQueueObjectProperties()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.put(Property.NAME, TEST_QUEUE);
        properties.put(Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(Property.AUTO_DELETE, true);
        properties.put(Property.TEMPORARY, true);
        properties.put(Property.DURABLE, false);
        properties.put(Property.EXCLUSIVE, false);
        return properties;
    }

    private ObjectProperties createExpectedExchangeObjectProperties()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.put(Property.NAME, TEST_EXCHANGE);
        properties.put(Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(Property.AUTO_DELETE, true);
        properties.put(Property.TEMPORARY, true);
        properties.put(Property.DURABLE, false);
        properties.put(Property.TYPE, TEST_EXCHANGE_TYPE);
        return properties;
    }

    private void assertBrokerChildCreateAuthorization(final ConfiguredObject object)
    {
        final String description = String.format("%s %s '%s'",
                LegacyOperation.CREATE.name().toLowerCase(),
                object.getCategoryClass().getSimpleName().toLowerCase(),
                "TEST");
        final ObjectProperties properties = new ObjectProperties().withDescription(description);
        properties.put(Property.CREATED_BY, TEST_USER);
        assertCreateAuthorization(object, LegacyOperation.CONFIGURE, ObjectType.BROKER, properties);
    }

    private void assertCreateAuthorization(final ConfiguredObject<?> configuredObject,
                                           final LegacyOperation aclOperation,
                                           final ObjectType aclObjectType,
                                           final ObjectProperties expectedProperties)
    {
        assertAuthorization(LegacyOperation.CREATE,
                configuredObject,
                aclOperation,
                aclObjectType,
                expectedProperties,
                Map.of());
    }

    private void assertBrokerChildUpdateAuthorization(final ConfiguredObject configuredObject)
    {
        final String description = String.format("%s %s '%s'",
                LegacyOperation.UPDATE.name().toLowerCase(),
                configuredObject.getCategoryClass().getSimpleName().toLowerCase(),
                configuredObject.getName());
        final ObjectProperties properties = new ObjectProperties().withDescription(description);
        properties.put(Property.CREATED_BY, TEST_USER);
        properties.addAttributeNames(Set.of(ConfiguredObject.DESCRIPTION));

        assertUpdateAuthorization(configuredObject, LegacyOperation.CONFIGURE, ObjectType.BROKER,
            properties, Map.of(ConfiguredObject.DESCRIPTION, "Test"));
    }

    private void assertUpdateAuthorization(final ConfiguredObject<?> configuredObject,
                                           final LegacyOperation aclOperation,
                                           final ObjectType aclObjectType,
                                           final ObjectProperties expectedProperties,
                                           final Map<String, Object> attributes)
    {
        assertAuthorization(LegacyOperation.UPDATE, configuredObject, aclOperation, aclObjectType, expectedProperties,
                attributes);
    }

    private void assertBrokerChildDeleteAuthorization(final ConfiguredObject configuredObject)
    {
        final String description = String.format("%s %s '%s'",
                LegacyOperation.DELETE.name().toLowerCase(),
                configuredObject.getCategoryClass().getSimpleName().toLowerCase(),
                configuredObject.getName());
        final ObjectProperties properties = new ObjectProperties().withDescription(description);
        properties.put(Property.CREATED_BY, TEST_USER);
        assertDeleteAuthorization(configuredObject, LegacyOperation.CONFIGURE, ObjectType.BROKER,
                properties);
    }


    private void assertDeleteAuthorization(final ConfiguredObject<?> configuredObject,
                                           final LegacyOperation aclOperation,
                                           final ObjectType aclObjectType,
                                           final ObjectProperties expectedProperties)
    {
        assertAuthorization(LegacyOperation.DELETE, configuredObject, aclOperation, aclObjectType, expectedProperties,
                Map.of());
    }

    private void assertAuthorization(final LegacyOperation operation,
                                     final ConfiguredObject<?> configuredObject,
                                     final LegacyOperation aclOperation,
                                     final ObjectType aclObjectType,
                                     final ObjectProperties expectedProperties,
                                     final Map<String, Object> attributes)
    {
        _adapter.authorise(operation, configuredObject, attributes);
        verify(_accessControl).authorise(aclOperation, aclObjectType, expectedProperties);
    }
}
