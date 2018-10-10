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
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.*;
import org.apache.qpid.server.queue.QueueConsumer;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class LegacyAccessControlAdapterTest extends UnitTestBase
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

    @Before
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
        VirtualHost vh = mock(VirtualHost.class);
        when(vh.getCategoryClass()).thenReturn(VirtualHost.class);
        when(vh.getName()).thenReturn(TEST_VIRTUAL_HOST);
        when(vh.getAttribute(VirtualHost.NAME)).thenReturn(TEST_VIRTUAL_HOST);
        when(vh.getParent()).thenReturn(_virtualHostNode);
        when(vh.getModel()).thenReturn(BrokerModel.getInstance());
        return vh;
    }

    private VirtualHostNode getMockVirtualHostNode()
    {
        VirtualHostNode vhn = mock(VirtualHostNode.class);
        when(vhn.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(vhn.getName()).thenReturn("testVHN");
        when(vhn.getAttribute(ConfiguredObject.NAME)).thenReturn("testVHN");
        when(vhn.getParent()).thenReturn(_broker);
        when(vhn.getModel()).thenReturn(BrokerModel.getInstance());
        when(vhn.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        return vhn;
    }


    @Test
    public void testAuthoriseCreateAccessControlProvider()
    {
        AccessControlProvider accessControlProvider = mock(AccessControlProvider.class);
        when(accessControlProvider.getParent()).thenReturn(_broker);
        when(accessControlProvider.getName()).thenReturn("TEST");
        when(accessControlProvider.getCategoryClass()).thenReturn(AccessControlProvider.class);
        when(accessControlProvider.getAttribute(Queue.CREATED_BY)).thenReturn(TEST_USER);


        assertBrokerChildCreateAuthorization(accessControlProvider);
    }

    @Test
    public void testAuthoriseCreateConsumer()
    {
        Queue queue = mock(Queue.class);
        when(queue.getParent()).thenReturn(_virtualHost);
        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(true);
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.PERMANENT);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getAttribute(Queue.CREATED_BY)).thenReturn(TEST_USER);
        when(queue.getCategoryClass()).thenReturn(Queue.class);

        Session session = mock(Session.class);
        when(session.getCategoryClass()).thenReturn(Session.class);
        when(session.getAttribute(Session.NAME)).thenReturn("1");

        QueueConsumer consumer = mock(QueueConsumer.class);
        when(consumer.getAttribute(QueueConsumer.NAME)).thenReturn("1");
        when(consumer.getParent()).thenReturn(queue);
        when(consumer.getCategoryClass()).thenReturn(Consumer.class);

        ObjectProperties properties = new ObjectProperties();
        properties.put(ObjectProperties.Property.NAME, TEST_QUEUE);
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(ObjectProperties.Property.AUTO_DELETE, false);
        properties.put(ObjectProperties.Property.TEMPORARY, false);
        properties.put(ObjectProperties.Property.DURABLE, true);
        properties.put(ObjectProperties.Property.EXCLUSIVE, false);
        properties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);

        assertAuthorization(LegacyOperation.CREATE, consumer, LegacyOperation.CONSUME, ObjectType.QUEUE, properties,
                            Collections.emptyMap());
    }


    @Test
    public void testAuthoriseUpdatePort()
    {
        Port mock = mock(Port.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Port.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    public void testAuthoriseUpdateUser()
    {
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getName()).thenReturn("testAuthenticationProvider");
        User mock = mock(User.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(User.class);
        when(mock.getParent()).thenReturn(authenticationProvider);
        ObjectProperties properties = new ObjectProperties(mock.getName());
        properties.setAttributeNames(Collections.singleton(ConfiguredObject.DESCRIPTION));
        assertUpdateAuthorization(mock,
                                  LegacyOperation.UPDATE,
                                  ObjectType.USER,
                                  properties,
                                  Collections.singletonMap(ConfiguredObject.DESCRIPTION, "Test"));
    }


    @Test
    public void testAuthoriseDeleteVirtualHost()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();

        VirtualHost virtualHost = mock(VirtualHost.class);
        when(virtualHost.getName()).thenReturn("test");
        when(virtualHost.getAttribute(ConfiguredObject.NAME)).thenReturn("test");
        when(virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        when(virtualHost.getParent()).thenReturn(vhn);
        ObjectProperties properties = new ObjectProperties(virtualHost.getName());
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, (String)virtualHost.getAttribute(VirtualHost.NAME));
        assertDeleteAuthorization(virtualHost, LegacyOperation.DELETE, ObjectType.VIRTUALHOST, properties);
    }

    @Test
    public void testAuthoriseDeleteKeyStore()
    {
        KeyStore mock = mock(KeyStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(KeyStore.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    public void testAuthoriseDeleteTrustStore()
    {
        TrustStore mock = mock(TrustStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(TrustStore.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    public void testAuthoriseDeleteGroup()
    {
        GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        Group mock = mock(Group.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Group.class);
        when(mock.getParent()).thenReturn(groupProvider);
        ObjectProperties properties = new ObjectProperties(mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.GROUP, properties);
    }

    @Test
    public void testAuthoriseDeleteGroupMember()
    {
        Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getName()).thenReturn("testGroup");
        GroupMember mock = mock(GroupMember.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupMember.class);
        when(mock.getParent()).thenReturn(group);
        ObjectProperties properties = new ObjectProperties(mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.UPDATE, ObjectType.GROUP, properties);
    }

    @Test
    public void testAuthoriseDeleteUser()
    {
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getName()).thenReturn("testAuthenticationProvider");
        User mock = mock(User.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(User.class);
        when(mock.getParent()).thenReturn(authenticationProvider);
        ObjectProperties properties = new ObjectProperties(mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.USER, properties);
    }

    @Test
    public void testAuthoriseCreateExchange()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = createExpectedExchangeObjectProperties();

        Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(TEST_EXCHANGE);
        when(exchange.getAttribute(ConfiguredObject.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(exchange.getAttribute(Exchange.DURABLE)).thenReturn(false);
        when(exchange.getAttribute(Exchange.TYPE)).thenReturn(TEST_EXCHANGE_TYPE);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);
        when(exchange.getParent()).thenReturn(vh);

        assertCreateAuthorization(exchange, LegacyOperation.CREATE, ObjectType.EXCHANGE, expectedProperties);
    }

    @Test
    public void testAuthoriseCreateQueue()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = createExpectedQueueObjectProperties();

        Queue queue = mock(Queue.class);
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
    public void testAuthoriseDeleteQueue()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = createExpectedQueueObjectProperties();

        Queue queueObject = mock(Queue.class);
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
    public void testAuthoriseUpdateQueue()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = createExpectedQueueObjectProperties();
        expectedProperties.setAttributeNames(Collections.singleton(ConfiguredObject.DESCRIPTION));

        Queue queueObject = mock(Queue.class);
        when(queueObject.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queueObject.getAttribute(ConfiguredObject.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(queueObject.getAttribute(Queue.OWNER)).thenReturn(null);
        when(queueObject.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queueObject.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queueObject.getParent()).thenReturn(vh);
        when(queueObject.getCategoryClass()).thenReturn(Queue.class);

        assertUpdateAuthorization(queueObject, LegacyOperation.UPDATE, ObjectType.QUEUE, expectedProperties,
                                  Collections.singletonMap(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    public void testAuthoriseUpdateExchange()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = createExpectedExchangeObjectProperties();
        expectedProperties.setAttributeNames(Collections.singleton(ConfiguredObject.DESCRIPTION));

        Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(TEST_EXCHANGE);
        when(exchange.getAttribute(Exchange.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(exchange.getAttribute(Exchange.DURABLE)).thenReturn(false);
        when(exchange.getAttribute(Exchange.TYPE)).thenReturn(TEST_EXCHANGE_TYPE);
        when(exchange.getParent()).thenReturn(vh);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);

        assertUpdateAuthorization(exchange, LegacyOperation.UPDATE, ObjectType.EXCHANGE, expectedProperties,
                                  Collections.singletonMap(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    public void testAuthoriseDeleteExchange()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = createExpectedExchangeObjectProperties();

        Exchange exchange = mock(Exchange.class);
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
    public void testAuthoriseCreateVirtualHostNode()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();
        final ObjectProperties expectedProperties = new ObjectProperties("testVHN");
        expectedProperties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);
        assertCreateAuthorization(vhn, LegacyOperation.CREATE, ObjectType.VIRTUALHOSTNODE,
                                  expectedProperties);
    }

    @Test
    public void testAuthoriseCreatePort()
    {
        Port port = mock(Port.class);
        when(port.getParent()).thenReturn(_broker);
        when(port.getName()).thenReturn("TEST");
        when(port.getCategoryClass()).thenReturn(Port.class);
        when(port.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(port);
    }

    @Test
    public void testAuthoriseCreateAuthenticationProvider()
    {
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getParent()).thenReturn(_broker);
        when(authenticationProvider.getName()).thenReturn("TEST");
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildCreateAuthorization(authenticationProvider);
    }

    @Test
    public void testAuthoriseCreateGroupProvider()
    {
        GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getParent()).thenReturn(_broker);
        when(groupProvider.getName()).thenReturn("TEST");
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(groupProvider);
    }


    @Test
    public void testAuthoriseCreateKeyStore()
    {
        KeyStore keyStore = mock(KeyStore.class);
        when(keyStore.getParent()).thenReturn(_broker);
        when(keyStore.getName()).thenReturn("TEST");
        when(keyStore.getCategoryClass()).thenReturn(KeyStore.class);
        when(keyStore.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(keyStore);
    }

    @Test
    public void testAuthoriseCreateTrustStore()
    {
        TrustStore trustStore = mock(TrustStore.class);
        when(trustStore.getParent()).thenReturn(_broker);
        when(trustStore.getName()).thenReturn("TEST");
        when(trustStore.getCategoryClass()).thenReturn(TrustStore.class);
        when(trustStore.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(trustStore);
    }

    @Test
    public void testAuthoriseCreateGroup()
    {
        GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getAttribute(GroupProvider.NAME)).thenReturn("testGroupProvider");
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        when(groupProvider.getModel()).thenReturn(BrokerModel.getInstance());

        Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getParent()).thenReturn(groupProvider);
        when(group.getAttribute(Group.NAME)).thenReturn("test");
        when(group.getName()).thenReturn("test");

        assertCreateAuthorization(group, LegacyOperation.CREATE, ObjectType.GROUP, new ObjectProperties("test"));
    }

    @Test
    public void testAuthoriseCreateGroupMember()
    {
        Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getAttribute(Group.NAME)).thenReturn("testGroup");
        when(group.getName()).thenReturn("testGroup");
        when(group.getModel()).thenReturn(BrokerModel.getInstance());

        GroupMember groupMember = mock(GroupMember.class);
        when(groupMember.getCategoryClass()).thenReturn(GroupMember.class);
        when(groupMember.getParent()).thenReturn(group);
        when(groupMember.getAttribute(Group.NAME)).thenReturn("test");
        when(groupMember.getName()).thenReturn("test");

        assertCreateAuthorization(groupMember, LegacyOperation.UPDATE, ObjectType.GROUP, new ObjectProperties("test"));
    }

    @Test
    public void testAuthoriseCreateUser()
    {
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getAttribute(AuthenticationProvider.NAME)).thenReturn("testAuthenticationProvider");
        when(authenticationProvider.getModel()).thenReturn(BrokerModel.getInstance());

        User user = mock(User.class);
        when(user.getCategoryClass()).thenReturn(User.class);
        when(user.getAttribute(User.NAME)).thenReturn("test");
        when(user.getName()).thenReturn("test");
        when(user.getParent()).thenReturn(authenticationProvider);
        when(user.getModel()).thenReturn(BrokerModel.getInstance());

        assertCreateAuthorization(user, LegacyOperation.CREATE, ObjectType.USER, new ObjectProperties("test"));
    }

    @Test
    public void testAuthoriseCreateVirtualHost()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = new ObjectProperties(TEST_VIRTUAL_HOST);
        expectedProperties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        assertCreateAuthorization(vh, LegacyOperation.CREATE, ObjectType.VIRTUALHOST,
                                  expectedProperties);
    }

    @Test
    public void testAuthoriseUpdateVirtualHostNode()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();
        ObjectProperties expectedProperties = new ObjectProperties(vhn.getName());
        expectedProperties.setAttributeNames(Collections.singleton(ConfiguredObject.DESCRIPTION));
        expectedProperties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);
        assertUpdateAuthorization(vhn,
                                  LegacyOperation.UPDATE,
                                  ObjectType.VIRTUALHOSTNODE,
                                  expectedProperties,
                                  Collections.singletonMap(ConfiguredObject.DESCRIPTION, "Test"));
    }


    @Test
    public void testAuthoriseUpdateAuthenticationProvider()
    {
        AuthenticationProvider mock = mock(AuthenticationProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    public void testAuthoriseUpdateGroupProvider()
    {
        GroupProvider mock = mock(GroupProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    public void testAuthoriseUpdateAccessControlProvider()
    {
        AccessControlProvider mock = mock(AccessControlProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AccessControlProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    public void testAuthoriseUpdateKeyStore()
    {
        KeyStore mock = mock(KeyStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(KeyStore.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    public void testAuthoriseUpdateTrustStore()
    {
        TrustStore mock = mock(TrustStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(TrustStore.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildUpdateAuthorization(mock);
    }

    @Test
    public void testAuthoriseUpdateGroup()
    {
        GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        Group mock = mock(Group.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Group.class);
        when(mock.getParent()).thenReturn(groupProvider);
        ObjectProperties properties = new ObjectProperties(mock.getName());
        properties.setAttributeNames(Collections.singleton(ConfiguredObject.DESCRIPTION));
        assertUpdateAuthorization(mock, LegacyOperation.UPDATE, ObjectType.GROUP, properties, Collections.singletonMap(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    public void testAuthoriseUpdateGroupMember()
    {
        Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getName()).thenReturn("testGroup");
        GroupMember mock = mock(GroupMember.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupMember.class);
        when(mock.getParent()).thenReturn(group);
        ObjectProperties properties = new ObjectProperties(mock.getName());
        properties.setAttributeNames(Collections.singleton(ConfiguredObject.DESCRIPTION));
        assertUpdateAuthorization(mock,
                                  LegacyOperation.UPDATE,
                                  ObjectType.GROUP,
                                  properties,
                                  Collections.singletonMap(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    public void testAuthoriseUpdateVirtualHost()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();

        VirtualHost virtualHost = mock(VirtualHost.class);
        when(virtualHost.getName()).thenReturn("test");
        when(virtualHost.getAttribute(ConfiguredObject.NAME)).thenReturn("test");
        when(virtualHost.getCategoryClass()).thenReturn(VirtualHost.class);
        when(virtualHost.getParent()).thenReturn(vhn);
        ObjectProperties properties = new ObjectProperties(virtualHost.getName());
        properties.setAttributeNames(Collections.singleton(ConfiguredObject.DESCRIPTION));
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, virtualHost.getName());
        assertUpdateAuthorization(virtualHost, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, properties,
                                  Collections.singletonMap(ConfiguredObject.DESCRIPTION, "Test"));
    }

    @Test
    public void testAuthoriseDeleteVirtualHostNode()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();
        final ObjectProperties expectedProperties = new ObjectProperties(vhn.getName());
        expectedProperties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);
        assertDeleteAuthorization(vhn, LegacyOperation.DELETE, ObjectType.VIRTUALHOSTNODE,
                                  expectedProperties);
    }

    @Test
    public void testAuthoriseDeletePort()
    {
        Port mock = mock(Port.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Port.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    public void testAuthoriseDeleteAuthenticationProvider()
    {
        AuthenticationProvider mock = mock(AuthenticationProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    public void testAuthoriseDeleteGroupProvider()
    {
        GroupProvider mock = mock(GroupProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    public void testAuthoriseDeleteAccessControlProvider()
    {
        AccessControlProvider mock = mock(AccessControlProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AccessControlProvider.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    public void testAuthoriseBrokerLoggerOperations()
    {
        BrokerLogger mock = mock(BrokerLogger.class);
        when(mock.getName()).thenReturn("TEST");
        when(mock.getCategoryClass()).thenReturn(BrokerLogger.class);
        when(mock.getParent()).thenReturn(_broker);
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);

        assertBrokerChildCreateAuthorization(mock);

        when(mock.getName()).thenReturn("test");
        assertBrokerChildUpdateAuthorization(mock);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    public void testAuthoriseBrokerLogInclusionRuleOperations()
    {
        BrokerLogger bl = mock(BrokerLogger.class);
        when(bl.getName()).thenReturn("LOGGER");
        when(bl.getCategoryClass()).thenReturn(BrokerLogger.class);
        when(bl.getParent()).thenReturn(_broker);
        when(bl.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);


        BrokerLogInclusionRule mock = mock(BrokerLogInclusionRule.class);
        when(mock.getName()).thenReturn("TEST");
        when(mock.getCategoryClass()).thenReturn(BrokerLogInclusionRule.class);
        when(mock.getParent()).thenReturn(bl);
        when(mock.getModel()).thenReturn(BrokerModel.getInstance());
        when(mock.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);
        assertBrokerChildCreateAuthorization(mock);

        when(mock.getName()).thenReturn("test");
        assertBrokerChildUpdateAuthorization(mock);
        assertBrokerChildDeleteAuthorization(mock);
    }

    @Test
    public void testAuthoriseInvokeVirtualHostDescendantMethod()
    {
        String methodName = "clearQueue";
        Queue queue = mock(Queue.class);
        when(queue.getParent()).thenReturn(_virtualHost);
        when(queue.getModel()).thenReturn(_model);
        when(queue.getName()).thenReturn(TEST_QUEUE);
        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(queue.getAttribute(ConfiguredObject.CREATED_BY)).thenReturn(TEST_USER);


        ObjectProperties expectedProperties = new ObjectProperties();
        expectedProperties.put(ObjectProperties.Property.NAME, TEST_QUEUE);
        expectedProperties.put(ObjectProperties.Property.METHOD_NAME, methodName);
        expectedProperties.put(ObjectProperties.Property.VIRTUALHOST_NAME, _virtualHost.getName());
        expectedProperties.put(ObjectProperties.Property.COMPONENT, "VirtualHost.Queue");
        expectedProperties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);

        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                                      same(ObjectType.QUEUE),
                                      any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        Result result = _adapter.authoriseMethod(queue, methodName, Collections.emptyMap());
        assertEquals("Unexpected authorise result", Result.ALLOWED, result);

        verify(_accessControl).authorise(eq(LegacyOperation.INVOKE), eq(ObjectType.QUEUE), eq(expectedProperties));
        verify(_accessControl, never()).authorise(eq(LegacyOperation.PURGE), eq(ObjectType.QUEUE), any(ObjectProperties.class));
    }
    @Test
    public void testAuthoriseInvokeBrokerDescendantMethod()
    {
        String methodName = "getStatistics";
        VirtualHostNode<?> virtualHostNode = _virtualHostNode;

        ObjectProperties expectedProperties = new ObjectProperties();
        expectedProperties.put(ObjectProperties.Property.NAME, virtualHostNode.getName());
        expectedProperties.put(ObjectProperties.Property.METHOD_NAME, methodName);
        expectedProperties.put(ObjectProperties.Property.COMPONENT, "Broker.VirtualHostNode");
        expectedProperties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);

        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                                      same(ObjectType.VIRTUALHOSTNODE),
                                      any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        Result result = _adapter.authoriseMethod(virtualHostNode, methodName, Collections.emptyMap());
        assertEquals("Unexpected authorise result", Result.ALLOWED, result);

        verify(_accessControl).authorise(eq(LegacyOperation.INVOKE), eq(ObjectType.VIRTUALHOSTNODE), eq(expectedProperties));
    }

    @Test
    public void testAuthorisePurge()
    {
        Queue queue = mock(Queue.class);
        when(queue.getParent()).thenReturn(_virtualHost);
        when(queue.getModel()).thenReturn(_model);
        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);

        ObjectProperties properties = createExpectedQueueObjectProperties();

        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                                      any(ObjectType.class),
                                      any(ObjectProperties.class))).thenReturn(Result.DENIED);

        when(_accessControl.authorise(same(LegacyOperation.PURGE),
                                      same(ObjectType.QUEUE),
                                      any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        Result result = _adapter.authoriseMethod(queue, "clearQueue", Collections.emptyMap());
        assertEquals("Unexpected authorise result", Result.ALLOWED, result);

        verify(_accessControl).authorise(eq(LegacyOperation.PURGE), eq(ObjectType.QUEUE), eq(properties));

    }

    @Test
    public void testAuthoriseLogsAccessOnBroker()
    {
        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                                      same(ObjectType.BROKER),
                                      any(ObjectProperties.class))).thenReturn(Result.DENIED);
        when(_accessControl.authorise(same(LegacyOperation.ACCESS_LOGS),
                                      same(ObjectType.BROKER),
                                      any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        ConfiguredObject logger = mock(BrokerLogger.class);
        when(logger.getCategoryClass()).thenReturn(BrokerLogger.class);
        when(logger.getModel()).thenReturn(_model);
        when(logger.getParent()).thenReturn(_broker);

        Result result = _adapter.authoriseMethod(logger, "getFile", Collections.singletonMap("fileName", "qpid.log"));
        assertEquals("Unexpected authorise result", Result.ALLOWED, result);

        verify(_accessControl).authorise(ACCESS_LOGS, BROKER, ObjectProperties.EMPTY);
    }

    @Test
    public void testAuthoriseLogsAccessOnVirtualHost()
    {
        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                                      same(ObjectType.VIRTUALHOST),
                                      any(ObjectProperties.class))).thenReturn(Result.DENIED);
        when(_accessControl.authorise(same(LegacyOperation.ACCESS_LOGS),
                                      same(ObjectType.VIRTUALHOST),
                                      any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        ConfiguredObject logger = mock(VirtualHostLogger.class);
        when(logger.getCategoryClass()).thenReturn(VirtualHostLogger.class);
        when(logger.getParent()).thenReturn(_virtualHost);
        when(logger.getModel()).thenReturn(_model);

        Result result = _adapter.authoriseMethod(logger, "getFile", Collections.singletonMap("fileName", "qpid.log"));
        assertEquals("Unexpected authorise result", Result.ALLOWED, result);

        ObjectProperties expectedObjectProperties = new ObjectProperties(_virtualHost.getName());
        verify(_accessControl).authorise(ACCESS_LOGS, VIRTUALHOST, expectedObjectProperties);
    }

    @Test
    public void testAuthoriseMethod()
    {
        when(_accessControl.authorise(same(LegacyOperation.INVOKE),
                                      any(ObjectType.class),
                                      any(ObjectProperties.class))).thenReturn(Result.DENIED);

        when(_accessControl.authorise(same(LegacyOperation.UPDATE),
                                      same(ObjectType.METHOD),
                                      any(ObjectProperties.class))).thenReturn(Result.ALLOWED);

        ObjectProperties properties = new ObjectProperties("deleteMessages");
        properties.put(ObjectProperties.Property.COMPONENT, "VirtualHost.Queue");
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);

        Queue queue = mock(Queue.class);
        when(queue.getParent()).thenReturn(_virtualHost);
        when(queue.getVirtualHost()).thenReturn(_virtualHost);
        when(queue.getModel()).thenReturn(_model);


        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);

        Result result = _adapter.authoriseMethod(queue, "deleteMessages", Collections.emptyMap());
        assertEquals("Unexpected authorise result", Result.ALLOWED, result);

        verify(_accessControl).authorise(eq(LegacyOperation.UPDATE), eq(ObjectType.METHOD), eq(properties));
    }

    @Test
    public void testAccessManagement()
    {
        _adapter.authoriseAction(_broker, "manage", Collections.emptyMap());
        verify(_accessControl).authorise(LegacyOperation.ACCESS, ObjectType.MANAGEMENT, ObjectProperties.EMPTY);
    }

    @Test
    public void testAuthorisePublish()
    {
        String routingKey = "routingKey";
        String exchangeName = "exchangeName";
        ObjectProperties properties = new ObjectProperties(TEST_VIRTUAL_HOST, exchangeName, routingKey);
        properties.put(ObjectProperties.Property.DURABLE, true);
        properties.put(ObjectProperties.Property.AUTO_DELETE, false);
        properties.put(ObjectProperties.Property.TEMPORARY, false);

        Exchange exchange = mock(Exchange.class);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);
        when(exchange.getAddressSpace()).thenReturn(_virtualHost);
        when(exchange.getName()).thenReturn(exchangeName);
        when(exchange.getLifetimePolicy()).thenReturn(LifetimePolicy.PERMANENT);
        when(exchange.isDurable()).thenReturn(true);
        Map<String,Object> args = new HashMap<>();
        args.put("routingKey",routingKey);
        _adapter.authoriseAction(exchange, "publish", args);

        verify(_accessControl).authorise(eq(LegacyOperation.PUBLISH), eq(ObjectType.EXCHANGE), eq(properties));
    }

    @Test
    public void testAuthoriseCreateConnection()
    {
        ObjectProperties properties = new ObjectProperties();
        properties.put(ObjectProperties.Property.NAME, TEST_VIRTUAL_HOST);
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);

        _adapter.authoriseAction(_virtualHost, "connect", Collections.emptyMap());

        verify(_accessControl).authorise(eq(LegacyOperation.ACCESS), eq(ObjectType.VIRTUALHOST), eq(properties));
    }

    private ObjectProperties createExpectedQueueObjectProperties()
    {
        ObjectProperties properties = new ObjectProperties();
        properties.put(ObjectProperties.Property.NAME, TEST_QUEUE);
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(ObjectProperties.Property.AUTO_DELETE, true);
        properties.put(ObjectProperties.Property.TEMPORARY, true);
        properties.put(ObjectProperties.Property.DURABLE, false);
        properties.put(ObjectProperties.Property.EXCLUSIVE, false);
        return properties;
    }

    private ObjectProperties createExpectedExchangeObjectProperties()
    {
        ObjectProperties properties = new ObjectProperties();
        properties.put(ObjectProperties.Property.NAME, TEST_EXCHANGE);
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(ObjectProperties.Property.AUTO_DELETE, true);
        properties.put(ObjectProperties.Property.TEMPORARY, true);
        properties.put(ObjectProperties.Property.DURABLE, false);
        properties.put(ObjectProperties.Property.TYPE, TEST_EXCHANGE_TYPE);
        return properties;
    }

    private void assertBrokerChildCreateAuthorization(ConfiguredObject object)
    {
        String description = String.format("%s %s '%s'",
                                           LegacyOperation.CREATE.name().toLowerCase(),
                                           object.getCategoryClass().getSimpleName().toLowerCase(),
                                           "TEST");
        ObjectProperties properties = new OperationLoggingDetails(description);
        properties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);
        assertCreateAuthorization(object, LegacyOperation.CONFIGURE, ObjectType.BROKER, properties);
    }


    private void assertCreateAuthorization(ConfiguredObject<?> configuredObject,
                                           LegacyOperation aclOperation,
                                           ObjectType aclObjectType,
                                           ObjectProperties expectedProperties)
    {
        assertAuthorization(LegacyOperation.CREATE,
                            configuredObject,
                            aclOperation,
                            aclObjectType,
                            expectedProperties,
                            Collections.emptyMap());
    }


    private void assertBrokerChildUpdateAuthorization(ConfiguredObject configuredObject)
    {
        String description = String.format("%s %s '%s'",
                                           LegacyOperation.UPDATE.name().toLowerCase(),
                                           configuredObject.getCategoryClass().getSimpleName().toLowerCase(),
                                           configuredObject.getName());
        ObjectProperties properties = new OperationLoggingDetails(description);
        properties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);
        properties.setAttributeNames(Collections.singleton(ConfiguredObject.DESCRIPTION));

        assertUpdateAuthorization(configuredObject, LegacyOperation.CONFIGURE, ObjectType.BROKER,
                                  properties, Collections.singletonMap(ConfiguredObject.DESCRIPTION, "Test"));
    }

    private void assertUpdateAuthorization(ConfiguredObject<?> configuredObject,
                                           LegacyOperation aclOperation,
                                           ObjectType aclObjectType,
                                           ObjectProperties expectedProperties, final Map<String, Object> attributes)
    {
        assertAuthorization(LegacyOperation.UPDATE, configuredObject, aclOperation, aclObjectType, expectedProperties,
                            attributes);
    }

    private void assertBrokerChildDeleteAuthorization(ConfiguredObject configuredObject)
    {
        String description = String.format("%s %s '%s'",
                                           LegacyOperation.DELETE.name().toLowerCase(),
                                           configuredObject.getCategoryClass().getSimpleName().toLowerCase(),
                                           configuredObject.getName());
        ObjectProperties properties = new OperationLoggingDetails(description);
        properties.put(ObjectProperties.Property.CREATED_BY, TEST_USER);
        assertDeleteAuthorization(configuredObject, LegacyOperation.CONFIGURE, ObjectType.BROKER,
                                  properties);
    }


    private void assertDeleteAuthorization(ConfiguredObject<?> configuredObject,
                                           LegacyOperation aclOperation,
                                           ObjectType aclObjectType,
                                           ObjectProperties expectedProperties)
    {
        assertAuthorization(LegacyOperation.DELETE, configuredObject, aclOperation, aclObjectType, expectedProperties,
                            Collections.emptyMap());
    }

    private void assertAuthorization(LegacyOperation operation,
                                     ConfiguredObject<?> configuredObject,
                                     LegacyOperation aclOperation,
                                     ObjectType aclObjectType,
                                     ObjectProperties expectedProperties, final Map<String, Object> attributes)
    {
        _adapter.authorise(operation, configuredObject, attributes);
        verify(_accessControl).authorise(eq(aclOperation), eq(aclObjectType), eq(expectedProperties));
    }
}
