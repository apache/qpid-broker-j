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

import static org.apache.qpid.server.security.access.config.ObjectType.BROKER;
import static org.apache.qpid.server.security.access.config.ObjectType.VIRTUALHOST;
import static org.apache.qpid.server.security.access.config.LegacyOperation.ACCESS_LOGS;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.model.*;
import org.apache.qpid.server.queue.QueueConsumer;
import org.apache.qpid.test.utils.QpidTestCase;

public class LegacyAccessControlAdapterTest extends QpidTestCase
{
    private static final String TEST_EXCHANGE_TYPE = "testExchangeType";
    private static final String TEST_VIRTUAL_HOST = "testVirtualHost";
    private static final String TEST_EXCHANGE = "testExchange";
    private static final String TEST_QUEUE = "testQueue";

    private LegacyAccessControl _accessControl;
    private VirtualHost<?> _virtualHost;
    private Broker _broker;
    private VirtualHostNode<?> _virtualHostNode;
    private LegacyAccessControlAdapter _adapter;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _accessControl = mock(LegacyAccessControl.class);
        _virtualHost = mock(VirtualHost.class);


        when(_virtualHost.getName()).thenReturn(TEST_VIRTUAL_HOST);
        when(_virtualHost.getAttribute(VirtualHost.NAME)).thenReturn(TEST_VIRTUAL_HOST);
        when(_virtualHost.getModel()).thenReturn(BrokerModel.getInstance());
        doReturn(VirtualHost.class).when(_virtualHost).getCategoryClass();

        _broker = mock(Broker.class);
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getName()).thenReturn("My Broker");
        when(_broker.getAttribute(Broker.NAME)).thenReturn("My Broker");
        when(_broker.getModel()).thenReturn(BrokerModel.getInstance());

        _virtualHostNode = getMockVirtualHostNode();

        _adapter = new LegacyAccessControlAdapter(_accessControl, BrokerModel.getInstance());
    }

    private VirtualHost getMockVirtualHost()
    {
        VirtualHost vh = mock(VirtualHost.class);
        when(vh.getCategoryClass()).thenReturn(VirtualHost.class);
        when(vh.getName()).thenReturn(TEST_VIRTUAL_HOST);
        when(vh.getAttribute(VirtualHost.NAME)).thenReturn(TEST_VIRTUAL_HOST);
        when(vh.getParent(VirtualHostNode.class)).thenReturn(_virtualHostNode);
        when(vh.getModel()).thenReturn(BrokerModel.getInstance());
        return vh;
    }

    private VirtualHostNode getMockVirtualHostNode()
    {
        VirtualHostNode vhn = mock(VirtualHostNode.class);
        when(vhn.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(vhn.getName()).thenReturn("testVHN");
        when(vhn.getAttribute(ConfiguredObject.NAME)).thenReturn("testVHN");
        when(vhn.getParent(Broker.class)).thenReturn(_broker);
        when(vhn.getModel()).thenReturn(BrokerModel.getInstance());
        return vhn;
    }


    public void testAuthoriseCreateAccessControlProvider()
    {
        AccessControlProvider accessControlProvider = mock(AccessControlProvider.class);
        when(accessControlProvider.getParent(Broker.class)).thenReturn(_broker);
        when(accessControlProvider.getName()).thenReturn("TEST");
        when(accessControlProvider.getCategoryClass()).thenReturn(AccessControlProvider.class);

        assertBrokerChildCreateAuthorization(accessControlProvider);
    }

    public void testAuthoriseCreateConsumer()
    {
        Queue queue = mock(Queue.class);
        when(queue.getParent(VirtualHost.class)).thenReturn(_virtualHost);
        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(true);
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.PERMANENT);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);

        Session session = mock(Session.class);
        when(session.getCategoryClass()).thenReturn(Session.class);
        when(session.getAttribute(Session.NAME)).thenReturn("1");

        QueueConsumer consumer = mock(QueueConsumer.class);
        when(consumer.getAttribute(QueueConsumer.NAME)).thenReturn("1");
        when(consumer.getParent(Queue.class)).thenReturn(queue);
        when(consumer.getCategoryClass()).thenReturn(Consumer.class);

        ObjectProperties properties = new ObjectProperties();
        properties.put(ObjectProperties.Property.NAME, TEST_QUEUE);
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);
        properties.put(ObjectProperties.Property.AUTO_DELETE, false);
        properties.put(ObjectProperties.Property.TEMPORARY, false);
        properties.put(ObjectProperties.Property.DURABLE, true);
        properties.put(ObjectProperties.Property.EXCLUSIVE, false);

        assertAuthorization(LegacyOperation.CREATE, consumer, LegacyOperation.CONSUME, ObjectType.QUEUE, properties, queue, session);
    }


    public void testAuthoriseUpdatePort()
    {
        Port mock = mock(Port.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Port.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildUpdateAuthorization(mock);
    }

    public void testAuthoriseUpdateUser()
    {
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getName()).thenReturn("testAuthenticationProvider");
        User mock = mock(User.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(User.class);
        when(mock.getParent(AuthenticationProvider.class)).thenReturn(authenticationProvider);
        ObjectProperties properties = new ObjectProperties((String)mock.getName());
        assertUpdateAuthorization(mock, LegacyOperation.UPDATE, ObjectType.USER, properties, authenticationProvider);
    }


    public void testAuthoriseDeleteVirtualHost()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();

        VirtualHost mock = mock(VirtualHost.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getAttribute(ConfiguredObject.NAME)).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(VirtualHost.class);
        when(mock.getParent(VirtualHostNode.class)).thenReturn(vhn);
        ObjectProperties properties = new ObjectProperties((String)mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.VIRTUALHOST, properties, vhn);
    }

    public void testAuthoriseDeleteKeyStore()
    {
        KeyStore mock = mock(KeyStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(KeyStore.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildDeleteAuthorization(mock);
    }

    public void testAuthoriseDeleteTrustStore()
    {
        TrustStore mock = mock(TrustStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(TrustStore.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildDeleteAuthorization(mock);
    }

    public void testAuthoriseDeleteGroup()
    {
        GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        Group mock = mock(Group.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Group.class);
        when(mock.getParent(GroupProvider.class)).thenReturn(groupProvider);
        ObjectProperties properties = new ObjectProperties((String)mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.GROUP, properties, groupProvider);
    }

    public void testAuthoriseDeleteGroupMember()
    {
        Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getName()).thenReturn("testGroup");
        GroupMember mock = mock(GroupMember.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupMember.class);
        when(mock.getParent(Group.class)).thenReturn(group);
        ObjectProperties properties = new ObjectProperties((String)mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.UPDATE, ObjectType.GROUP, properties, group);
    }

    public void testAuthoriseDeleteUser()
    {
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(authenticationProvider.getName()).thenReturn("testAuthenticationProvider");
        User mock = mock(User.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(User.class);
        when(mock.getParent(AuthenticationProvider.class)).thenReturn(authenticationProvider);
        ObjectProperties properties = new ObjectProperties((String)mock.getName());
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.USER, properties, authenticationProvider);
    }

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
        when(exchange.getParent(VirtualHost.class)).thenReturn(vh);

        assertCreateAuthorization(exchange, LegacyOperation.CREATE, ObjectType.EXCHANGE, expectedProperties, vh);
    }

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
        when(queue.getAttribute(Queue.ALTERNATE_EXCHANGE)).thenReturn(null);
        when(queue.getCategoryClass()).thenReturn(Queue.class);
        when(queue.getParent(VirtualHost.class)).thenReturn(vh);

        assertCreateAuthorization(queue, LegacyOperation.CREATE, ObjectType.QUEUE, expectedProperties, vh);
    }

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
        when(queueObject.getParent(VirtualHost.class)).thenReturn(vh);
        when(queueObject.getCategoryClass()).thenReturn(Queue.class);

        assertDeleteAuthorization(queueObject, LegacyOperation.DELETE, ObjectType.QUEUE, expectedProperties, vh);
    }

    public void testAuthoriseUpdateQueue()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = createExpectedQueueObjectProperties();

        Queue queueObject = mock(Queue.class);
        when(queueObject.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queueObject.getAttribute(ConfiguredObject.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(queueObject.getAttribute(Queue.OWNER)).thenReturn(null);
        when(queueObject.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queueObject.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queueObject.getParent(VirtualHost.class)).thenReturn(vh);
        when(queueObject.getCategoryClass()).thenReturn(Queue.class);

        assertUpdateAuthorization(queueObject, LegacyOperation.UPDATE, ObjectType.QUEUE, expectedProperties, vh);
    }

    public void testAuthoriseUpdateExchange()
    {
        VirtualHost vh = getMockVirtualHost();
        ObjectProperties expectedProperties = createExpectedExchangeObjectProperties();

        Exchange exchange = mock(Exchange.class);
        when(exchange.getName()).thenReturn(TEST_EXCHANGE);
        when(exchange.getAttribute(Exchange.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        when(exchange.getAttribute(Exchange.DURABLE)).thenReturn(false);
        when(exchange.getAttribute(Exchange.TYPE)).thenReturn(TEST_EXCHANGE_TYPE);
        when(exchange.getParent(VirtualHost.class)).thenReturn(vh);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);

        assertUpdateAuthorization(exchange, LegacyOperation.UPDATE, ObjectType.EXCHANGE, expectedProperties, vh);
    }

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
        when(exchange.getParent(VirtualHost.class)).thenReturn(vh);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);

        assertDeleteAuthorization(exchange, LegacyOperation.DELETE, ObjectType.EXCHANGE, expectedProperties, vh);
    }

    public void testAuthoriseCreateVirtualHostNode()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();
        assertCreateAuthorization(vhn, LegacyOperation.CREATE, ObjectType.VIRTUALHOSTNODE, new ObjectProperties("testVHN"), _broker);
    }

    public void testAuthoriseCreatePort()
    {
        Port port = mock(Port.class);
        when(port.getParent(Broker.class)).thenReturn(_broker);
        when(port.getName()).thenReturn("TEST");
        when(port.getCategoryClass()).thenReturn(Port.class);

        assertBrokerChildCreateAuthorization(port);
    }

    public void testAuthoriseCreateAuthenticationProvider()
    {
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getParent(Broker.class)).thenReturn(_broker);
        when(authenticationProvider.getName()).thenReturn("TEST");
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);

        assertBrokerChildCreateAuthorization(authenticationProvider);
    }

    public void testAuthoriseCreateGroupProvider()
    {
        GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getParent(Broker.class)).thenReturn(_broker);
        when(groupProvider.getName()).thenReturn("TEST");
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);

        assertBrokerChildCreateAuthorization(groupProvider);
    }


    public void testAuthoriseCreateKeyStore()
    {
        KeyStore keyStore = mock(KeyStore.class);
        when(keyStore.getParent(Broker.class)).thenReturn(_broker);
        when(keyStore.getName()).thenReturn("TEST");
        when(keyStore.getCategoryClass()).thenReturn(KeyStore.class);

        assertBrokerChildCreateAuthorization(keyStore);
    }

    public void testAuthoriseCreateTrustStore()
    {
        TrustStore trustStore = mock(TrustStore.class);
        when(trustStore.getParent(Broker.class)).thenReturn(_broker);
        when(trustStore.getName()).thenReturn("TEST");
        when(trustStore.getCategoryClass()).thenReturn(TrustStore.class);

        assertBrokerChildCreateAuthorization(trustStore);
    }

    public void testAuthoriseCreateGroup()
    {
        GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getAttribute(GroupProvider.NAME)).thenReturn("testGroupProvider");
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        when(groupProvider.getModel()).thenReturn(BrokerModel.getInstance());

        Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getParent(GroupProvider.class)).thenReturn(groupProvider);
        when(group.getAttribute(Group.NAME)).thenReturn("test");
        when(group.getName()).thenReturn("test");

        assertCreateAuthorization(group, LegacyOperation.CREATE, ObjectType.GROUP, new ObjectProperties("test"), groupProvider);
    }

    public void testAuthoriseCreateGroupMember()
    {
        Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getAttribute(Group.NAME)).thenReturn("testGroup");
        when(group.getName()).thenReturn("testGroup");
        when(group.getModel()).thenReturn(BrokerModel.getInstance());

        GroupMember groupMember = mock(GroupMember.class);
        when(groupMember.getCategoryClass()).thenReturn(GroupMember.class);
        when(groupMember.getParent(Group.class)).thenReturn(group);
        when(groupMember.getAttribute(Group.NAME)).thenReturn("test");
        when(groupMember.getName()).thenReturn("test");

        assertCreateAuthorization(groupMember, LegacyOperation.UPDATE, ObjectType.GROUP, new ObjectProperties("test"), group);
    }

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
        when(user.getParent(AuthenticationProvider.class)).thenReturn(authenticationProvider);
        when(user.getModel()).thenReturn(BrokerModel.getInstance());

        assertCreateAuthorization(user, LegacyOperation.CREATE, ObjectType.USER, new ObjectProperties("test"), authenticationProvider);
    }

    public void testAuthoriseCreateVirtualHost()
    {
        VirtualHost vh = getMockVirtualHost();
        assertCreateAuthorization(vh, LegacyOperation.CREATE, ObjectType.VIRTUALHOST, new ObjectProperties(TEST_VIRTUAL_HOST), _virtualHostNode);
    }

    public void testAuthoriseUpdateVirtualHostNode()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();
        assertUpdateAuthorization(vhn, LegacyOperation.UPDATE, ObjectType.VIRTUALHOSTNODE, new ObjectProperties(vhn.getName()), vhn);
    }


    public void testAuthoriseUpdateAuthenticationProvider()
    {
        AuthenticationProvider mock = mock(AuthenticationProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildUpdateAuthorization(mock);
    }

    public void testAuthoriseUpdateGroupProvider()
    {
        GroupProvider mock = mock(GroupProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupProvider.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildUpdateAuthorization(mock);
    }

    public void testAuthoriseUpdateAccessControlProvider()
    {
        AccessControlProvider mock = mock(AccessControlProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AccessControlProvider.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildUpdateAuthorization(mock);
    }

    public void testAuthoriseUpdateKeyStore()
    {
        KeyStore mock = mock(KeyStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(KeyStore.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildUpdateAuthorization(mock);
    }

    public void testAuthoriseUpdateTrustStore()
    {
        TrustStore mock = mock(TrustStore.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(TrustStore.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildUpdateAuthorization(mock);
    }

    public void testAuthoriseUpdateGroup()
    {
        GroupProvider groupProvider = mock(GroupProvider.class);
        when(groupProvider.getCategoryClass()).thenReturn(GroupProvider.class);
        when(groupProvider.getName()).thenReturn("testGroupProvider");
        Group mock = mock(Group.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Group.class);
        when(mock.getParent(GroupProvider.class)).thenReturn(groupProvider);
        ObjectProperties properties = new ObjectProperties((String)mock.getName());
        assertUpdateAuthorization(mock, LegacyOperation.UPDATE, ObjectType.GROUP, properties, groupProvider);
    }

    public void testAuthoriseUpdateGroupMember()
    {
        Group group = mock(Group.class);
        when(group.getCategoryClass()).thenReturn(Group.class);
        when(group.getName()).thenReturn("testGroup");
        GroupMember mock = mock(GroupMember.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupMember.class);
        when(mock.getParent(Group.class)).thenReturn(group);
        ObjectProperties properties = new ObjectProperties((String)mock.getName());
        assertUpdateAuthorization(mock, LegacyOperation.UPDATE, ObjectType.GROUP, properties, group);
    }

    public void testAuthoriseUpdateVirtualHost()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();

        VirtualHost mock = mock(VirtualHost.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getAttribute(ConfiguredObject.NAME)).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(VirtualHost.class);
        when(mock.getParent(VirtualHostNode.class)).thenReturn(vhn);
        ObjectProperties properties = new ObjectProperties((String)mock.getName());
        assertUpdateAuthorization(mock, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, properties, vhn);
    }

    public void testAuthoriseDeleteVirtualHostNode()
    {
        VirtualHostNode vhn = getMockVirtualHostNode();
        assertDeleteAuthorization(vhn, LegacyOperation.DELETE, ObjectType.VIRTUALHOSTNODE, new ObjectProperties(vhn.getName()), vhn);
    }

    public void testAuthoriseDeletePort()
    {
        Port mock = mock(Port.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(Port.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildDeleteAuthorization(mock);
    }

    public void testAuthoriseDeleteAuthenticationProvider()
    {
        AuthenticationProvider mock = mock(AuthenticationProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildDeleteAuthorization(mock);
    }

    public void testAuthoriseDeleteGroupProvider()
    {
        GroupProvider mock = mock(GroupProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(GroupProvider.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildDeleteAuthorization(mock);
    }

    public void testAuthoriseDeleteAccessControlProvider()
    {
        AccessControlProvider mock = mock(AccessControlProvider.class);
        when(mock.getName()).thenReturn("test");
        when(mock.getCategoryClass()).thenReturn(AccessControlProvider.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildDeleteAuthorization(mock);
    }

    public void testAuthoriseBrokerLoggerOperations()
    {
        BrokerLogger mock = mock(BrokerLogger.class);
        when(mock.getName()).thenReturn("TEST");
        when(mock.getCategoryClass()).thenReturn(BrokerLogger.class);
        when(mock.getParent(Broker.class)).thenReturn(_broker);
        assertBrokerChildCreateAuthorization(mock);

        when(mock.getName()).thenReturn("test");
        assertBrokerChildUpdateAuthorization(mock);
        assertBrokerChildDeleteAuthorization(mock);
    }

    public void testAuthoriseBrokerLogInclusionRuleOperations()
    {
        BrokerLogger bl = mock(BrokerLogger.class);
        when(bl.getName()).thenReturn("LOGGER");
        when(bl.getCategoryClass()).thenReturn(BrokerLogger.class);
        when(bl.getParent(Broker.class)).thenReturn(_broker);

        BrokerLogInclusionRule mock = mock(BrokerLogInclusionRule.class);
        when(mock.getName()).thenReturn("TEST");
        when(mock.getCategoryClass()).thenReturn(BrokerLogInclusionRule.class);
        when(mock.getParent(BrokerLogger.class)).thenReturn(bl);
        when(mock.getModel()).thenReturn(BrokerModel.getInstance());
        assertBrokerChildCreateAuthorization(mock, bl);

        when(mock.getName()).thenReturn("test");
        assertBrokerChildUpdateAuthorization(mock, bl);
        assertBrokerChildDeleteAuthorization(mock, bl);
    }


    public void testAuthoriseVirtualHostLoggerOperations()
    {
        ObjectProperties properties = new ObjectProperties(TEST_VIRTUAL_HOST);

        VirtualHostLogger<?> mock = mock(VirtualHostLogger.class);
        when(mock.getName()).thenReturn("TEST");
        doReturn(VirtualHostLogger.class).when(mock).getCategoryClass();
        when(mock.getParent(VirtualHost.class)).thenReturn(_virtualHost);
        when(mock.getModel()).thenReturn(BrokerModel.getInstance());

        assertCreateAuthorization(mock, LegacyOperation.CREATE, ObjectType.VIRTUALHOST, properties, _virtualHost);

        when(mock.getName()).thenReturn("test");

        assertUpdateAuthorization(mock, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, properties, _virtualHost);
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.VIRTUALHOST, properties, _virtualHost);
    }

    public void testAuthoriseVirtualHostLogInclusionRuleOperations()
    {
        ObjectProperties properties = new ObjectProperties(TEST_VIRTUAL_HOST);

        VirtualHostLogger<?> vhl = mock(VirtualHostLogger.class);
        when(vhl.getName()).thenReturn("LOGGER");
        doReturn(VirtualHostLogger.class).when(vhl).getCategoryClass();
        when(vhl.getParent(VirtualHost.class)).thenReturn(_virtualHost);
        when(vhl.getModel()).thenReturn(BrokerModel.getInstance());

        VirtualHostLogInclusionRule<?> mock = mock(VirtualHostLogInclusionRule.class);
        when(mock.getName()).thenReturn("TEST");
        doReturn(VirtualHostLogInclusionRule.class).when(mock).getCategoryClass();
        when(mock.getParent(VirtualHostLogger.class)).thenReturn(vhl);
        when(mock.getModel()).thenReturn(BrokerModel.getInstance());

        assertCreateAuthorization(mock, LegacyOperation.CREATE, ObjectType.VIRTUALHOST, properties, vhl);

        when(mock.getName()).thenReturn("test");

        assertUpdateAuthorization(mock, LegacyOperation.UPDATE, ObjectType.VIRTUALHOST, properties, vhl);
        assertDeleteAuthorization(mock, LegacyOperation.DELETE, ObjectType.VIRTUALHOST, properties, vhl);
    }

    public void testAuthorisePurge()
    {
        Queue queue = mock(Queue.class);
        when(queue.getParent(VirtualHost.class)).thenReturn(_virtualHost);
        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);
        when(queue.getAttribute(Queue.DURABLE)).thenReturn(false);
        when(queue.getAttribute(Queue.EXCLUSIVE)).thenReturn(ExclusivityPolicy.NONE);
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);

        ObjectProperties properties = createExpectedQueueObjectProperties();

        _adapter.authoriseMethod(queue, "clearQueue", Collections.<String,Object>emptyMap());
        verify(_accessControl).authorise(eq(LegacyOperation.PURGE), eq(ObjectType.QUEUE), eq(properties));

    }


    public void testAuthoriseLogsAccessOnBroker()
    {

        ConfiguredObject logger = mock(BrokerLogger.class);
        when(logger.getCategoryClass()).thenReturn(BrokerLogger.class);
        _adapter.authoriseMethod(logger, "getFile", Collections.singletonMap("fileName", (Object)"qpid.log"));

        verify(_accessControl).authorise(ACCESS_LOGS, BROKER, ObjectProperties.EMPTY);

    }

    public void testAuthoriseLogsAccessOnVirtualHost()
    {
        ConfiguredObject logger = mock(VirtualHostLogger.class);
        when(logger.getCategoryClass()).thenReturn(VirtualHostLogger.class);
        when(logger.getParent(eq(VirtualHost.class))).thenReturn(_virtualHost);

        _adapter.authoriseMethod(logger, "getFile", Collections.singletonMap("fileName", (Object)"qpid.log"));
        ObjectProperties expectedObjectProperties = new ObjectProperties(_virtualHost.getName());
        verify(_accessControl).authorise(ACCESS_LOGS, VIRTUALHOST, expectedObjectProperties);


    }

    public void testAuthoriseMethod()
    {
        ObjectProperties properties = new ObjectProperties("deleteMessages");
        properties.put(ObjectProperties.Property.COMPONENT, "VirtualHost.Queue");
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);

        Queue queue = mock(Queue.class);
        when(queue.getParent(VirtualHost.class)).thenReturn(_virtualHost);
        when(queue.getVirtualHost()).thenReturn(_virtualHost);

        when(queue.getAttribute(Queue.NAME)).thenReturn(TEST_QUEUE);
        when(queue.getCategoryClass()).thenReturn(Queue.class);


        _adapter.authoriseMethod(queue, "deleteMessages", Collections.<String,Object>emptyMap());
        verify(_accessControl).authorise(eq(LegacyOperation.UPDATE), eq(ObjectType.METHOD), eq(properties));

    }

    public void testAuthoriseUserOperation()
    {
        AuthenticationProvider authenticationProvider = mock(AuthenticationProvider.class);
        when(authenticationProvider.getParent(Broker.class)).thenReturn(_broker);
        when(authenticationProvider.getAttribute(Queue.NAME)).thenReturn("test");
        when(authenticationProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);


        ObjectProperties properties = new ObjectProperties("testUser");

        _adapter.authoriseMethod(authenticationProvider, "getPreferences", Collections.<String,Object>singletonMap("userId", "testUser"));
        verify(_accessControl).authorise(eq(LegacyOperation.UPDATE), eq(ObjectType.USER), eq(properties));

    }


    public void testAccessManagement()
    {
        _adapter.authoriseAction(_broker, "manage", Collections.<String,Object>emptyMap());
        verify(_accessControl).authorise(LegacyOperation.ACCESS, ObjectType.MANAGEMENT, ObjectProperties.EMPTY);

    }

    public void testAuthorisePublish()
    {
        String routingKey = "routingKey";
        String exchangeName = "exchangeName";
        boolean immediate = true;
        ObjectProperties properties = new ObjectProperties(TEST_VIRTUAL_HOST, exchangeName, routingKey, immediate);

        Exchange exchange = mock(Exchange.class);
        when(exchange.getCategoryClass()).thenReturn(Exchange.class);
        when(exchange.getAddressSpace()).thenReturn(_virtualHost);
        when(exchange.getName()).thenReturn(exchangeName);
        Map<String,Object> args = new HashMap<>();
        args.put("routingKey",routingKey);
        args.put("immediate", true);
        _adapter.authoriseAction(exchange, "publish", args);

        verify(_accessControl).authorise(eq(LegacyOperation.PUBLISH), eq(ObjectType.EXCHANGE), eq(properties));

    }

    public void testAuthoriseCreateConnection()
    {

        ObjectProperties properties = new ObjectProperties();
        properties.put(ObjectProperties.Property.NAME, TEST_VIRTUAL_HOST);
        properties.put(ObjectProperties.Property.VIRTUALHOST_NAME, TEST_VIRTUAL_HOST);

        _adapter.authoriseAction(_virtualHost, "connect", Collections.<String,Object>emptyMap());

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
        assertBrokerChildCreateAuthorization(object, _broker);
    }

    private void assertBrokerChildCreateAuthorization(ConfiguredObject object, ConfiguredObject parent)
    {
        String description = String.format("%s %s '%s'",
                                           LegacyOperation.CREATE.name().toLowerCase(),
                                           object.getCategoryClass().getSimpleName().toLowerCase(),
                                           "TEST");
        ObjectProperties properties = new OperationLoggingDetails(description);
        assertCreateAuthorization(object, LegacyOperation.CONFIGURE, ObjectType.BROKER, properties, parent);
    }


    private void assertCreateAuthorization(ConfiguredObject<?> configuredObject, LegacyOperation aclOperation, ObjectType aclObjectType, ObjectProperties expectedProperties, ConfiguredObject<?>... parents)
    {
        _adapter.authorise(LegacyOperation.CREATE, configuredObject);
        verify(_accessControl).authorise(eq(aclOperation), eq(aclObjectType), eq(expectedProperties));
    }


    private void assertBrokerChildUpdateAuthorization(ConfiguredObject configuredObject)
    {
        assertBrokerChildUpdateAuthorization(configuredObject, _broker);
    }

    private void assertBrokerChildUpdateAuthorization(ConfiguredObject configuredObject, ConfiguredObject parent)
    {
        String description = String.format("%s %s '%s'",
                                           LegacyOperation.UPDATE.name().toLowerCase(),
                                           configuredObject.getCategoryClass().getSimpleName().toLowerCase(),
                                           configuredObject.getName());
        ObjectProperties properties = new OperationLoggingDetails(description);

        assertUpdateAuthorization(configuredObject, LegacyOperation.CONFIGURE, ObjectType.BROKER,
                                  properties, parent);
    }

    private void assertUpdateAuthorization(ConfiguredObject<?> configuredObject, LegacyOperation aclOperation, ObjectType aclObjectType, ObjectProperties expectedProperties, ConfiguredObject... objects)
    {
        assertAuthorization(LegacyOperation.UPDATE, configuredObject, aclOperation, aclObjectType, expectedProperties, objects);
    }

    private void assertBrokerChildDeleteAuthorization(ConfiguredObject configuredObject)
    {
        assertBrokerChildDeleteAuthorization(configuredObject, _broker);
    }

    private void assertBrokerChildDeleteAuthorization(ConfiguredObject configuredObject, ConfiguredObject parent)
    {
        String description = String.format("%s %s '%s'",
                                           LegacyOperation.DELETE.name().toLowerCase(),
                                           configuredObject.getCategoryClass().getSimpleName().toLowerCase(),
                                           configuredObject.getName());
        ObjectProperties properties = new OperationLoggingDetails(description);

        assertDeleteAuthorization(configuredObject, LegacyOperation.CONFIGURE, ObjectType.BROKER,
                                  properties, parent);
    }


    private void assertDeleteAuthorization(ConfiguredObject<?> configuredObject, LegacyOperation aclOperation, ObjectType aclObjectType, ObjectProperties expectedProperties, ConfiguredObject... objects)
    {
        assertAuthorization(LegacyOperation.DELETE, configuredObject, aclOperation, aclObjectType, expectedProperties, objects);
    }

    private void assertAuthorization(LegacyOperation operation, ConfiguredObject<?> configuredObject, LegacyOperation aclOperation, ObjectType aclObjectType, ObjectProperties expectedProperties, ConfiguredObject... objects)
    {
        _adapter.authorise(operation, configuredObject);
        verify(_accessControl).authorise(eq(aclOperation), eq(aclObjectType), eq(expectedProperties));
    }
}
