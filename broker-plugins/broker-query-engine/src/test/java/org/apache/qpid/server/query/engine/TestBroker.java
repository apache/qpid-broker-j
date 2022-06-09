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
package org.apache.qpid.server.query.engine;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.ClassRule;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.query.engine.evaluator.settings.DefaultQuerySettings;
import org.apache.qpid.server.security.CertificateDetails;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhostnode.memory.MemoryVirtualHostNode;
import org.apache.qpid.test.utils.tls.TlsResource;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBroker
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static Broker<?> _broker;

    public static Broker<?> createBroker()
    {
        try
        {
            if (_broker != null)
            {
                return _broker;
            }

            _broker = BrokerTestHelper.createBrokerMock();

            List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Broker.class));
            when(_broker.getAttributeNames()).thenReturn(attributeNames);
            when(_broker.getAttribute(eq("name"))).thenReturn("mock");
            when(_broker.getName()).thenReturn("mock");

            final Map<String, Object> attributesMap = new HashMap<>();
            attributesMap.put(AuthenticationProvider.NAME, "ScramSHA256AuthenticationManager");
            attributesMap.put(AuthenticationProvider.TYPE, "SCRAM-SHA-256");
            attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
            ScramSHA256AuthenticationManager authProvider =
                    (ScramSHA256AuthenticationManager) _broker.getObjectFactory()
                                                              .create(AuthenticationProvider.class,
                                                                      attributesMap,
                                                                      _broker);
            when(_broker.getAuthenticationProviders()).thenReturn(Collections.singletonList(authProvider));

            HttpPort<?> httpPort = mock(HttpPort.class);
            when(httpPort.getPort()).thenReturn(40606);
            when(httpPort.getBoundPort()).thenReturn(40606);
            when(httpPort.getCreatedBy()).thenReturn("admin");
            when(httpPort.getCreatedTime()).thenReturn(new Date());
            when(httpPort.getName()).thenReturn("httpPort");
            when(httpPort.getState()).thenReturn(State.ACTIVE);
            when(httpPort.getType()).thenReturn("HTTP");
            when(httpPort.getAttribute(HttpPort.AUTHENTICATION_PROVIDER)).thenReturn(authProvider);

            Collection<Connection> connections = new ArrayList<>();

            Port<?> amqpPort = mock(Port.class);
            when(amqpPort.getConnections()).thenReturn(connections);
            when(amqpPort.getChildren(Connection.class)).thenReturn(connections);
            when(amqpPort.getPort()).thenReturn(40206);
            when(amqpPort.getBoundPort()).thenReturn(40206);
            when(amqpPort.getCreatedBy()).thenReturn("admin");
            when(amqpPort.getCreatedTime()).thenReturn(new Date());
            when(amqpPort.getName()).thenReturn("amqpPort");
            when(amqpPort.getState()).thenReturn(State.ACTIVE);
            when(amqpPort.getType()).thenReturn("AMQP");

            when(_broker.getPorts()).thenReturn(Arrays.asList(httpPort, amqpPort));
            when(_broker.getChildren(eq(Port.class))).thenReturn(Arrays.asList(httpPort, amqpPort));

            final Map<String, Object> vhnAttributes = new HashMap<>();
            vhnAttributes.put(VirtualHostNode.TYPE, MemoryVirtualHostNode.VIRTUAL_HOST_NODE_TYPE);
            vhnAttributes.put(VirtualHostNode.NAME, "default");
            VirtualHostNode<?> virtualHostNode =
                    _broker.getObjectFactory().create(VirtualHostNode.class, vhnAttributes, _broker);

            VirtualHostNode mockVirtualHostNode = mock(VirtualHostNode.class);
            when(mockVirtualHostNode.getAttributeNames()).thenReturn(virtualHostNode.getAttributeNames());
            when(mockVirtualHostNode.getAttribute(eq(VirtualHostNode.NAME))).thenReturn("mock");
            when(mockVirtualHostNode.getAttribute(eq(VirtualHostNode.TYPE))).thenReturn("mock");
            when(mockVirtualHostNode.getAttribute(eq(VirtualHostNode.CREATED_TIME)))
                .thenReturn(new Date(101, Calendar.JANUARY, 1, 12, 55, 30));
            when(mockVirtualHostNode.getCreatedTime()).thenReturn(new Date(101, Calendar.JANUARY, 1, 12, 55, 30));

            when(_broker.getVirtualHostNodes()).thenReturn(Arrays.asList(mockVirtualHostNode,
                                                                   mockVirtualHostNode,
                                                                   virtualHostNode));
            when(_broker.getChildren(eq(VirtualHostNode.class))).thenReturn(Arrays.asList(virtualHostNode,
                                                                                    mockVirtualHostNode));

            final Map<String, Object> vhAttributes = new HashMap<>();
            vhAttributes.put(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
            vhAttributes.put(VirtualHost.NAME, "default");
            TestMemoryVirtualHost virtualHost =
                    (TestMemoryVirtualHost) virtualHostNode.createChild(VirtualHost.class, vhAttributes);

            List<Queue> queues = new ArrayList<>();
            for (int i = 0; i < 10; i++)
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(Queue.ID, UUID.randomUUID());
                attributes.put(Queue.NAME, "QUEUE_" + i);
                attributes.put(Queue.LAST_UPDATED_TIME, new Date(2021, Calendar.JANUARY, 0, 0, 0, 0));
                attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
                if (i % 2 == 0)
                {
                    attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                }
                attributes.put(Queue.DESCRIPTION, "test description 1");
                queues.add(virtualHost.createChild(Queue.class, attributes));
            }
            for (int i = 10; i < 20; i++)
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(Queue.ID, UUID.randomUUID());
                attributes.put(Queue.NAME, "QUEUE_" + i);
                attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.REJECT);
                if (i % 2 == 0)
                {
                    attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                }
                attributes.put(Queue.DESCRIPTION, "test description 2");
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 1024 * 1024);
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100);
                queues.add(virtualHost.createChild(Queue.class, attributes));
            }
            for (int i = 20; i < 30; i++)
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(Queue.ID, UUID.randomUUID());
                attributes.put(Queue.NAME, "QUEUE_" + i);
                attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.FLOW_TO_DISK);
                if (i % 2 == 0)
                {
                    attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                }
                attributes.put(Queue.DESCRIPTION, "test description 3");
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 1024 * 1024);
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100);
                queues.add(virtualHost.createChild(Queue.class, attributes));
            }
            for (int i = 30; i < 40; i++)
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(Queue.ID, UUID.randomUUID());
                attributes.put(Queue.NAME, "QUEUE_" + i);
                attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.PRODUCER_FLOW_CONTROL);
                if (i % 2 == 0)
                {
                    attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                }
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 1024 * 1024);
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100);
                queues.add(virtualHost.createChild(Queue.class, attributes));
            }
            for (int i = 40; i < 50; i++)
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(Queue.ID, UUID.randomUUID());
                attributes.put(Queue.NAME, "QUEUE_" + i);
                attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.NONE);
                if (i % 2 == 0)
                {
                    attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                    attributes.put(Queue.LIFETIME_POLICY, LifetimePolicy.PERMANENT);
                }
                else
                {
                    attributes.put(Queue.LIFETIME_POLICY, LifetimePolicy.IN_USE);
                }
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 1024 * 1024);
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100);
                queues.add(virtualHost.createChild(Queue.class, attributes));
            }
            for (int i = 50; i < 60; i++)
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(Queue.ID, UUID.randomUUID());
                attributes.put(Queue.NAME, "QUEUE_" + i);
                attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.NONE);
                if (i % 2 == 0)
                {
                    attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                    attributes.put(Queue.LIFETIME_POLICY, LifetimePolicy.PERMANENT);
                }
                else
                {
                    attributes.put(Queue.LIFETIME_POLICY, LifetimePolicy.IN_USE);
                }
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 1024 * 1024);
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100);
                Queue queue = virtualHost.createChild(Queue.class, attributes);
                for (int j = 0; j < 65; j++)
                {
                    queue.enqueue(createMessage((long) j), null, null);
                }
                queues.add(queue);
            }

            for (int i = 60; i < 70; i++)
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(Queue.ID, UUID.randomUUID());
                attributes.put(Queue.NAME, "QUEUE_" + i);
                attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.NONE);
                if (i % 2 == 0)
                {
                    attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                }
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 1024 * 1024);
                attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 100);
                Queue queue = virtualHost.createChild(Queue.class, attributes);
                for (int j = 0; j < 95; j++)
                {
                    queue.enqueue(createMessage((long) j), null, null);
                }
                queues.add(queue);
            }

            List<Exchange> exchanges = new ArrayList<>();
            for (int i = 0; i < 10; i++)
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(Exchange.ID, UUID.randomUUID());
                attributes.put(Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
                attributes.put(Exchange.NAME, "EXCHANGE_" + i);
                attributes.put(Exchange.DESCRIPTION, "test description " + i);
                Exchange exchange = virtualHost.createChild(Exchange.class, attributes);
                exchange.bind("QUEUE_1", "#", new HashMap<>(), true);
                exchanges.add(exchange);
            }

            for (int i = 1 ; i < 11; i ++)
            {
                connections.add(createAMQPConnection(i, "127.0.0.1", "principal1"));
            }
            for (int i = 11 ; i < 21; i ++)
            {
                connections.add(createAMQPConnection(i, "127.0.0.2", "principal2"));
            }
            for (int i = 21 ; i < 31; i ++)
            {
                connections.add(createAMQPConnection(i, "127.0.0.3", "principal3"));
            }

            final Collection<TrustStore> trustStores = createTruststores();
            when(_broker.getChildren(eq(TrustStore.class))).thenReturn(trustStores);


        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }

        return _broker;
    }

    protected static Collection<TrustStore> createTruststores()
    {
        final Collection<TrustStore> trustStores = new ArrayList<>();
        final List<CertificateDetails> certificateDetails = createCertificateDetails();

        final TrustStore<?> trustStore = mock(TrustStore.class);
        when(trustStore.getName()).thenReturn("peersTruststore");
        when(trustStore.getAttribute(TrustStore.ID)).thenReturn(UUID.randomUUID());
        when(trustStore.getAttribute(TrustStore.NAME)).thenReturn("peersTruststore");
        when(trustStore.getAttribute(TrustStore.DESCRIPTION)).thenReturn("mock");
        when(trustStore.getCertificateDetails()).thenReturn(certificateDetails);

        trustStores.add(trustStore);

        return trustStores;
    }

    protected static List<CertificateDetails> createCertificateDetails()
    {
        final List<CertificateDetails> list = new ArrayList<>();

        final CertificateDetails certificateDetails1 = mock(CertificateDetails.class);
        when(certificateDetails1.getAlias()).thenReturn("aaa_mock");
        when(certificateDetails1.getIssuerName()).thenReturn("CN=aaa_mock");
        when(certificateDetails1.getSerialNumber()).thenReturn(String.valueOf(1L));
        when(certificateDetails1.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails1.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails1.getSubjectName()).thenReturn("CN=aaa_mock");
        when(certificateDetails1.getValidFrom()).thenReturn(createDate("2020-01-01 00:00:00"));
        when(certificateDetails1.getValidUntil()).thenReturn(createDate("2022-12-31 23:59:59"));
        when(certificateDetails1.getVersion()).thenReturn(3);
        list.add(certificateDetails1);

        final CertificateDetails certificateDetails2 = mock(CertificateDetails.class);
        when(certificateDetails2.getAlias()).thenReturn("bbb_mock");
        when(certificateDetails2.getIssuerName()).thenReturn("CN=bbb_mock");
        when(certificateDetails2.getSerialNumber()).thenReturn(String.valueOf(100L));
        when(certificateDetails2.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails2.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails2.getSubjectName()).thenReturn("CN=bbb_mock");
        when(certificateDetails2.getValidFrom()).thenReturn(createDate("2020-01-02 00:00:00"));
        when(certificateDetails2.getValidUntil()).thenReturn(createDate("2023-01-01 23:59:59"));
        list.add(certificateDetails2);

        final CertificateDetails certificateDetails3 = mock(CertificateDetails.class);
        when(certificateDetails3.getAlias()).thenReturn("ccc_mock");
        when(certificateDetails3.getIssuerName()).thenReturn("CN=ccc_mock");
        when(certificateDetails3.getSerialNumber()).thenReturn(String.valueOf(1000L));
        when(certificateDetails3.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails3.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails3.getSubjectName()).thenReturn("CN=ccc_mock");
        when(certificateDetails3.getValidFrom()).thenReturn(createDate("2020-01-03 00:00:00"));
        when(certificateDetails3.getValidUntil()).thenReturn(createDate("2023-01-02 23:59:59"));
        list.add(certificateDetails3);

        final CertificateDetails certificateDetails4 = mock(CertificateDetails.class);
        when(certificateDetails4.getAlias()).thenReturn("ddd_mock");
        when(certificateDetails4.getIssuerName()).thenReturn("CN=ddd_mock");
        when(certificateDetails4.getSerialNumber()).thenReturn(String.valueOf(10_000L));
        when(certificateDetails4.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails4.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails4.getSubjectName()).thenReturn("CN=ddd_mock");
        when(certificateDetails4.getValidFrom()).thenReturn(createDate("2020-01-04 00:00:00"));
        when(certificateDetails4.getValidUntil()).thenReturn(createDate("2023-01-03 23:59:59"));
        list.add(certificateDetails4);

        final CertificateDetails certificateDetails5 = mock(CertificateDetails.class);
        when(certificateDetails5.getAlias()).thenReturn("eee_mock");
        when(certificateDetails5.getIssuerName()).thenReturn("CN=eee_mock");
        when(certificateDetails5.getSerialNumber()).thenReturn(String.valueOf(100_000L));
        when(certificateDetails5.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails5.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails5.getSubjectName()).thenReturn("CN=eee_mock");
        when(certificateDetails5.getValidFrom()).thenReturn(createDate("2020-01-05 00:00:00"));
        when(certificateDetails5.getValidUntil()).thenReturn(createDate("2023-01-04 23:59:59"));
        list.add(certificateDetails5);

        final CertificateDetails certificateDetails6 = mock(CertificateDetails.class);
        when(certificateDetails6.getAlias()).thenReturn("fff_mock");
        when(certificateDetails6.getIssuerName()).thenReturn("CN=fff_mock");
        when(certificateDetails6.getSerialNumber()).thenReturn(String.valueOf(1000_000L));
        when(certificateDetails6.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails6.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails6.getSubjectName()).thenReturn("CN=fff_mock");
        when(certificateDetails6.getValidFrom()).thenReturn(createDate("2020-01-06 00:00:00"));
        when(certificateDetails6.getValidUntil()).thenReturn(createDate("2023-01-05 23:59:59"));
        list.add(certificateDetails6);

        final CertificateDetails certificateDetails7 = mock(CertificateDetails.class);
        when(certificateDetails7.getAlias()).thenReturn("ggg_mock");
        when(certificateDetails7.getIssuerName()).thenReturn("CN=ggg_mock");
        when(certificateDetails7.getSerialNumber()).thenReturn(String.valueOf(10_000_000L));
        when(certificateDetails7.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails7.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails7.getSubjectName()).thenReturn("CN=ggg_mock");
        when(certificateDetails7.getValidFrom()).thenReturn(createDate("2020-01-07 00:00:00"));
        when(certificateDetails7.getValidUntil()).thenReturn(createDate("2023-01-06 23:59:59"));
        list.add(certificateDetails7);

        final CertificateDetails certificateDetails8 = mock(CertificateDetails.class);
        when(certificateDetails8.getAlias()).thenReturn("hhh_mock");
        when(certificateDetails8.getIssuerName()).thenReturn("CN=hhh_mock");
        when(certificateDetails8.getSerialNumber()).thenReturn(String.valueOf(100_000_000L));
        when(certificateDetails8.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails8.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails8.getSubjectName()).thenReturn("CN=hhh_mock");
        when(certificateDetails8.getValidFrom()).thenReturn(createDate("2020-01-08 00:00:00"));
        when(certificateDetails8.getValidUntil()).thenReturn(createDate("2023-01-07 23:59:59"));
        list.add(certificateDetails8);

        final CertificateDetails certificateDetails9 = mock(CertificateDetails.class);
        when(certificateDetails9.getAlias()).thenReturn("iii_mock");
        when(certificateDetails9.getIssuerName()).thenReturn("CN=iii_mock");
        when(certificateDetails9.getSerialNumber()).thenReturn(String.valueOf(1000_000_000L));
        when(certificateDetails9.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails9.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails9.getSubjectName()).thenReturn("CN=iii_mock");
        when(certificateDetails9.getValidFrom()).thenReturn(createDate("2020-01-09 00:00:00"));
        when(certificateDetails9.getValidUntil()).thenReturn(createDate("2023-01-08 23:59:59"));
        list.add(certificateDetails9);

        final CertificateDetails certificateDetails10 = mock(CertificateDetails.class);
        when(certificateDetails10.getAlias()).thenReturn("jjj_mock");
        when(certificateDetails10.getIssuerName()).thenReturn("CN=jjj_mock");
        when(certificateDetails10.getSerialNumber()).thenReturn(new BigInteger("17593798617727720249").toString(10));
        when(certificateDetails10.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails10.getSubjectAltNames()).thenReturn(Collections.emptyList());
        when(certificateDetails10.getSubjectName()).thenReturn("CN=jjj_mock");
        when(certificateDetails10.getValidFrom()).thenReturn(createDate("2020-01-10 00:00:00"));
        when(certificateDetails10.getValidUntil()).thenReturn(createDate("2023-01-09 23:59:59"));
        list.add(certificateDetails10);

        return list;
    }

    protected static ServerMessage createMessage(Long id)
    {
        AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(String.valueOf(id));
        ServerMessage message = mock(ServerMessage.class);
        when(message.getMessageNumber()).thenReturn(id);
        when(message.getMessageHeader()).thenReturn(header);
        when(message.checkValid()).thenReturn(true);

        StoredMessage storedMessage = mock(StoredMessage.class);
        when(message.getStoredMessage()).thenReturn(storedMessage);

        MessageReference ref = mock(MessageReference.class);
        when(ref.getMessage()).thenReturn(message);

        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);

        return message;
    }

    protected static Connection createAMQPConnection(int number, String hostname, String principal)
    {
        List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Connection.class));
        AMQPConnection<?> connection = mock(AMQPConnection.class);
        when(connection.getAttributeNames()).thenReturn(attributeNames);
        when(connection.getAttribute(Connection.ID)).thenReturn(UUID.randomUUID());
        when(connection.getAttribute(Connection.NAME)).thenReturn(String.format("[%d] %s:%d", number, hostname, (47290 + number)));
        when(connection.getAttribute(Connection.DESCRIPTION)).thenReturn(null);
        when(connection.getAttribute(Connection.TYPE)).thenReturn("AMQP_1_0");
        when(connection.getAttribute(Connection.STATE)).thenReturn(State.ACTIVE);
        when(connection.getAttribute(Connection.DURABLE)).thenReturn(false);
        when(connection.getAttribute(Connection.LIFETIME_POLICY)).thenReturn(LifetimePolicy.PERMANENT);
        when(connection.getAttribute(Connection.CONTEXT)).thenReturn(new HashMap<>());
        when(connection.getAttribute(Connection.CLIENT_ID)).thenReturn("ID:" + UUID.randomUUID() + ":1");
        when(connection.getClientProduct()).thenReturn("QpidJMS");
        when(connection.getAttribute(Connection.CLIENT_VERSION)).thenReturn("0.32.0");
        when(connection.getAttribute(Connection.INCOMING)).thenReturn(true);
        when(connection.getAttribute(Connection.PRINCIPAL)).thenReturn(principal);
        when(connection.getProtocol()).thenReturn(Protocol.AMQP_1_0);
        when(connection.getAttribute(Connection.REMOTE_ADDRESS)).thenReturn(String.format("/%s:%d", hostname, (47290 + number)));
        when(connection.getAttribute(Connection.TRANSPORT)).thenReturn(Transport.TCP);
        return connection;
    }

    protected static Date createDate(String date)
    {
        final LocalDateTime dateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(DefaultQuerySettings.DATE_TIME_PATTERN));
        return Date.from(dateTime.toInstant(ZoneOffset.UTC));
    }
}
