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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.security.CertificateDetails;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhostnode.TestVirtualHostNode;

/**
 * Helper class for creating broker mock
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBroker
{
    private static Broker<?> _broker;

    private static final List<Connection> connections = new ArrayList<>();

    public static Broker<?> createBroker()
    {
        try
        {
            if (_broker != null)
            {
                return _broker;
            }

            _broker = BrokerTestHelper.createBrokerMock();

            final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Broker.class));
            when(_broker.getAttributeNames()).thenReturn(attributeNames);
            when(_broker.getAttribute(eq("name"))).thenReturn("mock");
            when(_broker.getName()).thenReturn("mock");

            final Map<String, Object> attributesMap = new HashMap<>();
            attributesMap.put(AuthenticationProvider.NAME, "ScramSHA256AuthenticationManager");
            attributesMap.put(AuthenticationProvider.TYPE, "SCRAM-SHA-256");
            attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
            final ScramSHA256AuthenticationManager authProvider =
                (ScramSHA256AuthenticationManager) _broker.getObjectFactory()
                    .create(AuthenticationProvider.class, attributesMap, _broker);
            when(_broker.getAuthenticationProviders()).thenReturn(List.of(authProvider));

            final List<String> portAttributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Port.class));
            final HttpPort<?> httpPort = mock(HttpPort.class);
            when(httpPort.getAttributeNames()).thenReturn(portAttributeNames);
            when(httpPort.getPort()).thenReturn(40606);
            when(httpPort.getBoundPort()).thenReturn(40606);
            when(httpPort.getCreatedBy()).thenReturn("admin");
            when(httpPort.getCreatedTime()).thenReturn(new Date());
            when(httpPort.getName()).thenReturn("httpPort");
            when(httpPort.getAttribute(Port.NAME)).thenReturn("httpPort");
            when(httpPort.getState()).thenReturn(State.ACTIVE);
            when(httpPort.getType()).thenReturn("HTTP");
            when(httpPort.getAttribute(HttpPort.AUTHENTICATION_PROVIDER)).thenReturn(authProvider);
            when(httpPort.getStatistics()).thenReturn(new HashMap<>());

            final Port<?> amqpPort = mock(Port.class);
            when(amqpPort.getAttributeNames()).thenReturn(portAttributeNames);
            when(amqpPort.getConnections()).thenReturn(connections);
            when(amqpPort.getChildren(Connection.class)).thenReturn(connections);
            when(amqpPort.getPort()).thenReturn(40206);
            when(amqpPort.getBoundPort()).thenReturn(40206);
            when(amqpPort.getCreatedBy()).thenReturn("admin");
            when(amqpPort.getCreatedTime()).thenReturn(new Date());
            when(amqpPort.getName()).thenReturn("amqpPort");
            when(amqpPort.getAttribute(Port.NAME)).thenReturn("amqpPort");
            when(amqpPort.getState()).thenReturn(State.ACTIVE);
            when(amqpPort.getType()).thenReturn("AMQP");
            final Map<String, Object> portStatistics = new HashMap<>();
            portStatistics.put("connectionCount", 30);
            portStatistics.put("totalConnectionCount", 30);
            when(httpPort.getStatistics()).thenReturn(portStatistics);

            when(_broker.getPorts()).thenReturn(Arrays.asList(httpPort, amqpPort));
            when(_broker.getChildren(eq(Port.class))).thenReturn(Arrays.asList(httpPort, amqpPort));

            final List<String> vhnAttributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(VirtualHostNode.class));
            final VirtualHostNode<?> virtualHostNode = mock(VirtualHostNode.class);
            when(virtualHostNode.getAttributeNames()).thenReturn(vhnAttributeNames);
            when(virtualHostNode.getAttribute(VirtualHostNode.ID)).thenReturn(UUID.randomUUID());
            when(virtualHostNode.getAttribute(eq(VirtualHostNode.NAME))).thenReturn("default");
            when(virtualHostNode.getAttribute(eq(VirtualHostNode.TYPE))).thenReturn(TestVirtualHostNode.VIRTUAL_HOST_NODE_TYPE);
            when(virtualHostNode.getAttribute(eq(VirtualHostNode.CREATED_TIME))).thenReturn(new Date());
            when(virtualHostNode.getCreatedTime()).thenReturn(new Date());

            final VirtualHostNode mockVirtualHostNode = mock(VirtualHostNode.class);
            when(mockVirtualHostNode.getAttributeNames()).thenReturn(vhnAttributeNames);
            when(mockVirtualHostNode.getAttribute(VirtualHostNode.ID)).thenReturn(UUID.randomUUID());
            when(mockVirtualHostNode.getAttribute(eq(VirtualHostNode.NAME))).thenReturn("mock");
            when(mockVirtualHostNode.getAttribute(eq(VirtualHostNode.TYPE))).thenReturn("mock");
            when(mockVirtualHostNode.getAttribute(eq(VirtualHostNode.CREATED_TIME)))
                .thenReturn(createDate("2001-01-01 12:55:30"));
            when(mockVirtualHostNode.getCreatedTime()).thenReturn(createDate("2001-01-01 12:55:30"));

            when(_broker.getVirtualHostNodes()).thenReturn(
                Arrays.asList(mockVirtualHostNode, virtualHostNode)
            );
            when(_broker.getChildren(eq(VirtualHostNode.class))).thenReturn(
                Arrays.asList(virtualHostNode, mockVirtualHostNode)
            );

            final List<String> vhAttributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(VirtualHost.class));
            final VirtualHost virtualHost = mock(VirtualHost.class);
            when(virtualHost.getAttributeNames()).thenReturn(vhAttributeNames);
            when(virtualHost.getAttribute(VirtualHost.ID)).thenReturn(UUID.randomUUID());
            when(virtualHost.getAttribute(VirtualHost.TYPE)).thenReturn(TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
            when(virtualHost.getAttribute(VirtualHost.NAME)).thenReturn("default");
            when(virtualHostNode.getChildren(eq(VirtualHost.class))).thenReturn(List.of(virtualHost));
            when(virtualHostNode.getVirtualHost()).thenReturn(virtualHost);

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

            final List<Queue> queues = new ArrayList<>();
            for (int i = 0; i < 10; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.RING, "test description 1");
                queues.add(queue);
            }
            for (int i = 10; i < 20; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.REJECT, "test description 2");
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES)).thenReturn(1024 * 1024);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES)).thenReturn(100);
                queues.add(queue);
            }
            for (int i = 20; i < 30; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.FLOW_TO_DISK, "test description 3");
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES)).thenReturn(1024 * 1024);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES)).thenReturn(100);
                queues.add(queue);
            }
            for (int i = 30; i < 40; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.PRODUCER_FLOW_CONTROL, null);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES)).thenReturn(1024 * 1024);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES)).thenReturn(100);
                queues.add(queue);
            }
            for (int i = 40; i < 50; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.NONE, null);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES)).thenReturn(1024 * 1024);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES)).thenReturn(100);
                if (i % 2 == 0)
                {
                    when(queue.getAttribute(Queue.EXPIRY_POLICY)).thenReturn(Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                    when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.PERMANENT);
                }
                else
                {
                    when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.IN_USE);
                }
                queues.add(queue);
            }
            for (int i = 50; i < 60; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.NONE, null);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES)).thenReturn(1024 * 1024);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES)).thenReturn(100);
                if (i % 2 == 0)
                {
                    when(queue.getAttribute(Queue.EXPIRY_POLICY)).thenReturn(Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
                    when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.PERMANENT);
                }
                else
                {
                    when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.IN_USE);
                }

                final Map<String, Object> statistics = new HashMap<>();
                statistics.put("availableMessages", 65);
                statistics.put("bindingCount", 0);
                statistics.put("queueDepthMessages", 65);
                statistics.put("queueDepthBytes", 0);
                statistics.put("totalExpiredBytes", 0);
                when(queue.getStatistics()).thenReturn(statistics);

                queues.add(queue);
            }

            for (int i = 60; i < 70; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.NONE, null);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES)).thenReturn(1024 * 1024);
                when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES)).thenReturn(100);

                final Map<String, Object> statistics = new HashMap<>();
                statistics.put("availableMessages", 95);
                statistics.put("bindingCount", 0);
                statistics.put("queueDepthMessages", 95);
                statistics.put("queueDepthBytes", 0);
                statistics.put("totalExpiredBytes", 0);
                when(queue.getStatistics()).thenReturn(statistics);

                queues.add(queue);
            }
            when(virtualHost.getChildren(eq(Queue.class))).thenReturn(queues);

            final List<Exchange> exchanges = new ArrayList<>();
            for (int i = 0; i < 10; i++)
            {
                exchanges.add(creatExchange(i));
            }
            when(virtualHost.getChildren(eq(Exchange.class))).thenReturn(exchanges);

            final Collection<TrustStore> trustStores = createTruststores();
            when(_broker.getChildren(eq(TrustStore.class))).thenReturn(trustStores);

            when(_broker.getAttribute("maximumHeapMemorySize")).thenReturn(10_000_000_000L);
            when(_broker.getAttribute("maximumDirectMemorySize")).thenReturn(1_500_000_000L);
            when(_broker.getStatistics()).thenReturn(getBrokerStatistics());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        return _broker;
    }

    protected static Queue createQueue(int number, OverflowPolicy overflowPolicy, String description)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Queue.class));
        final Queue<?> queue = mock(Queue.class);
        when(queue.getAttributeNames()).thenReturn(attributeNames);
        when(queue.getAttribute(Queue.ID)).thenReturn(UUID.randomUUID());
        when(queue.getAttribute(Queue.NAME)).thenReturn("QUEUE_" + number);
        when(queue.getAttribute(Queue.TYPE)).thenReturn("standard");
        final Date createdTime = new Date();
        when(queue.getAttribute(Queue.CREATED_TIME)).thenReturn(createdTime);
        when(queue.getCreatedTime()).thenReturn(createdTime);
        when(queue.getAttribute(Queue.LAST_UPDATED_TIME)).thenReturn(new Date());
        when(queue.getAttribute(Queue.OVERFLOW_POLICY)).thenReturn(overflowPolicy);
        if (number % 2 == 0)
        {
            when(queue.getAttribute(Queue.EXPIRY_POLICY)).thenReturn(Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);
        }
        else
        {
            when(queue.getAttribute(Queue.EXPIRY_POLICY)).thenReturn(Queue.ExpiryPolicy.DELETE);
        }
        when(queue.getAttribute(Queue.LIFETIME_POLICY)).thenReturn(LifetimePolicy.PERMANENT);
        when(queue.getAttribute(Queue.DESCRIPTION)).thenReturn(description);
        when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES)).thenReturn(-1);
        when(queue.getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES)).thenReturn(-1);

        final Map<String, Object> statistics = new HashMap<>();
        statistics.put("availableMessages", 0);
        statistics.put("bindingCount", number == 1 ? 10 : 0);
        statistics.put("queueDepthMessages", 0);
        statistics.put("queueDepthBytes", 0);
        statistics.put("totalExpiredBytes", 0);
        when(queue.getStatistics()).thenReturn(statistics);

        if (number > 0 && number < 11)
        {
            final Consumer consumer = createConsumer(number, "QUEUE_" + number);
            when(queue.getChildren(eq(Consumer.class))).thenReturn(new ArrayList<>(List.of(consumer)));
        }

        return queue;
    }

    protected static Exchange creatExchange(int number)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Exchange.class));
        final Exchange<?> exchange = mock(Exchange.class);
        when(exchange.getAttributeNames()).thenReturn(attributeNames);
        when(exchange.getAttribute(Exchange.ID)).thenReturn(UUID.randomUUID());
        when(exchange.getAttribute(Exchange.TYPE)).thenReturn(ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
        when(exchange.getAttribute(Exchange.NAME)).thenReturn("EXCHANGE_" + number);
        when(exchange.getName()).thenReturn("EXCHANGE_" + number);
        when(exchange.getAttribute(Exchange.DESCRIPTION)).thenReturn("test description " + number);

        final Binding binding = mock(Binding.class);
        when(binding.getName()).thenReturn("#");
        when(binding.getBindingKey()).thenReturn("#");
        when(binding.getType()).thenReturn("binding");
        when(binding.getDestination()).thenReturn("QUEUE_1");
        when(binding.getArguments()).thenReturn(new HashMap<>());

        when(exchange.getAttribute("bindings")).thenReturn(List.of(binding));
        when(exchange.getBindings()).thenReturn(List.of(binding));

        return exchange;
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
        when(certificateDetails1.getSubjectAltNames()).thenReturn(List.of());
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
        when(certificateDetails2.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails2.getSubjectName()).thenReturn("CN=bbb_mock");
        when(certificateDetails2.getValidFrom()).thenReturn(createDate("2020-01-02 00:00:00"));
        when(certificateDetails2.getValidUntil()).thenReturn(createDate("2023-01-01 23:59:59"));
        list.add(certificateDetails2);

        final CertificateDetails certificateDetails3 = mock(CertificateDetails.class);
        when(certificateDetails3.getAlias()).thenReturn("ccc_mock");
        when(certificateDetails3.getIssuerName()).thenReturn("CN=ccc_mock");
        when(certificateDetails3.getSerialNumber()).thenReturn(String.valueOf(1000L));
        when(certificateDetails3.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails3.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails3.getSubjectName()).thenReturn("CN=ccc_mock");
        when(certificateDetails3.getValidFrom()).thenReturn(createDate("2020-01-03 00:00:00"));
        when(certificateDetails3.getValidUntil()).thenReturn(createDate("2023-01-02 23:59:59"));
        list.add(certificateDetails3);

        final CertificateDetails certificateDetails4 = mock(CertificateDetails.class);
        when(certificateDetails4.getAlias()).thenReturn("ddd_mock");
        when(certificateDetails4.getIssuerName()).thenReturn("CN=ddd_mock");
        when(certificateDetails4.getSerialNumber()).thenReturn(String.valueOf(10_000L));
        when(certificateDetails4.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails4.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails4.getSubjectName()).thenReturn("CN=ddd_mock");
        when(certificateDetails4.getValidFrom()).thenReturn(createDate("2020-01-04 00:00:00"));
        when(certificateDetails4.getValidUntil()).thenReturn(createDate("2023-01-03 23:59:59"));
        list.add(certificateDetails4);

        final CertificateDetails certificateDetails5 = mock(CertificateDetails.class);
        when(certificateDetails5.getAlias()).thenReturn("eee_mock");
        when(certificateDetails5.getIssuerName()).thenReturn("CN=eee_mock");
        when(certificateDetails5.getSerialNumber()).thenReturn(String.valueOf(100_000L));
        when(certificateDetails5.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails5.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails5.getSubjectName()).thenReturn("CN=eee_mock");
        when(certificateDetails5.getValidFrom()).thenReturn(createDate("2020-01-05 00:00:00"));
        when(certificateDetails5.getValidUntil()).thenReturn(createDate("2023-01-04 23:59:59"));
        list.add(certificateDetails5);

        final CertificateDetails certificateDetails6 = mock(CertificateDetails.class);
        when(certificateDetails6.getAlias()).thenReturn("fff_mock");
        when(certificateDetails6.getIssuerName()).thenReturn("CN=fff_mock");
        when(certificateDetails6.getSerialNumber()).thenReturn(String.valueOf(1000_000L));
        when(certificateDetails6.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails6.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails6.getSubjectName()).thenReturn("CN=fff_mock");
        when(certificateDetails6.getValidFrom()).thenReturn(createDate("2020-01-06 00:00:00"));
        when(certificateDetails6.getValidUntil()).thenReturn(createDate("2023-01-05 23:59:59"));
        list.add(certificateDetails6);

        final CertificateDetails certificateDetails7 = mock(CertificateDetails.class);
        when(certificateDetails7.getAlias()).thenReturn("ggg_mock");
        when(certificateDetails7.getIssuerName()).thenReturn("CN=ggg_mock");
        when(certificateDetails7.getSerialNumber()).thenReturn(String.valueOf(10_000_000L));
        when(certificateDetails7.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails7.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails7.getSubjectName()).thenReturn("CN=ggg_mock");
        when(certificateDetails7.getValidFrom()).thenReturn(createDate("2020-01-07 00:00:00"));
        when(certificateDetails7.getValidUntil()).thenReturn(createDate("2023-01-06 23:59:59"));
        list.add(certificateDetails7);

        final CertificateDetails certificateDetails8 = mock(CertificateDetails.class);
        when(certificateDetails8.getAlias()).thenReturn("hhh_mock");
        when(certificateDetails8.getIssuerName()).thenReturn("CN=hhh_mock");
        when(certificateDetails8.getSerialNumber()).thenReturn(String.valueOf(100_000_000L));
        when(certificateDetails8.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails8.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails8.getSubjectName()).thenReturn("CN=hhh_mock");
        when(certificateDetails8.getValidFrom()).thenReturn(createDate("2020-01-08 00:00:00"));
        when(certificateDetails8.getValidUntil()).thenReturn(createDate("2023-01-07 23:59:59"));
        list.add(certificateDetails8);

        final CertificateDetails certificateDetails9 = mock(CertificateDetails.class);
        when(certificateDetails9.getAlias()).thenReturn("iii_mock");
        when(certificateDetails9.getIssuerName()).thenReturn("CN=iii_mock");
        when(certificateDetails9.getSerialNumber()).thenReturn(String.valueOf(1000_000_000L));
        when(certificateDetails9.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails9.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails9.getSubjectName()).thenReturn("CN=iii_mock");
        when(certificateDetails9.getValidFrom()).thenReturn(createDate("2020-01-09 00:00:00"));
        when(certificateDetails9.getValidUntil()).thenReturn(createDate("2023-01-08 23:59:59"));
        list.add(certificateDetails9);

        final CertificateDetails certificateDetails10 = mock(CertificateDetails.class);
        when(certificateDetails10.getAlias()).thenReturn("jjj_mock");
        when(certificateDetails10.getIssuerName()).thenReturn("CN=jjj_mock");
        when(certificateDetails10.getSerialNumber()).thenReturn(new BigInteger("17593798617727720249").toString(10));
        when(certificateDetails10.getSignatureAlgorithm()).thenReturn("SHA512withRSA");
        when(certificateDetails10.getSubjectAltNames()).thenReturn(List.of());
        when(certificateDetails10.getSubjectName()).thenReturn("CN=jjj_mock");
        when(certificateDetails10.getValidFrom()).thenReturn(createDate("2020-01-10 00:00:00"));
        when(certificateDetails10.getValidUntil()).thenReturn(createDate("2023-01-09 23:59:59"));
        list.add(certificateDetails10);

        return list;
    }

    protected static Connection createAMQPConnection(int number, String hostname, String principal)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Connection.class));
        AMQPConnection<?> connection = mock(AMQPConnection.class);
        when(connection.getAttributeNames()).thenReturn(attributeNames);
        final UUID id = UUID.randomUUID();
        when(connection.getAttribute(Connection.ID)).thenReturn(id);
        when(connection.getId()).thenReturn(id);
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

        final Session session = createSession(0);
        when(connection.getSessions()).thenReturn(List.of(session));
        when(connection.getChildren(eq(Session.class))).thenReturn(List.of(session));

        return connection;
    }

    protected static Consumer<?, ?> createConsumer(int connectionNumber, String queueName)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Consumer.class));
        final Consumer<?, ?> consumer = mock(Consumer.class);
        when(consumer.getAttributeNames()).thenReturn(attributeNames);
        when(consumer.getAttribute(Consumer.ID)).thenReturn(UUID.randomUUID());
        when(consumer.getAttribute(Consumer.NAME)).thenReturn(
            String.format("%d|1|qpid-jms:receiver:ID:%s:1:1:1:%s", connectionNumber, UUID.randomUUID(), queueName)
        );
        final Session session = (Session) connections.get(connectionNumber).getSessions().iterator().next();
        when(consumer.getSession()).thenReturn(session);
        when(consumer.getAttribute("session")).thenReturn(session);
        return consumer;
    }

    protected static Session<?> createSession(int sessionNumber)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Session.class));
        final Session<?> session = mock(Session.class);
        when(session.getAttributeNames()).thenReturn(attributeNames);
        final UUID id = UUID.randomUUID();
        when(session.getAttribute(Session.ID)).thenReturn(id);
        when(session.getId()).thenReturn(id);
        when(session.getAttribute(Session.NAME)).thenReturn(sessionNumber);
        when(session.getName()).thenReturn(String.valueOf(sessionNumber));
        when(session.getType()).thenReturn("Session");
        when(session.getDescription()).thenReturn("test description");
        when(session.getDesiredState()).thenReturn(State.ACTIVE);
        when(session.getState()).thenReturn(State.ACTIVE);
        when(session.isDurable()).thenReturn(true);
        when(session.getLifetimePolicy()).thenReturn(LifetimePolicy.PERMANENT);
        when(session.getChannelId()).thenReturn(0);
        when(session.getLastOpenedTime()).thenReturn(new Date());
        when(session.isProducerFlowBlocked()).thenReturn(false);
        when(session.getLastUpdatedTime()).thenReturn(new Date());
        when(session.getLastUpdatedBy()).thenReturn("admin");
        when(session.getCreatedBy()).thenReturn("admin");
        when(session.getCreatedTime()).thenReturn(new Date());
        when(session.getStatistics()).thenReturn(new HashMap<>());
        return session;
    }

    protected static Map<String, Object> getBrokerStatistics()
    {
        final Map<String, Object> map = new HashMap<>();
        map.put("usedHeapMemorySize", 6_000_000_000L);
        map.put("usedDirectMemorySize", 500_000_000L);
        map.put("processCpuLoad", 0.052);
        return map;
    }

    protected static Date createDate(String date)
    {
        final LocalDateTime dateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"));
        return Date.from(dateTime.toInstant(ZoneOffset.UTC));
    }
}
