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
package org.apache.qpid.server.protocol.v1_0;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.SocketAddress;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class AMQPConnection_1_0ImplTest extends UnitTestBase
{
    private Broker<?> _broker;
    private ServerNetworkConnection _network;
    private AmqpPort _port;
    private AggregateTicker _aggregateTicket;
    private QueueManagingVirtualHost<?> _virtualHost;

    @Before
    public void setUp() throws Exception
    {
        _broker = BrokerTestHelper.createBrokerMock();
        final Model model = _broker.getModel();
        final TaskExecutor taskExecutor = _broker.getTaskExecutor();
        _network = mock(ServerNetworkConnection.class);
        when(_network.getLocalAddress()).thenReturn(mock(SocketAddress.class));
        _port = mock(AmqpPort.class);
        when(_port.getModel()).thenReturn(model);
        when(_port.getTaskExecutor()).thenReturn(taskExecutor);
        when(_port.getChildExecutor()).thenReturn(taskExecutor);
        _aggregateTicket = mock(AggregateTicker.class);
        _virtualHost = BrokerTestHelper.createVirtualHost("test", _broker, true, this);
    }

    @Test
    public void testGetOpenTransactions()
    {
        final AMQPConnection_1_0Impl connection = new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP, 0, _aggregateTicket);
        connection.setAddressSpace(_virtualHost);
        final IdentifiedTransaction tx1 = connection.createIdentifiedTransaction();
        final IdentifiedTransaction tx2 = connection.createIdentifiedTransaction();

        final Iterator<ServerTransaction> iterator = connection.getOpenTransactions();

        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(equalTo(tx1.getServerTransaction())));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(equalTo(tx2.getServerTransaction())));
        assertThat(iterator.hasNext(), is(false));
    }
}