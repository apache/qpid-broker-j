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
package org.apache.qpid.server.store.berkeleydb.replication;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.systests.JmsProvider;
import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;
import org.apache.qpid.tests.utils.RunBrokerAdmin;

@RunBrokerAdmin(type = "BDB-HA")
public class GroupJmsTestBase extends JmsTestBase
{
    private static final int FAILOVER_CYCLECOUNT = 40;
    private static final int FAILOVER_CONNECTDELAY = 1000;
    static final int SHORT_FAILOVER_CYCLECOUNT = 2;
    static final int SHORT_FAILOVER_CONNECTDELAY = 200;

    @Override
    public GroupBrokerAdmin getBrokerAdmin()
    {
        return (GroupBrokerAdmin) super.getBrokerAdmin();
    }

    @Override
    public ConnectionBuilder getConnectionBuilder()
    {
        final ConnectionBuilder connectionBuilder = getJmsProvider().getConnectionBuilder()
                                                                    .setClientId(getTestName())
                                                                    .setFailoverReconnectDelay(FAILOVER_CONNECTDELAY)
                                                                    .setFailoverReconnectAttempts(FAILOVER_CYCLECOUNT)
                                                                    .setVirtualHost("test")
                                                                    .setFailover(true)
                                                                    .setHost(getBrokerAdmin().getHost());
        int[] ports = getBrokerAdmin().getGroupAmqpPorts();
        for (int i = 0; i < ports.length; i++)
        {
            int port = ports[i];
            if (i == 0)
            {
                connectionBuilder.setPort(port);
            }
            else
            {
                connectionBuilder.addFailoverPort(port);
            }
        }
        return connectionBuilder;
    }

    protected void assertProduceConsume(final Queue queue) throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            assertThat(Utils.produceConsume(connection, queue), is(equalTo(true)));
        }
        finally
        {
            connection.close();
        }
    }


    protected JmsProvider getJmsProvider()
    {
        return Utils.getJmsProvider();
    }

    protected Queue createTestQueue(final Connection connection) throws JMSException
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            return getJmsProvider().createQueue(session, getTestName());
        }
        finally
        {
            session.close();
        }
    }

}
