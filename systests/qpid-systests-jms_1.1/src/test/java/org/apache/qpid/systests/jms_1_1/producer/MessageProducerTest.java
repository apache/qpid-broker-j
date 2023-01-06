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

package org.apache.qpid.systests.jms_1_1.producer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.JmsTestBase;

public class MessageProducerTest extends JmsTestBase
{
    @Test
    public void produceToUnknownQueue() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_0_10))).matches(getProtocol()), "QPID-7818");

        Connection connection = getConnectionBuilder().setSyncPublish(true).build();

        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue invalidDestination = session.createQueue("unknown");

            try
            {
                MessageProducer sender = session.createProducer(invalidDestination);
                sender.send(session.createMessage());
                fail("Exception not thrown");
            }
            catch (InvalidDestinationException e)
            {
                //PASS
            }
            catch (JMSException e)
            {
                assertThat("Allowed for the Qpid JMS AMQP 0-x client",
                           getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));
                //PASS
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void anonymousSenderSendToUnknownQueue() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_0_10))).matches(getProtocol()), "QPID-7818");

        Connection connection = getConnectionBuilder().setSyncPublish(true).build();

        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue invalidDestination = session.createQueue("unknown");

            try
            {
                MessageProducer sender = session.createProducer(null);
                sender.send(invalidDestination, session.createMessage());
                fail("Exception not thrown");
            }
            catch (InvalidDestinationException e)
            {
                //PASS
            }
            catch (JMSException e)
            {
                assertThat("Allowed for the Qpid JMS AMQP 0-x client",
                           getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));
                //PASS
            }
        }
        finally
        {
            connection.close();
        }
    }
}
