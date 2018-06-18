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
package org.apache.qpid.tests.protocol.v1_0.extensions.qpid.queue;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;

import org.junit.Test;

import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class DeletionOfQueueWithLinksTest extends BrokerAdminUsingTestBase
{
    @Test
    public void deleteQueueWithAttachedSender() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateProtocol().consumeResponse()
                                                     .open().consumeResponse(Open.class)
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);
            assertThat(responseAttach.getRole(), is(Role.RECEIVER));

            Flow flow = interaction.consumeResponse(Flow.class).getLatestResponse(Flow.class);
            assertThat(flow.getLinkCredit().intValue(), is(greaterThan(1)));

            try
            {
                getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);
            }
            catch (IntegrityViolationException e)
            {
                // pass
            }

            interaction.detachClose(true).detach().consumeResponse().getLatestResponse(Detach.class);

            getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);
        }
    }

    @Test
    public void deleteQueueWithAttachedReceiver() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateProtocol()
                                                     .consumeResponse()
                                                     .open()
                                                     .consumeResponse(Open.class)
                                                     .begin()
                                                     .consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach()
                                                     .consumeResponse(Attach.class)
                                                     .getLatestResponse(Attach.class);

            assertThat(responseAttach.getRole(), is(Role.SENDER));

            try
            {
                getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);
            }
            catch (IntegrityViolationException e)
            {
                // pass
            }

            interaction.detachClose(true).detach().consumeResponse().getLatestResponse(Detach.class);

            getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);
        }
    }

}
