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
package org.apache.qpid.tests.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class TransactionTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "10.tx.commit",
            description = "This command commits all messages published and accepted in the current transaction.")
    public void messageSendCommit() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(UTF_8);
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(sessionName)
                       .tx().selectId(0).select()
                       .message()
                       .transferDestination(BrokerAdmin.TEST_QUEUE_NAME)
                       .transferId(1)
                       .transfer()
                       .session()
                       .flushCompleted()
                       .flush();

            SessionCompleted completed;
            do
            {
                completed = interaction.consumeResponse().getLatestResponse(SessionCompleted.class);
            }
            while (!completed.getCommands().includes(1));

            int queueDepthMessages = getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(queueDepthMessages, is(equalTo(0)));

            interaction.tx().commitId(2).commit()
                       .session()
                       .flushCompleted()
                       .flush();

            completed = interaction.consumeResponse().getLatestResponse(SessionCompleted.class);
            assertThat(completed.getCommands().includes(2), is(equalTo(true)));

            queueDepthMessages = getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(queueDepthMessages, is(equalTo(1)));
        }
    }
}
