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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionResult;
import org.apache.qpid.server.protocol.v0_10.transport.QueueQueryResult;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.server.protocol.v0_10.transport.SessionDetached;
import org.apache.qpid.server.protocol.v0_10.transport.SessionFlush;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class QueueTest extends BrokerAdminUsingTestBase
{
    private static final byte[] SESSION_NAME = "test".getBytes(UTF_8);

    @Test
    @SpecificationTest(section = "10.queue.declare", description = "This command creates or checks a queue.")
    public void queueDeclare() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            SessionCompleted completed = interaction.negotiateOpen()
                                                    .channelId(1)
                                                    .attachSession(SESSION_NAME)
                                                    .queue()
                                                    .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .declareId(0)
                                                    .declare()
                                                    .session()
                                                    .flushCompleted()
                                                    .flush()
                                                    .consumeResponse()
                                                    .getLatestResponse(SessionCompleted.class);

            assertThat(completed.getCommands().includes(0), is(equalTo(true)));
            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "The alternate-exchange field specifies how messages on this queue should be treated when "
                          + "they are rejected by a subscriber, or when they are orphaned by queue deletion.")
    public void queueDeclareWithAlternateExchange() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declareAlternateExchange("amq.direct")
                       .declareId(0)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "if the alternate-exchange does not match the name of any existing exchange on the server, "
                          + "then an exception must be raised.")
    public void queueDeclareAlternateExchangeNotFound() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .queue()
                                                     .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .declareAlternateExchange("unknownExchange")
                                                     .declareId(0)
                                                     .declare()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "The client MAY ask the server to assert that a queue exists without creating the queue if "
                          + "not.")
    public void queueDeclarePassive() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declarePassive(true)
                       .declareId(0)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse()
                       .getLatestResponse(SessionCompleted.class);
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "[...] If the queue does not exist, the server treats this as a failure.")
    public void queueDeclarePassiveQueueNotFound() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .queue()
                                                     .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .declarePassive(true)
                                                     .declareId(0)
                                                     .declare()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "If set when creating a new queue, the queue will be marked as durable. Durable queues "
                          + "remain active when a server restarts.")
    public void queueDeclareDurable() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declareId(0)
                       .declareDurable(true)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);
        }

        assumeThat(getBrokerAdmin().supportsRestart(), Matchers.is(true));
        getBrokerAdmin().restart();

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declarePassive(true)
                       .declareId(0)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse()
                       .getLatestResponse(SessionCompleted.class);
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "If the server receives a declare, bind, consume or get request for a queue that has been"
                          + "declared as exclusive by an existing client session, it MUST raise an exception.")
    public void queueDeclareAttemptedConsumeOfExclusivelyDeclaredQueue() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declareId(0)
                       .declareExclusive(true)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);

            try (FrameTransport transport2 = new FrameTransport(getBrokerAdmin()).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                ExecutionException response = interaction2.negotiateOpen()
                                                          .channelId(1)
                                                          .attachSession("test2".getBytes(UTF_8))
                                                          .message()
                                                          .subscribeDestination("mysub")
                                                          .subscribeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                          .subscribeId(0)
                                                          .subscribe()
                                                          .session()
                                                          .flushCompleted()
                                                          .flush()
                                                          .consumeResponse(SessionCommandPoint.class)
                                                          .consumeResponse()
                                                          .getLatestResponse(ExecutionException.class);

                assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.RESOURCE_LOCKED)));
            }
        }

        try (FrameTransport transport2 = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction2 = transport2.newInteraction();
            interaction2.negotiateOpen()
                        .channelId(1)
                        .attachSession("test2".getBytes(UTF_8))
                        .message()
                        .subscribeDestination("mysub")
                        .subscribeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                        .subscribeId(0)
                        .subscribe()
                        .session()
                        .flushCompleted()
                        .flush()
                        .consumeResponse(SessionCompleted.class);
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "If the server receives a declare, bind, consume or get request for a queue that has been"
                          + "declared as exclusive by an existing client session, it MUST raise an exception.")
    public void queueDeclareRedeclareOfExclusivelyDeclaredQueue() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declareId(0)
                       .declareExclusive(true)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);

            try (FrameTransport transport2 = new FrameTransport(getBrokerAdmin()).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                ExecutionException response = interaction2.negotiateOpen()
                                                          .channelId(1)
                                                          .attachSession("test2".getBytes(UTF_8))
                                                          .queue()
                                                          .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                          .declareId(0)
                                                          .declareExclusive(true)
                                                          .declare()
                                                          .session()
                                                          .flushCompleted()
                                                          .flush()
                                                          .consumeResponse(SessionCommandPoint.class)
                                                          .consumeResponse()
                                                          .getLatestResponse(ExecutionException.class);

                assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.RESOURCE_LOCKED)));
            }
        }

        try (FrameTransport transport2 = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction2 = transport2.newInteraction();
            interaction2.negotiateOpen()
                        .channelId(1)
                        .attachSession("test2".getBytes(UTF_8))
                        .queue()
                        .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                        .declareId(0)
                        .declareExclusive(true)
                        .declare()
                        .session()
                        .flushCompleted()
                        .flush()
                        .consumeResponse(SessionCompleted.class);
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "If this field [auto-delete] is set and the exclusive field is also set, then the queue "
                          + "MUST be deleted when the session closes.")
    public void queueDeclareAutoDeleteAndExclusiveDeletedBySessionDetach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declareId(0)
                       .declareExclusive(true)
                       .declareAutoDelete(true)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class)
                       .session()
                       .detachName(SESSION_NAME)
                       .detach()
                       .consumeResponse(SessionDetached.class);

            ExecutionException response = interaction.channelId(2)
                                                     .attachSession(SESSION_NAME)
                                                     .queue()
                                                     .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .declareId(0)
                                                     .declarePassive(true)
                                                     .declare()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.declare",
            description = "If this field is set and the exclusive field is not set the queue is deleted when all the "
                          + "consumers have finished using it. Last consumer can be cancelled either explicitly or "
                          + "because its session is closed.")
    public void queueDeclareAutoDeleteDeletedByLastConsumerCancelled() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declareId(0)
                       .declareAutoDelete(true)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);
        }

        try (FrameTransport transport2 = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction2 = transport2.newInteraction();
            String subscriberName = "mysub";
            interaction2.negotiateOpen()
                        .channelId(1)
                        .attachSession("test2".getBytes(UTF_8))
                        .queue()
                        .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                        .declareId(0)
                        .declarePassive(true)
                        .declare()
                        .message()
                        .subscribeDestination(subscriberName)
                        .subscribeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                        .subscribeId(1)
                        .subscribe()
                        .session()
                        .flushCompleted()
                        .flush()
                        .consumeResponse(SessionCompleted.class)
                        .message()
                        .cancelId(2)
                        .cancelDestination(subscriberName)
                        .cancel()
                        .session()
                        .flushCompleted()
                        .flush()
                        .consumeResponse(SessionCompleted.class);

            ExecutionException response = interaction2.queue()
                                                      .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                      .declareId(3)
                                                      .declarePassive(true)
                                                      .declare()
                                                      .session()
                                                      .flushCompleted()
                                                      .flush()
                                                      .consumeResponse(SessionCommandPoint.class)
                                                      .consumeResponse()
                                                      .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.delete", description = "This command deletes a queue.")
    public void queueDelete() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .deleteQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .deleteId(0)
                       .delete()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);

            ExecutionException response = interaction.queue()
                                                     .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .declarePassive(true)
                                                     .declareId(1)
                                                     .declare()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.delete",
            description = "The queue must exist. If the client attempts to delete a non-existing queue the server "
                          + "MUST raise an exception.")
    public void queueDeleteQueueNotFound() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .queue()
                                                     .deleteQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .deleteId(0)
                                                     .delete()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_FOUND)));
        }
    }
    @Test
    @SpecificationTest(section = "10.queue.delete",
            description = "If set, the server will only delete the queue if it has no consumers. If the queue has "
                          + "consumers the server does does not delete it but raises an exception instead.")
    public void queueDeleteQueueDeleteWithConsumer() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport consumerTransport = new FrameTransport(getBrokerAdmin()).connect();
             FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction consumerInteraction = consumerTransport.newInteraction();
            String subscriberName = "mysub";
            consumerInteraction.negotiateOpen()
                               .channelId(1)
                               .attachSession(SESSION_NAME)
                               .message()
                               .subscribeDestination(subscriberName)
                               .subscribeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                               .subscribeId(1)
                               .subscribe()
                               .session()
                               .flushCompleted()
                               .flush()
                               .consumeResponse(SessionCompleted.class);

            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession("test2".getBytes(UTF_8))
                                                     .queue()
                                                     .deleteQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .deleteId(0)
                                                     .deleteIfUnused(true)
                                                     .delete()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.PRECONDITION_FAILED)));

            consumerInteraction.message()
                               .cancelId(2)
                               .cancelDestination(subscriberName)
                               .cancel()
                               .session()
                               .flushCompleted()
                               .flush()
                               .consumeResponse(SessionCompleted.class);

            consumerInteraction.queue()
                               .deleteQueue(BrokerAdmin.TEST_QUEUE_NAME)
                               .deleteId(0)
                               .deleteIfUnused(true)
                               .delete()
                               .session()
                               .flushCompleted()
                               .flush()
                               .consumeResponse(SessionCompleted.class);
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.purge", description = "This command removes all messages from a queue.")
    public void queuePurge() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message");

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionResult result = interaction.negotiateOpen()
                                                .channelId(1)
                                                .attachSession(SESSION_NAME)
                                                .queue()
                                                .queryQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                .queryId(1)
                                                .query()
                                                .session()
                                                .flushCompleted()
                                                .flush()
                                                .consumeResponse(SessionCommandPoint.class)
                                                .consumeResponse().getLatestResponse(ExecutionResult.class);
            assertThat(((QueueQueryResult) result.getValue()).getMessageCount(), is(1L));

            interaction.queue()
                    .purgeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                    .purgeId(0)
                    .purge()
                    .session()
                    .flushCompleted()
                    .flush()
                    .consumeResponse(SessionFlush.class)
                    .consumeResponse(SessionCompleted.class);

            result = interaction.queue()
                                .queryQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                .queryId(1)
                                .query()
                                .session()
                                .flushCompleted()
                                .flush()
                                .consumeResponse(SessionCompleted.class)
                                .consumeResponse().getLatestResponse(ExecutionResult.class);
            assertThat(((QueueQueryResult) result.getValue()).getMessageCount(), is(0L));
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.purge",
            description = "The queue must exist. If the client attempts to purge a non-existing queue the server "
                          + "MUST raise an exception.")
    public void queuePurgeQueueNotFound() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .queue()
                                                     .purgeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .purgeId(0)
                                                     .purge()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "10.queue.query",
            description = "This command requests information about a queue.")
    public void queueQuery() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message");

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionResult result = interaction.negotiateOpen()
                                                        .channelId(1)
                                                        .attachSession(SESSION_NAME)
                                                        .queue()
                                                        .queryQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                        .queryId(0)
                                                        .query()
                                                        .session()
                                                        .flushCompleted()
                                                        .flush()
                                                        .consumeResponse(SessionCommandPoint.class)
                                                        .consumeResponse().getLatestResponse(ExecutionResult.class);
            QueueQueryResult queryResult = (QueueQueryResult) result.getValue();
            assertThat(queryResult.getQueue(), is(equalTo(BrokerAdmin.TEST_QUEUE_NAME)));
            assertThat(queryResult.getAlternateExchange(), is(nullValue()));
            assertThat(queryResult.getMessageCount(), is(1L));
        }
    }
}
