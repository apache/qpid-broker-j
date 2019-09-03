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
import static org.apache.qpid.server.filter.AMQPFilterTypes.JMS_SELECTOR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.Collections;

import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.v0_10.transport.ExchangeBoundResult;
import org.apache.qpid.server.protocol.v0_10.transport.ExchangeQueryResult;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionResult;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ExchangeTest extends BrokerAdminUsingTestBase
{
    private static final byte[] SESSION_NAME = "test".getBytes(UTF_8);

    @Test
    @SpecificationTest(section = "10.exchange.declare", description = "verify exchange exists, create if needed.")
    public void exchangeDeclare() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            SessionCompleted completed = interaction.negotiateOpen()
                                                    .channelId(1)
                                                    .attachSession(SESSION_NAME)
                                                    .exchange()
                                                    .declareExchange("myexch")
                                                    .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                                                    .declareId(0)
                                                    .declare()
                                                    .session()
                                                    .flushCompleted()
                                                    .flush()
                                                    .consumeResponse()
                                                    .getLatestResponse(SessionCompleted.class);

            assertThat(completed.getCommands().includes(0), is(equalTo(true)));
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.declare",
            description = "In the event that a message cannot be routed, this is the name of the exchange to which the"
                          + " message will be sent.")
    public void exchangeDeclareWithAlternateExchange() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .exchange()
                       .declareExchange("myexch")
                       .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                       .declareAlternateExchange(ExchangeDefaults.DIRECT_EXCHANGE_NAME)
                       .declareId(0)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.declare",
            description = "if the alternate-exchange does not match the name of any existing exchange on the server, "
                          + "then an exception must be raised.")
    public void exchangeDeclareAlternateExchangeNotFound() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .exchange()
                                                     .declareExchange("myexch")
                                                     .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
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
    @SpecificationTest(section = "10.exchange.declare",
            description = "If set [durable] when creating a new exchange, the exchange will be marked as durable. "
                          + "Durable exchanges remain active when a server restarts. ")
    public void exchangeDeclareDurable() throws Exception
    {
        String exchangeName = "myexch";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .exchange()
                       .declareExchange(exchangeName)
                       .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
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
                       .exchange()
                       .declareExchange(exchangeName)
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
    @SpecificationTest(section = "10.exchange.delete", description = "delete an exchange")
    public void exchangeDelete() throws Exception
    {
        String exchangeName = "myexch";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .exchange()
                       .declareExchange(exchangeName)
                       .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                       .declareId(0)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);

            interaction.exchange()
                       .deleteExchange(exchangeName)
                       .deleteId(1)
                       .delete()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);

            ExecutionResult result = interaction.exchange()
                                                .queryExchange(exchangeName)
                                                .queryId(2)
                                                .query()
                                                .session()
                                                .flushCompleted()
                                                .flush()
                                                .consumeResponse(SessionCommandPoint.class)
                                                .consumeResponse().getLatestResponse(ExecutionResult.class);
            ExchangeQueryResult queryResult = (ExchangeQueryResult) result.getValue();
            assertThat(queryResult.getNotFound(), is(equalTo(true)));
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.delete",
            description = "An exchange MUST NOT be deleted if it is in use as an alternate-exchange by a queue or by "
                          + "another exchange.")
    public void exchangeDeleteInUseAsAlternate() throws Exception
    {
        String exchangeName1 = "myexch1";
        String exchangeName2 = "myexch2";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .exchange()
                                                     .declareExchange(exchangeName1)
                                                     .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                                                     .declareId(0)
                                                     .declare()
                                                     .exchange()
                                                     .declareExchange(exchangeName2)
                                                     .declareAlternateExchange(exchangeName1)
                                                     .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                                                     .declareId(1)
                                                     .declare()
                                                     .exchange()
                                                     .deleteExchange(exchangeName1)
                                                     .deleteId(2)
                                                     .delete()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_ALLOWED)));
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.query", description = "request information about an exchange")
    public void exchangeQuery() throws Exception
    {
        String exchangeName = "myexch";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionResult result = interaction.negotiateOpen()
                                                .channelId(1)
                                                .attachSession(SESSION_NAME)
                                                .exchange()
                                                .declareId(0)
                                                .declareExchange(exchangeName)
                                                .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                                                .declare()
                                                .exchange()
                                                .queryId(1)
                                                .queryExchange(exchangeName)
                                                .query()
                                                .session()
                                                .flushCompleted()
                                                .flush()
                                                .consumeResponse(SessionCommandPoint.class)
                                                .consumeResponse().getLatestResponse(ExecutionResult.class);
            ExchangeQueryResult queryResult = (ExchangeQueryResult) result.getValue();
            assertThat(queryResult.getNotFound(), is(equalTo(false)));
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.bind",
            description = "This command binds a queue to an exchange."
                          + " Until a queue is bound it will not receive any messages.")
    public void exchangeBind() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .exchange()
                       .bindId(0)
                       .bindExchange("amq.direct")
                       .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .bindBindingKey("bk")
                       .bind()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);

            ExecutionResult execResult = interaction.exchange()
                                                    .boundId(2)
                                                    .boundExchange("amq.direct")
                                                    .boundQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .boundBindingKey("bk")
                                                    .bound()
                                                    .session()
                                                    .flushCompleted()
                                                    .flush()
                                                    .consumeResponse(SessionCommandPoint.class)
                                                    .consumeResponse()
                                                    .getLatestResponse(ExecutionResult.class);

            assertThat(execResult.getValue(), is(instanceOf(ExchangeBoundResult.class)));
            ExchangeBoundResult result = (ExchangeBoundResult) execResult.getValue();
            assertThat(result.getExchangeNotFound(), is(equalTo(false)));
            assertThat(result.getQueueNotFound(), is(equalTo(false)));
            assertThat(result.getKeyNotMatched(), is(equalTo(false)));
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.bind",
            description =
                    "A client MUST NOT be allowed to bind a non-existent and unnamed queue (i.e. empty queue name)"
                    + " to an exchange.")
    @Ignore("spec requires INVALID_ARGUMENT error but NOT_FOUND is returned")
    public void exchangeBindEmptyQueue() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .exchange()
                                                     .bindId(0)
                                                     .bindExchange("amq.direct")
                                                     .bindBindingKey("bk")
                                                     .bindQueue("")
                                                     .bind()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.INVALID_ARGUMENT)));
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.bind",
            description =
                    "A client MUST NOT be allowed to bind a non-existent and unnamed queue (i.e. empty queue name)"
                    + " to an exchange.")
    @Ignore("spec requires INVALID_ARGUMENT error but NOT_FOUND is returned")
    public void exchangeBindNonExistingQueue() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .exchange()
                                                     .bindId(0)
                                                     .bindExchange("amq.direct")
                                                     .bindBindingKey("bk")
                                                     .bindQueue("non-existing")
                                                     .bind()
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
    @SpecificationTest(section = "10.exchange.bind",
            description = "The name of the exchange MUST NOT be a blank or empty string.")
    public void exchangeBindEmptyExchange() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .exchange()
                                                     .bindId(0)
                                                     .bindExchange("")
                                                     .bindBindingKey("bk")
                                                     .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .bind()
                                                     .session()
                                                     .flushCompleted()
                                                     .flush()
                                                     .consumeResponse(SessionCommandPoint.class)
                                                     .consumeResponse()
                                                     .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.INVALID_ARGUMENT)));
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.bind",
            description = "The name of the exchange MUST NOT be a blank or empty string.")
    public void exchangeBindNonExistingExchange() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionException response = interaction.negotiateOpen()
                                                     .channelId(1)
                                                     .attachSession(SESSION_NAME)
                                                     .exchange()
                                                     .bindId(0)
                                                     .bindExchange("non-existing")
                                                     .bindBindingKey("bk")
                                                     .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .bind()
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
    @SpecificationTest(section = "10.exchange.bind",
            description = "A server MUST ignore duplicate bindings - that is, two or more bind commands with the"
                          + " same exchange, queue, and binding-key - without treating these as an error."
                          + " The value of the arguments used for the binding MUST NOT be altered"
                          + " by subsequent binding requests.")
    public void exchangeBindIgnoreDuplicates() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .exchange()
                       .bindId(0)
                       .bindExchange("amq.direct")
                       .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .bindBindingKey("bk")
                       .bind()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class)
                       .exchange()
                       .bindId(1)
                       .bindExchange("amq.direct")
                       .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .bindBindingKey("bk")
                       .bindArguments(Collections.singletonMap(JMS_SELECTOR.name(), "name='a'"))
                       .bind()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);

            ExecutionResult execResult = interaction.exchange()
                                                    .boundId(2)
                                                    .boundExchange("amq.direct")
                                                    .boundQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .boundBindingKey("bk")
                                                    .boundArguments(Collections.singletonMap(JMS_SELECTOR.name(),
                                                                                             "name='a'"))
                                                    .bound()
                                                    .session()
                                                    .flushCompleted()
                                                    .flush()
                                                    .consumeResponse(SessionCommandPoint.class)
                                                    .consumeResponse()
                                                    .getLatestResponse(ExecutionResult.class);

            assertThat(execResult.getValue(), is(instanceOf(ExchangeBoundResult.class)));
            ExchangeBoundResult result = (ExchangeBoundResult) execResult.getValue();
            assertThat(result.getExchangeNotFound(), is(equalTo(false)));
            assertThat(result.getQueueNotFound(), is(equalTo(false)));
            assertThat(result.getKeyNotMatched(), is(equalTo(false)));
            assertThat(result.getArgsNotMatched(), is(equalTo(true)));
        }
    }

    @Test
    @SpecificationTest(section = "10.exchange.bind",
            description = " Bindings between durable queues and durable exchanges are automatically durable and"
                          + "  the server MUST restore such bindings after a server restart.")
    public void exchangeBindMultipleBindings() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), Matchers.is(true));

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
                       .consumeResponse(SessionCompleted.class)
                       .exchange()
                       .bindId(1)
                       .bindExchange("amq.direct")
                       .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .bindBindingKey("bk")
                       .bind()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);
        }

        getBrokerAdmin().restart();

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionResult execResult = interaction.negotiateOpen()
                                                    .channelId(1)
                                                    .attachSession(SESSION_NAME)
                                                    .exchange()
                                                    .boundId(0)
                                                    .boundExchange("amq.direct")
                                                    .boundQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .boundBindingKey("bk")
                                                    .bound()
                                                    .session()
                                                    .flushCompleted()
                                                    .flush()
                                                    .consumeResponse(SessionCommandPoint.class)
                                                    .consumeResponse()
                                                    .getLatestResponse(ExecutionResult.class);

            assertThat(execResult.getValue(), is(instanceOf(ExchangeBoundResult.class)));
            ExchangeBoundResult result = (ExchangeBoundResult) execResult.getValue();
            assertThat(result.getExchangeNotFound(), is(equalTo(false)));
            assertThat(result.getQueueNotFound(), is(equalTo(false)));
            assertThat(result.getKeyNotMatched(), is(equalTo(false)));
        }
    }

    @Test
    @SpecificationTest(section = "11.exchange.unbind",
            description = "This command unbinds a queue from an exchange.")
    public void exchangeUnbind() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .exchange()
                       .bindId(0)
                       .bindExchange("amq.direct")
                       .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .bindBindingKey("bk")
                       .bind()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class)
                       .exchange()
                       .unbindId(1)
                       .unbindExchange("amq.direct")
                       .unbindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .unbindBindingKey("bk")
                       .unbind()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse(SessionCompleted.class);

            ExecutionResult execResult = interaction.exchange()
                                                    .boundId(2)
                                                    .boundExchange("amq.direct")
                                                    .boundQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .boundBindingKey("bk")
                                                    .bound()
                                                    .session()
                                                    .flushCompleted()
                                                    .flush()
                                                    .consumeResponse(SessionCommandPoint.class)
                                                    .consumeResponse()
                                                    .getLatestResponse(ExecutionResult.class);

            assertThat(execResult.getValue(), is(instanceOf(ExchangeBoundResult.class)));
            ExchangeBoundResult result = (ExchangeBoundResult) execResult.getValue();
            assertThat(result.getExchangeNotFound(), is(equalTo(false)));
            assertThat(result.getQueueNotFound(), is(equalTo(false)));
            assertThat(result.getKeyNotMatched(), is(equalTo(true)));
        }
    }

    @Test
    @SpecificationTest(section = "11.exchange.unbind",
            description = "This command unbinds a queue from an exchange.")
    public void exchangeUnbindWithoutBindingKey() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final ExecutionException response = interaction.negotiateOpen()
                                                           .channelId(1)
                                                           .attachSession(SESSION_NAME)
                                                           .exchange()
                                                           .bindId(0)
                                                           .bindExchange("amq.direct")
                                                           .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                           .bindBindingKey("bk")
                                                           .bind()
                                                           .session()
                                                           .flushCompleted()
                                                           .flush()
                                                           .consumeResponse(SessionCompleted.class)
                                                           .exchange()
                                                           .unbindId(1)
                                                           .unbindExchange("amq.direct")
                                                           .unbindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                           .unbind()
                                                           .session()
                                                           .flushCompleted()
                                                           .flush()
                                                           .consumeResponse(SessionCommandPoint.class)
                                                           .consumeResponse()
                                                           .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.ILLEGAL_ARGUMENT)));
        }
    }

    @Test
    @SpecificationTest(section = "11.exchange.unbind",
            description = "If the queue does not exist the server MUST raise an exception.")
    public void exchangeUnbindNonExistingQueue() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final ExecutionException response = interaction.negotiateOpen()
                                                           .channelId(1)
                                                           .attachSession(SESSION_NAME)
                                                           .exchange()
                                                           .bindId(0)
                                                           .bindExchange("amq.direct")
                                                           .bindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                           .bindBindingKey("bk")
                                                           .bind()
                                                           .session()
                                                           .flushCompleted()
                                                           .flush()
                                                           .consumeResponse(SessionCompleted.class)
                                                           .exchange()
                                                           .unbindId(1)
                                                           .unbindExchange("amq.direct")
                                                           .unbindQueue("not-existing")
                                                           .unbindBindingKey("bk")
                                                           .unbind()
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
    @SpecificationTest(section = "11.exchange.unbind",
            description = "If the exchange does not exist the server MUST raise an exception.")
    public void exchangeUnbindNonExistingExchange() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final ExecutionException response = interaction.negotiateOpen()
                                                           .channelId(1)
                                                           .attachSession(SESSION_NAME)
                                                           .exchange()
                                                           .unbindId(0)
                                                           .unbindExchange("not-existing")
                                                           .unbindQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                           .unbindBindingKey(BrokerAdmin.TEST_QUEUE_NAME)
                                                           .unbind()
                                                           .session()
                                                           .flushCompleted()
                                                           .flush()
                                                           .consumeResponse(SessionCommandPoint.class)
                                                           .consumeResponse()
                                                           .getLatestResponse(ExecutionException.class);

            assertThat(response.getErrorCode(), is(equalTo(ExecutionErrorCode.NOT_FOUND)));
        }
    }
}
