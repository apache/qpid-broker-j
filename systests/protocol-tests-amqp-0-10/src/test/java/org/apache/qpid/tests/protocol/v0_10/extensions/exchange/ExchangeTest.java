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
package org.apache.qpid.tests.protocol.v0_10.extensions.exchange;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;

import org.junit.Test;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.tests.protocol.v0_10.FrameTransport;
import org.apache.qpid.tests.protocol.v0_10.Interaction;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class ExchangeTest extends BrokerAdminUsingTestBase
{
    private static final byte[] SESSION_NAME = "test".getBytes(UTF_8);

    @Test
    public void exchangeDeclareValidWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            SessionCompleted completed = interaction.negotiateOpen()
                                                    .channelId(1)
                                                    .attachSession(SESSION_NAME)
                                                    .exchange()
                                                    .declareExchange("test")
                                                    .declareArguments(Collections.singletonMap("unroutableMessageBehaviour", "REJECT"))
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
    public void exchangeDeclareInvalidWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .exchange()
                       .declareExchange("test")
                       .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                       .declareId(0)
                       .declareArguments(Collections.singletonMap("foo", "bar"))
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush();

            ExecutionException exception =
                    interaction.consume(ExecutionException.class, SessionCompleted.class, SessionCommandPoint.class);

            assertThat(exception.getErrorCode(), is(equalTo(ExecutionErrorCode.ILLEGAL_ARGUMENT)));
        }
    }

}
