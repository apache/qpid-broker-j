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
package org.apache.qpid.tests.protocol.v0_8.extension.exchange;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;

import org.junit.Test;

import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class ExchangeTest extends BrokerAdminUsingTestBase
{
    private static final String TEST_EXCHANGE = "testExchange";

    @Test
    public void exchangeDeclareValidWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange()
                       .declareName(TEST_EXCHANGE)
                       .declareArguments(Collections.singletonMap("unroutableMessageBehaviour", "REJECT"))
                       .declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);

            ExchangeBoundOkBody response = interaction.exchange()
                                                      .boundExchangeName(TEST_EXCHANGE)
                                                      .bound()
                                                      .consumeResponse()
                                                      .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.NO_BINDINGS)));
        }
    }

    @Test
    public void exchangeDeclareInvalidWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionCloseBody response = interaction.negotiateOpen()
                                                      .channel()
                                                      .open()
                                                      .consumeResponse(ChannelOpenOkBody.class)
                                                      .exchange()
                                                      .declareName(TEST_EXCHANGE)
                                                      .declareArguments(Collections.singletonMap("foo", "bar"))
                                                      .declare()
                                                      .consumeResponse(ConnectionCloseBody.class)
                                                      .getLatestResponse(ConnectionCloseBody.class);

            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.INVALID_ARGUMENT)));
        }
    }
}
