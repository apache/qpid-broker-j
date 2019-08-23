/*
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
package org.apache.qpid.tests.protocol.v1_0.transport.security.sasl.extensions.qpid;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY, value = "500")
public class SaslAuthenticationTimeoutTest extends BrokerAdminUsingTestBase
{
    private static final Symbol PLAIN = Symbol.getSymbol("PLAIN");
    private static final byte[] SASL_AMQP_HEADER_BYTES = "AMQP\3\1\0\0".getBytes(StandardCharsets.UTF_8);

    @Before
    public void setUp()
    {
        assumeThat(getBrokerAdmin().isSASLSupported(), is(true));
        assumeThat(getBrokerAdmin().isSASLMechanismSupported(PLAIN.toString()), is(true));
    }

    @Test
    public void authenticationTimeout() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final byte[] saslHeaderResponse = interaction.protocolHeader(SASL_AMQP_HEADER_BYTES)
                                                         .negotiateProtocol().consumeResponse()
                                                         .getLatestResponse(byte[].class);
            assertThat(saslHeaderResponse, is(equalTo(SASL_AMQP_HEADER_BYTES)));

            SaslMechanisms mechanismsResponse = interaction.consumeResponse().getLatestResponse(SaslMechanisms.class);
            assertThat(Arrays.asList(mechanismsResponse.getSaslServerMechanisms()), hasItem(PLAIN));

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
