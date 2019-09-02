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
package org.apache.qpid.tests.protocol.v0_8.extension.authtimeout;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.util.Arrays;

import org.junit.Test;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY, value = "500")
public class AuthenticationTimeoutTest extends BrokerAdminUsingTestBase
{
    @Test
    public void authenticationTimeout() throws Exception
    {
        assumeThat(getBrokerAdmin().isSASLMechanismSupported("PLAIN"), is(true));

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final ConnectionStartBody start = interaction.negotiateProtocol()
                                                         .consumeResponse()
                                                         .getLatestResponse(ConnectionStartBody.class);

            assertThat(Arrays.asList(new String(start.getMechanisms()).split(" ")), hasItem("PLAIN"));

            interaction.connection()
                       .startOkMechanism("PLAIN")
                       .startOk()
                       .consumeResponse(ConnectionSecureBody.class);

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
