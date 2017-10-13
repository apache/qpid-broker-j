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

package org.apache.qpid.tests.protocol.v1_0;

import static org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError.DECODE_ERROR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.StringWriter;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class DecodeErrorTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "3.2",
            description = "Altogether a message consists of the following sections: Zero or one header,"
                          + " Zero or one delivery-annotations, [...]")
    public void illegalMessageFormatPayload() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {

            List<QpidByteBuffer> payloads = new ArrayList<>();
            final HeaderSection headerSection = new Header().createEncodingRetainingSection();
            payloads.add(headerSection.getEncodedForm());
            headerSection.dispose();
            final StringWriter stringWriter = new StringWriter("string in between annotation sections");
            QpidByteBuffer encodedString = QpidByteBuffer.allocate(stringWriter.getEncodedSize());
            stringWriter.writeToBuffer(encodedString);
            encodedString.flip();
            payloads.add(encodedString);
            final DeliveryAnnotationsSection
                    deliveryAnnotationsSection =
                    new DeliveryAnnotations(Collections.emptyMap()).createEncodingRetainingSection();
            payloads.add(deliveryAnnotationsSection.getEncodedForm());
            deliveryAnnotationsSection.dispose();

            final Detach detachResponse;
            try (QpidByteBuffer combinedPayload = QpidByteBuffer.concatenate(payloads))
            {
                detachResponse = transport.newInteraction()
                                          .negotiateProtocol().consumeResponse()
                                          .open().consumeResponse(Open.class)
                                          .begin().consumeResponse(Begin.class)
                                          .attachRole(Role.SENDER)
                                          .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                          .attach().consumeResponse(Attach.class)
                                          .consumeResponse(Flow.class)
                                          .transferMessageFormat(UnsignedInteger.ZERO)
                                          .transferPayload(combinedPayload)
                                          .transfer()
                                          .consumeResponse()
                                          .getLatestResponse(Detach.class);
            }
            payloads.forEach(QpidByteBuffer::dispose);
            assertThat(detachResponse.getError(), is(notNullValue()));
            assertThat(detachResponse.getError().getCondition(), is(equalTo(DECODE_ERROR)));
        }
    }

    @Test
    @SpecificationTest(section = "3.5.9",
            description = "The value of this entry MUST be of a type which provides the lifetime-policy archetype.")
    public void nodePropertiesLifetimePolicy() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Source source = new Source();
            source.setDynamic(Boolean.TRUE);
            source.setDynamicNodeProperties(Collections.singletonMap(Symbol.valueOf("lifetime-policy"),
                                                                     UnsignedInteger.MAX_VALUE));
            final Response<?> latestResponse = transport.newInteraction()
                                                        .negotiateProtocol().consumeResponse()
                                                        .open().consumeResponse(Open.class)
                                                        .begin().consumeResponse(Begin.class)
                                                        .attachSource(source)
                                                        .attachRole(Role.SENDER)
                                                        .attach().consumeResponse()
                                                        .getLatestResponse();

            assertThat(latestResponse, is(notNullValue()));
            final Object responseBody = latestResponse.getBody();
            final Error error;
            if (responseBody instanceof End)
            {
                error = ((End) responseBody).getError();
            }
            else if (responseBody instanceof Close)
            {
                error = ((Close) responseBody).getError();
            }
            else
            {
                fail(String.format("Expected response of either Detach, End, or Close. Got '%s'", responseBody));
                error = null;
            }

            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(DECODE_ERROR)));
        }
    }

    @Test
    @SpecificationTest(section = "3.5.9",
            description = "The value of this entry MUST be of a type which provides the lifetime-policy archetype.")
    public void nodePropertiesSupportedDistributionModes() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Target target = new Target();
            target.setDynamic(Boolean.TRUE);
            target.setDynamicNodeProperties(Collections.singletonMap(Symbol.valueOf("supported-dist-modes"),
                                                                     UnsignedInteger.ZERO));
            final Response<?> latestResponse = transport.newInteraction()
                                                        .negotiateProtocol().consumeResponse()
                                                        .open().consumeResponse(Open.class)
                                                        .begin().consumeResponse(Begin.class)
                                                        .attachTarget(target)
                                                        .attachRole(Role.SENDER)
                                                        .attach().consumeResponse()
                                                        .getLatestResponse();

            assertThat(latestResponse, is(notNullValue()));
            final Object responseBody = latestResponse.getBody();
            final Error error;
            if (responseBody instanceof End)
            {
                error = ((End) responseBody).getError();
            }
            else if (responseBody instanceof Close)
            {
                error = ((Close) responseBody).getError();
            }
            else
            {
                fail(String.format("Expected response of either Detach, End, or Close. Got '%s'", responseBody));
                error = null;
            }

            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(DECODE_ERROR)));
        }
    }
}
