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
import static org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError.INVALID_FIELD;
import static org.apache.qpid.tests.protocol.v1_0.ProtocolAsserts.assertAttachError;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.StringWriter;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class DecodeErrorTest extends BrokerAdminUsingTestBase
{
    @Test
    @SpecificationTest(section = "3.2",
            description = "Altogether a message consists of the following sections: Zero or one header,"
                          + " Zero or one delivery-annotations, [...]")
    public void illegalMessage() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin()
                       .consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach()
                       .consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class,
                                             flow -> assumeThat(flow.getLinkCredit(),
                                                                is(greaterThan(UnsignedInteger.ZERO))));

            try(final QpidByteBuffer payload = buildInvalidMessage())
            {
                interaction.transferMessageFormat(UnsignedInteger.ZERO)
                           .transferPayload(payload)
                           .transferSettled(true)
                           .transferMessageFormat(UnsignedInteger.ZERO)
                           .transfer()
                           .sync();
            }

            interaction.closeUnconditionally();
        }

        final String validMessage = getTestName() + "_2";
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, validMessage);
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(validMessage)));
    }

    @Test
    @SpecificationTest(section = "3.5.9",
            description = "Node Properties [...] lifetime-policy [...] "
                          + "The value of this entry MUST be of a type which provides the lifetime-policy archetype.")
    public void nodePropertiesLifetimePolicy() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setDynamic(Boolean.TRUE);
            source.setDynamicNodeProperties(Collections.singletonMap(Symbol.valueOf("lifetime-policy"),
                                                                     UnsignedInteger.MAX_VALUE));
            final Interaction interaction = transport.newInteraction()
                                                        .negotiateOpen()
                                                        .begin().consumeResponse(Begin.class)
                                                        .attachSource(source)
                                                        .attachRole(Role.RECEIVER)
                                                        .attach()
                                                        .sync();

            assertAttachError(interaction, DECODE_ERROR, INVALID_FIELD);
        }
    }

    @Test
    @SpecificationTest(section = "3.5.9",
            description = "Node Properties [...] supported-dist-modes [...] "
                          + "The value of this entry MUST be of a type which provides the lifetime-policy archetype.")
    public void nodePropertiesSupportedDistributionModes() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Target target = new Target();
            target.setDynamic(Boolean.TRUE);
            target.setDynamicNodeProperties(Collections.singletonMap(Symbol.valueOf("supported-dist-modes"),
                                                                     UnsignedInteger.ZERO));
            final Interaction interaction = transport.newInteraction()
                                                        .negotiateOpen()
                                                        .begin().consumeResponse(Begin.class)
                                                        .attachTarget(target)
                                                        .attachRole(Role.SENDER)
                                                        .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                        .attach().sync();
            assertAttachError(interaction, DECODE_ERROR, INVALID_FIELD);
        }
    }

    private QpidByteBuffer buildInvalidMessage()
    {
        final List<QpidByteBuffer> payloads = new ArrayList<>();
        try
        {
            final Header header = new Header();
            header.setTtl(UnsignedInteger.valueOf(10000L));
            final HeaderSection headerSection = header.createEncodingRetainingSection();
            try
            {
                payloads.add(headerSection.getEncodedForm());
            }
            finally
            {
                headerSection.dispose();
            }

            final StringWriter stringWriter = new StringWriter("string in between message sections");
            final QpidByteBuffer encodedString = QpidByteBuffer.allocate(stringWriter.getEncodedSize());
            stringWriter.writeToBuffer(encodedString);
            encodedString.flip();
            payloads.add(encodedString);

            final Map<Symbol, Object> annotationMap = Collections.singletonMap(Symbol.valueOf("foo"), "bar");
            final DeliveryAnnotations annotations = new DeliveryAnnotations(annotationMap);
            final DeliveryAnnotationsSection deliveryAnnotationsSection = annotations.createEncodingRetainingSection();
            try
            {
                payloads.add(deliveryAnnotationsSection.getEncodedForm());
            }
            finally
            {
                deliveryAnnotationsSection.dispose();
            }

            final AmqpValueSection payload = new AmqpValue(getTestName()).createEncodingRetainingSection();
            try
            {
                payloads.add(payload.getEncodedForm());
            }
            finally
            {
                payload.dispose();
            }

            return QpidByteBuffer.concatenate(payloads);
        }
        finally
        {
            payloads.forEach(QpidByteBuffer::dispose);
        }
    }

}
