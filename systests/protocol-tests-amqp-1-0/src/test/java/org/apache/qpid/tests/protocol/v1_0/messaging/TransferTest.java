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

package org.apache.qpid.tests.protocol.v1_0.messaging;

import static org.apache.qpid.tests.protocol.v1_0.Matchers.protocolHeader;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.framing.TransportFrame;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.MessageEncoder;
import org.apache.qpid.tests.protocol.v1_0.PerformativeResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.Response;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;

public class TransferTest extends ProtocolTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "1.3.4",
            description = "Transfer without mandatory fields should result in a decoding error.")
    public void emptyTransfer() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress))
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ZERO;
            transport.doAttachSendingLink(linkHandle, BrokerAdmin.TEST_QUEUE_NAME);

            Transfer transfer = new Transfer();
            transport.sendPerformative(transfer, UnsignedShort.valueOf((short) 0));
            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Close.class)));
            Close responseClose = (Close) response.getFrameBody();
            assertThat(responseClose.getError(), is(notNullValue()));
            assertThat(responseClose.getError().getCondition(), equalTo(AmqpError.DECODE_ERROR));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.12",
            description = "Transfering A Message.")
    public void transfer() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress))
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ZERO;
            transport.doAttachSendingLink(linkHandle, BrokerAdmin.TEST_QUEUE_NAME);

            MessageEncoder messageEncoder = new MessageEncoder();
            messageEncoder.addData("foo");

            Transfer transfer = new Transfer();
            transfer.setHandle(linkHandle);
            transfer.setPayload(messageEncoder.getPayload());

            transport.sendPerformative(transfer);
            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Disposition.class)));
            Disposition responseDisposition = (Disposition) response.getFrameBody();
            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.TRUE));
            assertThat(responseDisposition.getState(), is(instanceOf(Accepted.class)));

            transport.assertNoMoreResponses();
        }
    }

    @Test
    @SpecificationTest(section = "", description = "Pipelined message send")
    public void presettledPipelined() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress))
        {
            byte[] protocolHeader = "AMQP\0\1\0\0".getBytes(StandardCharsets.UTF_8);
            Open open = new Open();
            open.setContainerId("testContainerId");

            Begin begin = new Begin();
            begin.setNextOutgoingId(UnsignedInteger.ZERO);
            begin.setIncomingWindow(UnsignedInteger.ZERO);
            begin.setOutgoingWindow(UnsignedInteger.ZERO);

            Attach attach = new Attach();
            attach.setName("testLink");
            final UnsignedInteger linkHandle = new UnsignedInteger(0);
            attach.setHandle(linkHandle);
            attach.setRole(Role.SENDER);
            attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
            Source source = new Source();
            attach.setSource(source);
            Target target = new Target();
            attach.setTarget(target);

            MessageEncoder messageEncoder = new MessageEncoder();
            messageEncoder.addData("foo");
            Transfer transfer = new Transfer();
            transfer.setHandle(linkHandle);
            transfer.setPayload(messageEncoder.getPayload());
            transfer.setSettled(Boolean.TRUE);

            Close close = new Close();

            final short channel = (short) 37;
            final ListenableFuture<Void> future = transport.sendPipelined(protocolHeader,
                                                                          new TransportFrame((short) 0, open),
                                                                          new TransportFrame(channel, begin),
                                                                          new TransportFrame(channel, attach),
                                                                          new TransportFrame(channel, transfer, transfer.getPayload()),
                                                                          new TransportFrame((short) 0, close));
            future.get(FrameTransport.RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);

            final Response response = transport.getNextResponse();
            assertThat(response, is(protocolHeader(protocolHeader)));

            final PerformativeResponse openResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(openResponse, is(notNullValue()));
            assertThat(openResponse.getFrameBody(), is(instanceOf(Open.class)));
            final PerformativeResponse beginResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(beginResponse, is(notNullValue()));
            assertThat(beginResponse.getFrameBody(), is(instanceOf(Begin.class)));
            final PerformativeResponse attachResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(attachResponse, is(notNullValue()));
            assertThat(attachResponse.getFrameBody(), is(instanceOf(Attach.class)));
            final PerformativeResponse flowResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(flowResponse, is(notNullValue()));
            assertThat(flowResponse.getFrameBody(), is(instanceOf(Flow.class)));
/*
            final PerformativeResponse dispositionResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(dispositionResponse, is(notNullValue()));
            assertThat(dispositionResponse.getFrameBody(), is(instanceOf(Disposition.class)));
            final PerformativeResponse detachResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(detachResponse, is(notNullValue()));
            assertThat(detachResponse.getFrameBody(), is(instanceOf(Detach.class)));
            final PerformativeResponse endResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(endResponse, is(notNullValue()));
            assertThat(endResponse.getFrameBody(), is(instanceOf(End.class)));
*/
            final PerformativeResponse closeResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(closeResponse, is(notNullValue()));
            assertThat(closeResponse.getFrameBody(), is(instanceOf(Close.class)));
        }
    }
}
