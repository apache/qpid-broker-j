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

package org.apache.qpid.tests.protocol.v1_0.transaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declare;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declared;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionErrors;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.PerformativeResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;

public class DischargeTest extends ProtocolTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "4.3",
            description = "If the coordinator is unable to complete the discharge, the coordinator MUST convey the error to the controller "
                          + "as a transaction-error. If the source for the link to the coordinator supports the rejected outcome, then the "
                          + "message MUST be rejected with this outcome carrying the transaction-error.")
    public void dischargeUnknownTransactionIdWhenSourceSupportsRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ZERO;
            final Attach attach = new Attach();
            attach.setName("testSendingLink");
            attach.setHandle(linkHandle);
            attach.setRole(Role.SENDER);
            attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
            Source source = new Source();
            source.setOutcomes(Rejected.REJECTED_SYMBOL);
            attach.setSource(source);

            Coordinator target = new Coordinator();
            attach.setTarget(target);
            transport.doAttachSendingLink(attach);

            final Binary txnId = declareTransaction(transport, linkHandle);
            assertThat(txnId, is(notNullValue()));

            PerformativeResponse flowResponse =  transport.getNextResponse();
            assertThat(flowResponse, is(notNullValue()));
            assertThat(flowResponse.getBody(), is(instanceOf(Flow.class)));

            dischargeTransaction(transport, linkHandle, new Binary("nonExistingTransaction".getBytes(UTF_8)));

            PerformativeResponse dischargeResponse =  transport.getNextResponse();

            assertThat(dischargeResponse, is(notNullValue()));
            assertThat(dischargeResponse.getBody(), is(instanceOf(Disposition.class)));
            Disposition dischargeDisposition = (Disposition) dischargeResponse.getBody();
            assertThat(dischargeDisposition.getState(), is(instanceOf(Rejected.class)));
            final Error error = ((Rejected) dischargeDisposition.getState()).getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(TransactionErrors.UNKNOWN_ID)));
        }
    }

    @Test
    @SpecificationTest(section = "4.3",
            description = "If the coordinator is unable to complete the discharge, the coordinator MUST convey the error to the controller "
                          + "as a transaction-error. [...] If the source does not support "
                          + "the rejected outcome, the transactional resource MUST detach the link to the coordinator, with the detach "
                          + "performative carrying the transaction-error.")
    public void dischargeUnknownTransactionIdWhenSourceDoesNotSupportRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ZERO;
            final Attach attach = new Attach();
            attach.setName("testSendingLink");
            attach.setHandle(linkHandle);
            attach.setRole(Role.SENDER);
            attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
            Source source = new Source();
            source.setOutcomes(Accepted.ACCEPTED_SYMBOL);
            attach.setSource(source);

            Coordinator target = new Coordinator();
            attach.setTarget(target);
            transport.doAttachSendingLink(attach);

            final Binary txnId = declareTransaction(transport, linkHandle);
            assertThat(txnId, is(notNullValue()));

            PerformativeResponse flowResponse =  transport.getNextResponse();
            assertThat(flowResponse, is(notNullValue()));
            assertThat(flowResponse.getBody(), is(instanceOf(Flow.class)));

            dischargeTransaction(transport, linkHandle, new Binary("nonExistingTransaction".getBytes(UTF_8)));

            PerformativeResponse detachResponse = transport.getNextResponse();

            assertThat(detachResponse, is(notNullValue()));
            assertThat(detachResponse.getBody(), is(instanceOf(Detach.class)));
            Error error = ((Detach) detachResponse.getBody()).getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(TransactionErrors.UNKNOWN_ID)));
        }
    }

    private void dischargeTransaction(final FrameTransport transport,
                                      final UnsignedInteger linkHandle,
                                      final Binary txnId) throws Exception
    {
        Transfer dischargeTransactionTransfer = new Transfer();
        dischargeTransactionTransfer.setDeliveryId(UnsignedInteger.ONE);
        dischargeTransactionTransfer.setDeliveryTag(new Binary("discharge".getBytes(UTF_8)));
        dischargeTransactionTransfer.setHandle(linkHandle);
        final Discharge discharge = new Discharge();
        discharge.setTxnId(txnId);
        setPayload(discharge, dischargeTransactionTransfer);
        transport.sendPerformative(dischargeTransactionTransfer);
    }

    private Binary declareTransaction(final FrameTransport transport, final UnsignedInteger linkHandle) throws Exception
    {
        Transfer declareTransactionTransfer = new Transfer();
        declareTransactionTransfer.setDeliveryId(UnsignedInteger.ZERO);
        declareTransactionTransfer.setDeliveryTag(new Binary("declare".getBytes(UTF_8)));
        declareTransactionTransfer.setHandle(linkHandle);
        setPayload(new Declare(), declareTransactionTransfer);
        transport.sendPerformative(declareTransactionTransfer);

        PerformativeResponse declareResponse =  transport.getNextResponse();

        assertThat(declareResponse, is(notNullValue()));
        assertThat(declareResponse.getBody(), is(instanceOf(Disposition.class)));
        Disposition disposition = (Disposition) declareResponse.getBody();
        assertThat(disposition.getState(), is(instanceOf(Declared.class)));
        assertThat(disposition.getSettled(), is(equalTo(true)));
        return ((Declared) disposition.getState()).getTxnId();
    }

    private void setPayload(final Object payload, final Transfer transfer)
    {
        AmqpValue amqpValue = new AmqpValue(payload);
        final AmqpValueSection section = amqpValue.createEncodingRetainingSection();
        final List<QpidByteBuffer> encodedForm = section.getEncodedForm();
        transfer.setPayload(encodedForm);
        section.dispose();

        for (QpidByteBuffer qbb: encodedForm)
        {
            qbb.dispose();
        }
    }
}
