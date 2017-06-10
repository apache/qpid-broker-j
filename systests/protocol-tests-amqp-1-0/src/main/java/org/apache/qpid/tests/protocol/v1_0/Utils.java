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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;

import org.hamcrest.core.Is;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class Utils
{
    public static boolean doesNodeExist(final InetSocketAddress brokerAddress,
                                        final String nodeAddress) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            transport.doBeginSession();

            final boolean queueExists;
            Attach validationAttach = new Attach();
            validationAttach.setName("validationAttach");
            validationAttach.setHandle(UnsignedInteger.ZERO);
            validationAttach.setRole(Role.RECEIVER);
            Source validationSource = new Source();
            validationSource.setAddress(nodeAddress);
            validationAttach.setSource(validationSource);
            validationAttach.setTarget(new Target());
            transport.sendPerformative(validationAttach);
            PerformativeResponse validationResponse = (PerformativeResponse) transport.getNextResponse();
            assertThat(validationResponse, is(notNullValue()));
            assertThat(validationResponse.getFrameBody(), is(instanceOf(Attach.class)));
            final Attach attachValidationResponse = (Attach) validationResponse.getFrameBody();
            if (attachValidationResponse.getSource() != null)
            {
                queueExists = true;
                Detach validationDetach = new Detach();
                validationDetach.setHandle(validationAttach.getHandle());
                validationDetach.setClosed(true);
                transport.sendPerformative(validationDetach);
                PerformativeResponse validationDetachResponse = (PerformativeResponse) transport.getNextResponse();
                assertThat(validationDetachResponse, is(notNullValue()));
                assertThat(validationDetachResponse.getFrameBody(), is(instanceOf(Detach.class)));
            }
            else
            {
                queueExists = false;
                PerformativeResponse validationDetachResponse = (PerformativeResponse) transport.getNextResponse();
                assertThat(validationDetachResponse, is(notNullValue()));
                assertThat(validationDetachResponse.getFrameBody(), is(instanceOf(Detach.class)));
            }
            return queueExists;
        }
    }

    public static Object receiveMessage(final InetSocketAddress brokerAddress,
                                        final String queueName) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            transport.doAttachReceivingLink(queueName);
            Flow flow = new Flow();
            flow.setIncomingWindow(UnsignedInteger.ONE);
            flow.setNextIncomingId(UnsignedInteger.ZERO);
            flow.setOutgoingWindow(UnsignedInteger.ZERO);
            flow.setNextOutgoingId(UnsignedInteger.ZERO);
            flow.setHandle(UnsignedInteger.ZERO);
            flow.setLinkCredit(UnsignedInteger.ONE);

            transport.sendPerformative(flow);

            MessageDecoder messageDecoder = new MessageDecoder();
            boolean hasMore;
            do
            {
                PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();
                assertThat(response, Is.is(notNullValue()));
                assertThat(response.getFrameBody(), Is.is(instanceOf(Transfer.class)));
                Transfer responseTransfer = (Transfer) response.getFrameBody();
                messageDecoder.addTransfer(responseTransfer);
                hasMore = Boolean.TRUE.equals(responseTransfer.getMore());
            }
            while (hasMore);

            return messageDecoder.getData();
        }
    }
}
