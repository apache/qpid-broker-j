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

package org.apache.qpid.systests.end_to_end_conversion;

import static org.junit.Assume.assumeTrue;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientInstruction;
import org.apache.qpid.systests.end_to_end_conversion.client.MessageDescription;
import org.apache.qpid.systests.end_to_end_conversion.client.VerificationException;

public class SimpleConversionTest extends EndToEndConversionTestBase
{
    private static final long TEST_TIMEOUT = 30000L;
    public static final String QUEUE_NAME = "testQueue";
    public static final String REPLY_QUEUE_NAME = "testReplyQueue";
    private static final String QUEUE_JNDI_NAME = "queue";
    private static final String REPLY_QUEUE_JNDI_NAME = "replyQueue";


    private HashMap<String, String> _defaultDestinations;

    @Before
    public void setup()
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
        getBrokerAdmin().createQueue(REPLY_QUEUE_NAME);

        _defaultDestinations = new HashMap<>();
        _defaultDestinations.put("queue." + QUEUE_JNDI_NAME, QUEUE_NAME);
        _defaultDestinations.put("queue." + REPLY_QUEUE_JNDI_NAME, REPLY_QUEUE_NAME);
/*
        destinations.put("topic.topic", "testTopic");
        destinations.put("topic.replyTopic", "testReplyTopic");
        destinations.put("destination.destination", "testDestination");
        destinations.put("destination.replyDestination", "testReplyDestination");
*/
    }

    @Test
    public void textMessage() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setMessageType(MessageDescription.MessageType.TEXT_MESSAGE);
        messageDescription.setContent("foobar");

        performSimpleTest(messageDescription);
    }

    @Test
    public void bytesMessage() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setMessageType(MessageDescription.MessageType.BYTES_MESSAGE);
        messageDescription.setContent(new byte[]{0x00, (byte) 0xFF, (byte) 0xc3});

        performSimpleTest(messageDescription);
    }

    @Test
    public void mapMessage() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setMessageType(MessageDescription.MessageType.MAP_MESSAGE);
        HashMap<String, Object> content = new HashMap<>();
        content.put("int", 42);
        content.put("boolean", true);
        content.put("string", "testString");
        messageDescription.setContent(content);

        performSimpleTest(messageDescription);
    }

    @Test
    public void type() throws Exception
    {
        final String type = "testType";
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setHeader(MessageDescription.MessageHeader.TYPE, type);

        performSimpleTest(messageDescription);
    }

    @Test
    public void correlationId() throws Exception
    {
        final String correlationId = "myCorrelationId";
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setHeader(MessageDescription.MessageHeader.CORRELATION_ID, correlationId);

        performSimpleTest(messageDescription);
    }

    @Test
    public void correlationIdAsBytes() throws Exception
    {
        assumeTrue("This test is known to fail for pre 0-10 subscribers (QPID-7897)",
                   EnumSet.of(Protocol.AMQP_0_10, Protocol.AMQP_1_0).contains(getSubscriberProtocolVersion()));
        assumeTrue("This test is known to fail for pre 0-10 subscribers (QPID-7899)",
                   EnumSet.of(Protocol.AMQP_0_10, Protocol.AMQP_1_0).contains(getPublisherProtocolVersion()));

        final byte[] correlationId = new byte[]{(byte) 0xFF, 0x00, (byte) 0xC3};
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setHeader(MessageDescription.MessageHeader.CORRELATION_ID, correlationId);

        performSimpleTest(messageDescription);
    }

    @Test
    public void property() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setProperty("intProperty", 42);
        messageDescription.setProperty("stringProperty", "foobar");
        messageDescription.setProperty("booleanProperty", true);
        messageDescription.setProperty("doubleProperty", 37.5);

        performSimpleTest(messageDescription);
    }

    @Test
    public void replyTo() throws Exception
    {
        performReplyToTest(REPLY_QUEUE_JNDI_NAME);
    }

    @Test
    public void replyToTemporaryQueue() throws Exception
    {
        performReplyToTest(TEMPORARY_QUEUE_JNDI_NAME);
    }

    public void performReplyToTest(final String temporaryQueueJndiName) throws Exception
    {
        assumeTrue("This test is known to fail for pre 0-10 subscribers (QPID-7898)",
                   EnumSet.of(Protocol.AMQP_0_10, Protocol.AMQP_1_0).contains(getSubscriberProtocolVersion()));

        final String correlationId = "testCorrelationId";
        final String destinationJndiName = QUEUE_JNDI_NAME;

        final List<ClientInstruction>
                publisherInstructions = new ClientInstructionBuilder().configureDestinations(_defaultDestinations)
                                                                      .publishMessage(destinationJndiName)
                                                                      .withReplyToJndiName(
                                                                                                    temporaryQueueJndiName)
                                                                      .withHeader(MessageDescription.MessageHeader.CORRELATION_ID,
                                                                                  correlationId)
                                                                      .build();
        final List<ClientInstruction> subscriberInstructions = new ClientInstructionBuilder().configureDestinations(_defaultDestinations)
                                                                                             .receiveMessage(destinationJndiName)
                                                                                             .withHeader(MessageDescription.MessageHeader.CORRELATION_ID,
                                                                                                         correlationId)
                                                                                             .build();
        performTest(publisherInstructions, subscriberInstructions);
    }

    public void performSimpleTest(final MessageDescription messageDescription) throws Exception
    {
        final String destinationJndiName = QUEUE_JNDI_NAME;
        final List<ClientInstruction> publisherInstructions =
                new ClientInstructionBuilder().configureDestinations(_defaultDestinations)
                                              .publishMessage(destinationJndiName, messageDescription)
                                              .build();
        final List<ClientInstruction> subscriberInstructions =
                new ClientInstructionBuilder().configureDestinations(_defaultDestinations)
                                              .receiveMessage(destinationJndiName, messageDescription)
                                              .build();
        performTest(publisherInstructions,subscriberInstructions);
    }

    public void performTest(final List<ClientInstruction> publisherInstructions,
                            final List<ClientInstruction> subscriberInstructions) throws Exception
    {
        final ListenableFuture<?> publisherFuture = runPublisher(publisherInstructions);
        final ListenableFuture<?> subscriberFuture = runSubscriber(subscriberInstructions);
        try
        {
            Futures.allAsList(publisherFuture, subscriberFuture).get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e)
        {
            final Throwable cause = e.getCause();
            if (cause instanceof VerificationException)
            {
                throw new AssertionError("Client failed verification", cause);
            }
            else if (cause instanceof Exception)
            {
                throw ((Exception) cause);
            }
            else
            {
                throw e;
            }
        }
    }
}
