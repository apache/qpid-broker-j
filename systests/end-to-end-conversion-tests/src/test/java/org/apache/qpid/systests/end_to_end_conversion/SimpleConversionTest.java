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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientInstruction;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientMessage;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientResult;
import org.apache.qpid.systests.end_to_end_conversion.client.MessageDescription;
import org.apache.qpid.systests.end_to_end_conversion.client.SerializableTestClass;
import org.apache.qpid.systests.end_to_end_conversion.client.VerificationException;

public class SimpleConversionTest extends EndToEndConversionTestBase
{
    private static final long TEST_TIMEOUT = 30000L;
    private static final String QUEUE_JNDI_NAME = "queue";
    private static final EnumSet<Protocol> AMQP_PRE010_PROTOCOLS =
            EnumSet.of(Protocol.AMQP_0_9, Protocol.AMQP_0_9_1, Protocol.AMQP_0_8);
    private static final String JMS_MESSAGE_IDPOLICY_MESSAGE_IDTYPE = "jms.messageIDPolicy.messageIDType";

    private HashMap<String, String> _defaultDestinations;
    @Rule
    public TestName _testName = new TestName();

    @Before
    public void setup()
    {
        final String queueName = _testName.getMethodName();
        getBrokerAdmin().createQueue(queueName);

        _defaultDestinations = new HashMap<>();
        _defaultDestinations.put("queue." + QUEUE_JNDI_NAME, queueName);
    }

    @Test
    public void message() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setMessageType(MessageDescription.MessageType.MESSAGE);
        performSimpleTest(messageDescription);
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
    public void streamMessage() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setMessageType(MessageDescription.MessageType.STREAM_MESSAGE);
        messageDescription.setContent(Lists.newArrayList(true,
                                                         (byte) -7,
                                                         (short) 259,
                                                         Integer.MAX_VALUE,
                                                         Long.MAX_VALUE,
                                                         37.5f,
                                                         38.5,
                                                         "testString",
                                                         null,
                                                         new byte[]{0x24, 0x00, (byte) 0xFF}));

        performSimpleTest(messageDescription);
    }

    @Test
    public void mapMessage() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setMessageType(MessageDescription.MessageType.MAP_MESSAGE);
        HashMap<String, Object> content = new HashMap<>();
        content.put("boolean", true);
        content.put("byte", (byte) -7);
        content.put("short", (short) 259);
        content.put("int", 42);
        content.put("long", Long.MAX_VALUE);
        content.put("float", 37.5f);
        content.put("double", 37.5);
        content.put("string", "testString");
        content.put("byteArray", new byte[] {0x24 , 0x00, (byte) 0xFF});

        messageDescription.setContent(content);

        performSimpleTest(messageDescription);
    }

    @Test
    public void objectMessage() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setMessageType(MessageDescription.MessageType.OBJECT_MESSAGE);
        messageDescription.setContent(new SerializableTestClass(Collections.singletonMap("testKey", "testValue"),
                                                                Collections.singletonList(42)));

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
        final byte[] correlationId = new byte[]{(byte) 0xFF, 0x00, (byte) 0xC3};
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setHeader(MessageDescription.MessageHeader.CORRELATION_ID, correlationId);

        performSimpleTest(messageDescription);
    }

    @Test
    public void providerAssignedMessageId09_010() throws Exception
    {
        assumeTrue(AMQP_PRE010_PROTOCOLS.contains(getPublisherProtocolVersion())
                   && EnumSet.of(Protocol.AMQP_0_10).contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.emptyMap());

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // Subscriber receives 0-10 UUID message-id.  Qpid JMS 0-x library synthesizes the ID: prefix
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(publishedMessage.getJMSMessageID()));
    }

    @Test
    public void providerAssignedMessageId09_10() throws Exception
    {
        assumeTrue(AMQP_PRE010_PROTOCOLS.contains(getPublisherProtocolVersion())
                   && EnumSet.of(Protocol.AMQP_1_0).contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.emptyMap());

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        String publishedJmsMessageID = publishedMessage.getJMSMessageID();
        assertThat(publishedJmsMessageID, startsWith("ID:"));
        String barePublishedJmsMessageID = publishedJmsMessageID.substring("ID:".length());
        String expectedSubscriberJmsMessageID = String.format("ID:AMQP_UUID:%s", barePublishedJmsMessageID);
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(expectedSubscriberJmsMessageID));
    }

    @Test
    public void providerAssignedMessageId010_09() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_0_10).contains(getPublisherProtocolVersion())
                   && AMQP_PRE010_PROTOCOLS.contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.emptyMap());

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // Subscriber receives AMQShortString message-id with a ID: prefix.  The conversion layer already synthesizes
        // this. See MessageConverter_0_10_to_0_8.java:130
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(publishedMessage.getJMSMessageID()));
    }

    @Test
    public void providerAssignedMessageId010_10() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_0_10).contains(getPublisherProtocolVersion())
                   && EnumSet.of(Protocol.AMQP_1_0).contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.emptyMap());

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // Publisher sends a 0-10 UUID message-id. This is converted into message-id-uuid.  The Qpid JMS
        // Client returns a ID:AMQP_UUID:
        final String publishedJmsMessageID = publishedMessage.getJMSMessageID();
        assertThat(publishedJmsMessageID, startsWith("ID:"));
        String barePublishedJmsMessageID = publishedJmsMessageID.substring("ID:".length());
        String expectedSubscriberJmsMessageID = String.format("ID:AMQP_UUID:%s", barePublishedJmsMessageID);
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(expectedSubscriberJmsMessageID));
    }

    @Test
    public void providerAssignedMessageId_DefaultMode_10_09() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion())
                   && AMQP_PRE010_PROTOCOLS.contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.emptyMap());

        // On the wire the "message-id-string comprises an identity of the publisher + a message sequence number
        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        assertThat(subscriberMessage.getJMSMessageID(), equalTo(publishedMessage.getJMSMessageID()));
    }

    @Test
    public void providerAssignedMessageId_UuidMode_10_09() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion())
                   && AMQP_PRE010_PROTOCOLS
                           .contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.singletonMap(
                JMS_MESSAGE_IDPOLICY_MESSAGE_IDTYPE, "UUID"));

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // On the wire the message id is a AMQP 1.0 UUID
        final String publishedJmsMessageID = publishedMessage.getJMSMessageID();
        assertThat(publishedJmsMessageID, startsWith("ID:AMQP_UUID:"));
        String barePublishedJmsMessageID = publishedJmsMessageID.substring("ID:AMQP_UUID:".length());
        String expectedSubscriberJmsMessageID = String.format("%s", barePublishedJmsMessageID);
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(expectedSubscriberJmsMessageID));
    }

    @Test
    public void providerAssignedMessageId_UuidStringMode_10_09() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion())
                   && AMQP_PRE010_PROTOCOLS.contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.singletonMap(
                JMS_MESSAGE_IDPOLICY_MESSAGE_IDTYPE, "UUID_STRING"));

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // On the wire the message-id is a string containing a UUID with no prefix
        final String publishedJmsMessageID = publishedMessage.getJMSMessageID();
        assertThat(publishedJmsMessageID, startsWith("ID:AMQP_NO_PREFIX:"));
        String barePublishedJmsMessageID = publishedJmsMessageID.substring("ID:AMQP_NO_PREFIX:".length());
        String expectedSubscriberJmsMessageID = String.format("%s", barePublishedJmsMessageID);
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(expectedSubscriberJmsMessageID));

    }

    @Test
    public void providerAssignedMessageId_PrefixedUuidStringMode_10_09() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion())
                   && AMQP_PRE010_PROTOCOLS.contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.singletonMap(
                JMS_MESSAGE_IDPOLICY_MESSAGE_IDTYPE, "PREFIXED_UUID_STRING"));

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // On the wire the message-id is a message-id-string containing a UUID with ID: prefix
        final String publishedJmsMessageID = publishedMessage.getJMSMessageID();
        assertThat(publishedJmsMessageID, startsWith("ID:"));
        String barePublishedJmsMessageID = publishedJmsMessageID.substring("ID:".length());
        String expectedSubscriberJmsMessageID = String.format("ID:%s", barePublishedJmsMessageID);
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(expectedSubscriberJmsMessageID));
    }

    @Test
    public void providerAssignedMessageId_DefaultMode_10_010() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion())
                   && EnumSet.of(Protocol.AMQP_0_10).contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.emptyMap());

        // On the wire the message-id is a message-id-string comprising an identity of the pubisher + a message
        // sequence number
        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // Conversion layer manufactures an UUID.  This will be unpredictable to the client.
        assertThat(subscriberMessage.getJMSMessageID(), is(notNullValue()));
    }

    @Test
    public void providerAssignedMessageId_UuidMode_10_010() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion())
                   && EnumSet.of(Protocol.AMQP_0_10).contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.singletonMap(
                JMS_MESSAGE_IDPOLICY_MESSAGE_IDTYPE, "UUID"));

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // On the wire the message-id is a message-id-uuid
        final String publishedJmsMessageID = publishedMessage.getJMSMessageID();
        assertThat(publishedJmsMessageID, startsWith("ID:AMQP_UUID:"));
        String barePublishedJmsMessageID = publishedJmsMessageID.substring("ID:AMQP_UUID:".length());
        String expectedSubscriberJmsMessageID = String.format("ID:%s", barePublishedJmsMessageID);
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(expectedSubscriberJmsMessageID));
    }

    @Test
    public void providerAssignedMessageId_UuidStringMode_10_010() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion())
                   && EnumSet.of(Protocol.AMQP_0_10).contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.singletonMap(
                JMS_MESSAGE_IDPOLICY_MESSAGE_IDTYPE, "UUID_STRING"));

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // On the wire the message-id is a message-id-string containing a UUID without prefix
        final String publishedJmsMessageID = publishedMessage.getJMSMessageID();
        assertThat(publishedJmsMessageID, startsWith("ID:AMQP_NO_PREFIX:"));
        String barePublishedJmsMessageID = publishedJmsMessageID.substring("ID:AMQP_NO_PREFIX:".length());
        String expectedSubscriberJmsMessageID = String.format("ID:%s", barePublishedJmsMessageID);
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(expectedSubscriberJmsMessageID));
    }

    @Test
    public void providerAssignedMessageId_PrefixedUuidStringMode_10_010() throws Exception
    {
        assumeTrue(EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion())
                   && EnumSet.of(Protocol.AMQP_0_10).contains(getSubscriberProtocolVersion()));

        List<ClientMessage> clientResults = performProviderAssignedMessageIdTest(Collections.singletonMap(
                JMS_MESSAGE_IDPOLICY_MESSAGE_IDTYPE, "PREFIXED_UUID_STRING"));

        ClientMessage publishedMessage = clientResults.get(0);
        ClientMessage subscriberMessage = clientResults.get(1);

        // On the wire the message-id is a message-id-string containing a UUID with ID: prefix
        final String publishedJmsMessageID = publishedMessage.getJMSMessageID();
        assertThat(publishedJmsMessageID, startsWith("ID:"));
        String barePublishedJmsMessageID = publishedJmsMessageID.substring("ID:".length());
        String expectedSubscriberJmsMessageID = String.format("ID:%s", barePublishedJmsMessageID);
        assertThat(subscriberMessage.getJMSMessageID(), equalTo(expectedSubscriberJmsMessageID));
    }

    @Test
    public void property() throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();
        messageDescription.setProperty("booleanProperty", true);
        messageDescription.setProperty("byteProperty", (byte) -7);
        messageDescription.setProperty("shortProperty", (short) 259);
        messageDescription.setProperty("intProperty", 42);
        messageDescription.setProperty("longProperty", Long.MAX_VALUE);
        messageDescription.setProperty("floatProperty", 37.5f);
        messageDescription.setProperty("doubleProperty", 37.5);
        messageDescription.setProperty("stringProperty", "foobar");

        performSimpleTest(messageDescription);
    }

    @Test
    public void replyToStaticQueue() throws Exception
    {
        final String replyQueueName = _testName.getMethodName() + "ReplyQueue";
        final String replyQueueJndiName = "replyQueue";
        _defaultDestinations.put("queue." + replyQueueJndiName, replyQueueName);
        getBrokerAdmin().createQueue(replyQueueName);
        performReplyToTest(replyQueueJndiName);
    }

    @Test
    public void replyToTemporaryQueue() throws Exception
    {
        performReplyToTest(TEMPORARY_QUEUE_JNDI_NAME);
    }

    @Test
    public void replyToAmqp10Topic() throws Exception
    {
        assumeTrue("This test is for AMQP 1.0 publisher",
                    EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion()));

        final String replyTopicJndiName = "replyTopic";
        _defaultDestinations.put("topic." + replyTopicJndiName, "amq.topic/topic");
        performReplyToTest(replyTopicJndiName);
    }

    @Test
    public void replyToAmqp0xTopic() throws Exception
    {
        assumeFalse("This test is for AMQP 0-x publisher",
                    EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion()));

        String jndiName = "testTopic";
        _defaultDestinations.put("topic." + jndiName, "myTopic");
        performReplyToTest(jndiName);
    }

    @Test
    public void replyToBURLDestination() throws Exception
    {
        assumeFalse("This test is for AMQP 0-x publisher",
                   EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion()));

        String jndiName = "testDestination";
        String testDestination = _testName.getMethodName() + "MyQueue";
        _defaultDestinations.put("destination." + jndiName,
                                 String.format("BURL:direct://amq.direct//%s?routingkey='%s'", testDestination, testDestination));

        getBrokerAdmin().createQueue(testDestination);

        performReplyToTest(jndiName);
    }

    @Test
    public void replyToAddressDestination() throws Exception
    {
        assumeFalse("This test is for AMQP 0-x publisher",
                    EnumSet.of(Protocol.AMQP_1_0).contains(getPublisherProtocolVersion()));

        assumeTrue("QPID-7902: setJMSReplyTo for address based destination is broken on client side for 0-8...0-9-1",
                    EnumSet.of(Protocol.AMQP_0_10).contains(getPublisherProtocolVersion()));

        String replyToJndiName = "replyToJndiName";
        String consumeReplyToJndiName = "consumeReplyToJndiName";
        String testDestination = _testName.getMethodName() + "MyQueue";
        _defaultDestinations.put("destination." + replyToJndiName, "ADDR: amq.fanout/testReplyToQueue");
        _defaultDestinations.put("destination." + consumeReplyToJndiName,
                                 "ADDR: testReplyToQueue; {create:always, node: {type: queue, x-bindings:[{exchange: 'amq.fanout', key: testReplyToQueue}]}}");

        getBrokerAdmin().createQueue(testDestination);

        performReplyToTest(replyToJndiName, consumeReplyToJndiName);
    }

    private void performReplyToTest(final String jndiName) throws Exception
    {
        performReplyToTest(jndiName, null);
    }

    private void performReplyToTest(final String replyToJndiName, final String consumeReplyToJndiName) throws Exception
    {
        final String correlationId = "testCorrelationId";
        final String destinationJndiName = QUEUE_JNDI_NAME;

        final List<ClientInstruction>
                publisherInstructions = new ClientInstructionBuilder().configureDestinations(_defaultDestinations)
                                                                      .publishMessage(destinationJndiName)
                                                                      .withReplyToJndiName(replyToJndiName)
                                                                      .withConsumeReplyToJndiName(consumeReplyToJndiName)
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

    private List<ClientResult> performSimpleTest(final MessageDescription messageDescription) throws Exception
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
        return performTest(publisherInstructions,subscriberInstructions);
    }

    private List<ClientResult> performTest(final List<ClientInstruction> publisherInstructions,
                                           final List<ClientInstruction> subscriberInstructions) throws Exception
    {
        final ListenableFuture<ClientResult> publisherFuture = runPublisher(publisherInstructions);
        final ListenableFuture<ClientResult> subscriberFuture = runSubscriber(subscriberInstructions);
        try
        {
            return Futures.allAsList(publisherFuture, subscriberFuture).get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
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

    private List<ClientMessage> performProviderAssignedMessageIdTest(final Map<String, String> publisherConnectionUrlConfig) throws Exception
    {
        final MessageDescription messageDescription = new MessageDescription();

        final String destinationJndiName = QUEUE_JNDI_NAME;
        final List<ClientInstruction> publisherInstructions =
                new ClientInstructionBuilder().configureConnectionUrl(publisherConnectionUrlConfig)
                                              .configureDestinations(_defaultDestinations)
                                              .publishMessage(destinationJndiName, messageDescription)
                                              .build();
        final List<ClientInstruction> subscriberInstructions =
                new ClientInstructionBuilder().configureDestinations(_defaultDestinations)
                                              .receiveMessage(destinationJndiName, messageDescription)
                                              .build();
        List<ClientResult> clientResults = performTest(publisherInstructions, subscriberInstructions);
        assertThat("Unexpected number of client results", clientResults.size(), equalTo(2));

        ClientResult publishedClientResult = clientResults.get(0);
        assertThat("Unexpected number of published client messages", publishedClientResult.getClientMessages().size(), equalTo(1));
        ClientMessage publishedMessage = publishedClientResult.getClientMessages().get(0);

        ClientResult subscriberClientResults = clientResults.get(1);
        assertThat("Unexpected number of published client messages", subscriberClientResults.getClientMessages().size(), equalTo(1));
        ClientMessage subscriberMessage = subscriberClientResults.getClientMessages().get(0);

        return Arrays.asList(publishedMessage, subscriberMessage);
    }
}
