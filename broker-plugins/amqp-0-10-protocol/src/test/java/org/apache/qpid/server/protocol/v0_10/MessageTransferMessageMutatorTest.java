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
package org.apache.qpid.server.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TestMemoryMessageStore;
import org.apache.qpid.test.utils.UnitTestBase;

class MessageTransferMessageMutatorTest extends UnitTestBase
{
    private static final short TEST_PRIORITY = (short) 1;
    private static final String TEST_HEADER_NAME = "foo";
    private static final String TEST_HEADER_VALUE = "bar";
    private static final String TEST_CONTENT_TYPE = "text/plain";
    private static final String TEST_CONTENT = "testContent";

    private MessageStore _messageStore;
    private MessageTransferMessageMutator _messageMutator;

    @BeforeEach
    void setUp()
    {
        _messageStore = new TestMemoryMessageStore();
        final MessageTransferMessage message = createTestMessage();
        _messageMutator = new MessageTransferMessageMutator(message, _messageStore);
    }

    @AfterEach
    void tearDown()
    {
        _messageStore.closeMessageStore();
    }

    @Test
    void setPriority()
    {
        _messageMutator.setPriority((byte) (TEST_PRIORITY + 1));
        assertThat(_messageMutator.getPriority(), is(equalTo((byte) (TEST_PRIORITY + 1))));
    }

    @Test
    void getPriority()
    {
        assertThat((int) _messageMutator.getPriority(), is(equalTo((int) TEST_PRIORITY)));
    }

    @Test
    void create()
    {
        _messageMutator.setPriority((byte) (TEST_PRIORITY + 1));

        final MessageTransferMessage newMessage = _messageMutator.create();

        assertThat(newMessage.getMessageHeader().getPriority(), is(equalTo((byte) (TEST_PRIORITY + 1))));
        assertThat(newMessage.getMessageHeader().getMimeType(), is(equalTo(TEST_CONTENT_TYPE)));
        assertThat(newMessage.getMessageHeader().getHeader(TEST_HEADER_NAME), is(equalTo(TEST_HEADER_VALUE)));

        final QpidByteBuffer content = newMessage.getContent();

        final byte[] bytes = new byte[content.remaining()];
        content.copyTo(bytes);
        assertThat(new String(bytes, UTF_8), is(equalTo(TEST_CONTENT)));
    }

    private MessageTransferMessage createTestMessage()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setPriority(MessageDeliveryPriority.get(TEST_PRIORITY));
        final MessageProperties messageProperties = new MessageProperties();

        messageProperties.setContentType(TEST_CONTENT_TYPE);
        messageProperties.setApplicationHeaders(Map.of(TEST_HEADER_NAME, TEST_HEADER_VALUE));

        final Header header = new Header(deliveryProperties, messageProperties);
        final QpidByteBuffer content = QpidByteBuffer.wrap(TEST_CONTENT.getBytes(UTF_8));
        final MessageMetaData_0_10 messageMetaData =
                new MessageMetaData_0_10(header, content.remaining(), System.currentTimeMillis());
        final MessageHandle<MessageMetaData_0_10> addedMessage = _messageStore.addMessage(messageMetaData);
        addedMessage.addContent(content);
        return new MessageTransferMessage(addedMessage.allContentAdded(), null);
    }
}
