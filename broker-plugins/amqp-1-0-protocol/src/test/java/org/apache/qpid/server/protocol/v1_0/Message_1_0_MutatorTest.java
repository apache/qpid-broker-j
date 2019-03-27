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
package org.apache.qpid.server.protocol.v1_0;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationPropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TestMemoryMessageStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class Message_1_0_MutatorTest extends UnitTestBase
{
    private static final byte TEST_PRIORITY = (byte) 1;
    private static final String TEST_HEADER_NAME = "foo";
    private static final String TEST_HEADER_VALUE = "bar";
    private static final String TEST_CONTENT_TYPE = "text/plain";
    private static final String TEST_CONTENT = "testContent";
    private MessageStore _messageStore;
    private Message_1_0_Mutator _messageMutator;

    @Before
    public void setUp() throws Exception
    {
        _messageStore = new TestMemoryMessageStore();
        final Message_1_0 message = createTestMessage();
        _messageMutator = new Message_1_0_Mutator(message, _messageStore);
    }

    @After
    public void tearDown()
    {
        _messageStore.closeMessageStore();
    }

    @Test
    public void setPriority()
    {
        _messageMutator.setPriority((byte) (TEST_PRIORITY + 1));
        assertThat(_messageMutator.getPriority(), is(equalTo((byte) (TEST_PRIORITY + 1))));
    }

    @Test
    public void getPriority()
    {
        assertThat((int) _messageMutator.getPriority(), is(equalTo((int) TEST_PRIORITY)));
    }

    @Test
    public void create() throws Exception
    {
        _messageMutator.setPriority((byte) (TEST_PRIORITY + 1));

        final Message_1_0 newMessage = _messageMutator.create();

        assertThat(newMessage.getMessageHeader().getPriority(), is(equalTo((byte) (TEST_PRIORITY + 1))));
        assertThat(newMessage.getMessageHeader().getMimeType(), is(equalTo(TEST_CONTENT_TYPE)));
        assertThat(newMessage.getMessageHeader().getHeader(TEST_HEADER_NAME), is(equalTo(TEST_HEADER_VALUE)));

        final QpidByteBuffer content = newMessage.getContent();

        final SectionDecoderImpl sectionDecoder =
                new SectionDecoderImpl(MessageConverter_v1_0_to_Internal.TYPE_REGISTRY.getSectionDecoderRegistry());
        final List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(content);
        assertThat(sections.size(), is(equalTo(1)));

        final Object value = sections.get(0).getValue();
        assertThat(value, is(equalTo(TEST_CONTENT)));
    }

    private Message_1_0 createTestMessage()
    {
        final QpidByteBuffer content = new AmqpValue(TEST_CONTENT).createEncodingRetainingSection().getEncodedForm();
        final long contentSize = content.remaining();

        final Header header = new Header();
        header.setPriority(UnsignedByte.valueOf(TEST_PRIORITY));
        final HeaderSection headerSection = header.createEncodingRetainingSection();

        final Properties properties = new Properties();
        properties.setContentType(Symbol.valueOf(TEST_CONTENT_TYPE));
        final PropertiesSection propertiesSection = properties.createEncodingRetainingSection();

        final ApplicationPropertiesSection applicationPropertiesSection =
                new ApplicationProperties(Collections.singletonMap(TEST_HEADER_NAME, TEST_HEADER_VALUE))
                        .createEncodingRetainingSection();

        final MessageMetaData_1_0 mmd = new MessageMetaData_1_0(headerSection,
                                                                null,
                                                                null,
                                                                propertiesSection,
                                                                applicationPropertiesSection,
                                                                null,
                                                                System.currentTimeMillis(),
                                                                contentSize);

        final MessageHandle<MessageMetaData_1_0> handle = _messageStore.addMessage(mmd);
        handle.addContent(content);
        return new Message_1_0(handle.allContentAdded());
    }
}
