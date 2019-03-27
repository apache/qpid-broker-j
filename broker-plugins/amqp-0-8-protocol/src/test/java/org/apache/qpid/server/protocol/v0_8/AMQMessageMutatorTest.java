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
package org.apache.qpid.server.protocol.v0_8;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TestMemoryMessageStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class AMQMessageMutatorTest extends UnitTestBase
{
    private static final byte TEST_PRIORITY = (byte) 1;
    private static final String TEST_HEADER_NAME = "foo";
    private static final String TEST_HEADER_VALUE = "bar";
    private static final String TEST_CONTENT_TYPE = "text/plain";
    private static final String TEST_CONTENT = "testContent";
    private MessageStore _messageStore;
    private AMQMessageMutator _messageMutator;

    @Before
    public void setUp() throws Exception
    {
        _messageStore = new TestMemoryMessageStore();
        final AMQMessage message = createTestMessage();
        _messageMutator = new AMQMessageMutator(message, _messageStore);
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
    public void create()
    {
        _messageMutator.setPriority((byte) (TEST_PRIORITY + 1));

        AMQMessage newMessage = _messageMutator.create();

        assertThat(newMessage.getMessageHeader().getPriority(), is(equalTo((byte) (TEST_PRIORITY + 1))));
        assertThat(newMessage.getMessageHeader().getMimeType(), is(equalTo(TEST_CONTENT_TYPE)));
        assertThat(newMessage.getMessageHeader().getHeader(TEST_HEADER_NAME), is(equalTo(TEST_HEADER_VALUE)));

        QpidByteBuffer content = newMessage.getContent();

        final byte[] bytes = new byte[content.remaining()];
        content.copyTo(bytes);
        assertThat(new String(bytes, UTF_8), is(equalTo(TEST_CONTENT)));
    }

    private AMQMessage createTestMessage()
    {
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setPriority(TEST_PRIORITY);
        basicContentHeaderProperties.setHeaders(FieldTableFactory.createFieldTable(Collections.singletonMap(
                TEST_HEADER_NAME,
                TEST_HEADER_VALUE)));
        basicContentHeaderProperties.setContentType(TEST_CONTENT_TYPE);

        QpidByteBuffer content = QpidByteBuffer.wrap(TEST_CONTENT.getBytes(UTF_8));

        final ContentHeaderBody contentHeader = new ContentHeaderBody(basicContentHeaderProperties, content.remaining());
        final MessagePublishInfo publishInfo = new MessagePublishInfo(AMQShortString.valueOf("testExchange"),
                                                                      true,
                                                                      true,
                                                                      AMQShortString.valueOf("testRoutingKey"));
        final MessageMetaData messageMetaData =
                new MessageMetaData(publishInfo, contentHeader, System.currentTimeMillis());
        final MessageHandle<MessageMetaData> handle = _messageStore.addMessage(messageMetaData);
        handle.addContent(content);
        return new AMQMessage(handle.allContentAdded());
    }
}
