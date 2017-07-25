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
package org.apache.qpid.server.protocol.converter.v0_8_v1_0;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Data;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.typedmessage.TypedBytesContentReader;
import org.apache.qpid.server.util.ByteBufferUtils;
import org.apache.qpid.test.utils.QpidTestCase;

public class MessageConverter_1_0_to_v0_8Test extends QpidTestCase
{
    private final MessageConverter_1_0_to_v0_8 _converter = new MessageConverter_1_0_to_v0_8();
    private final StoredMessage<MessageMetaData_1_0> _handle = mock(StoredMessage.class);

    private final MessageMetaData_1_0 _metaData = mock(MessageMetaData_1_0.class);
    private final MessageMetaData_1_0.MessageHeader_1_0 _header = mock(MessageMetaData_1_0.MessageHeader_1_0.class);

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        when(_handle.getMetaData()).thenReturn(_metaData);
        when(_metaData.getMessageHeader()).thenReturn(_header);
    }

    public void testConvertStringMessageBody() throws Exception
    {
        final String expected = "helloworld";

        final AmqpValue value = new AmqpValue(expected);
        configureMessageContent(value.createEncodingRetainingSection());
        final Message_1_0 sourceMessage = new Message_1_0(_handle);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        assertEquals("text/plain", convertedMessage.getMessageHeader().getMimeType());

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals(expected.getBytes(), getBytes(content));
    }

    public void testConvertListMessageBody() throws Exception
    {
        final List<Object> expected = Lists.<Object>newArrayList("helloworld", 43, 1L);

        final AmqpValue value = new AmqpValue(expected);
        configureMessageContent(value.createEncodingRetainingSection());
        final Message_1_0 sourceMessage = new Message_1_0(_handle);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        assertEquals("jms/stream-message", convertedMessage.getMessageHeader().getMimeType());

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        TypedBytesContentReader reader = new TypedBytesContentReader(ByteBuffer.wrap(getBytes(content)));
        assertEquals(expected.get(0), reader.readObject());
        assertEquals(expected.get(1), reader.readObject());
        assertEquals(expected.get(2), reader.readObject());

        try
        {
            reader.readObject();
            fail("Exception not thrown");
        }
        catch (EOFException e)
        {
            //  PASS
        }
    }

    public void testConvertMapMessageBody() throws Exception
    {
        final Map<String, String> expected = Collections.singletonMap("key", "value");

        final AmqpValue value = new AmqpValue(expected);
        configureMessageContent(value.createEncodingRetainingSection());
        final Message_1_0 sourceMessage = new Message_1_0(_handle);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        assertEquals("jms/map-message", convertedMessage.getMessageHeader().getMimeType());

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        TypedBytesContentReader reader = new TypedBytesContentReader(ByteBuffer.wrap(getBytes(content)));
        assertEquals(expected.size(), reader.readIntImpl());
        assertEquals("key", reader.readStringImpl());
        assertEquals(expected.get("key"), reader.readObject());
        try
        {
            reader.readString();
            fail("Exception not thrown");
        }
        catch (EOFException e)
        {
            //  PASS
        }
    }

    public void testConvertBytesMessageBody() throws Exception
    {
        final String expected = "helloworld";

        final Data value = new Data(new Binary(expected.getBytes()));
        configureMessageContent(value.createEncodingRetainingSection());
        final Message_1_0 sourceMessage = new Message_1_0(_handle);

        final AMQMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        assertEquals("application/octet-stream", convertedMessage.getMessageHeader().getMimeType());

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());
        assertArrayEquals(expected.getBytes(), getBytes(content));
    }

    private void configureMessageContent(final EncodingRetainingSection section)
    {
        final QpidByteBuffer combined = QpidByteBuffer.wrap(ByteBufferUtils.combine(section.getEncodedForm()));
        when(_handle.getContentSize()).thenReturn((int) section.getEncodedSize());
        final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
        final ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);

        when(_handle.getContent(offsetCaptor.capture(), sizeCaptor.capture())).then(new Answer<Collection<QpidByteBuffer>>()
        {
            @Override
            public Collection<QpidByteBuffer> answer(final InvocationOnMock invocation) throws Throwable
            {
                final QpidByteBuffer view = combined.view(offsetCaptor.getValue(), sizeCaptor.getValue());
                return Collections.singleton(view);
            }
        });
    }

    private byte[] getBytes(final Collection<QpidByteBuffer> content) throws Exception
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (QpidByteBuffer buf: content)
        {
            ByteStreams.copy(buf.asInputStream(), bos);
            buf.dispose();
        }
        return bos.toByteArray();
    }
}
