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
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.MessageMetaData;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v1_0.Message_1_0;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.typedmessage.TypedBytesContentWriter;
import org.apache.qpid.test.utils.QpidTestCase;

public class MessageConverter_0_8_to_1_0Test extends QpidTestCase
{
    private final MessageConverter_0_8_to_1_0 _converter = new MessageConverter_0_8_to_1_0();
    private final AMQPDescribedTypeRegistry _typeRegistry = AMQPDescribedTypeRegistry.newInstance()
                                                                                     .registerTransportLayer()
                                                                                     .registerMessagingLayer()
                                                                                     .registerTransactionLayer()
                                                                                     .registerSecurityLayer();

    private final StoredMessage<MessageMetaData> _handle = mock(StoredMessage.class);

    private final MessageMetaData _metaData = mock(MessageMetaData.class);
    private final AMQMessageHeader _header = mock(AMQMessageHeader.class);
    private final ContentHeaderBody _contentHeaderBody = mock(ContentHeaderBody.class);
    private final BasicContentHeaderProperties _basicContentHeaderProperties = mock(BasicContentHeaderProperties.class);

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        when(_handle.getMetaData()).thenReturn(_metaData);
        when(_metaData.getMessageHeader()).thenReturn(_header);
        when(_metaData.getMessagePublishInfo()).thenReturn(new MessagePublishInfo());
        when(_metaData.getContentHeaderBody()).thenReturn(_contentHeaderBody);
        when(_contentHeaderBody.getProperties()).thenReturn(_basicContentHeaderProperties);
    }

    public void testConvertStringMessageBody() throws Exception
    {
        final String expected = "helloworld";

        final AMQMessage sourceMessage = getAmqMessage(expected.getBytes(), "text/plain");

        final Message_1_0 convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        List<EncodingRetainingSection<?>> sections = getEncodingRetainingSections(content, 1);
        assertEquals(expected, sections.get(0).getValue());
    }

    public void testConvertBytesMessageBody() throws Exception
    {
        final byte[] expected = "helloworld".getBytes();

        final AMQMessage sourceMessage = getAmqMessage(expected, "application/octet-stream");

        final Message_1_0 convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        List<EncodingRetainingSection<?>> sections = getEncodingRetainingSections(content, 1);
        final Binary value = (Binary) sections.get(0).getValue();
        assertArrayEquals(expected, value.getArray());
    }

    public void testConvertListMessageBody() throws Exception
    {
        final List<Object> expected = Lists.<Object>newArrayList("apple", 43, 31.42D);
        final byte[] messageBytes = getJmsStreamMessageBytes(expected);

        final AMQMessage sourceMessage = getAmqMessage(messageBytes, "jms/stream-message");

        final Message_1_0 convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        List<EncodingRetainingSection<?>> sections = getEncodingRetainingSections(content, 1);
        assertEquals(expected, sections.get(0).getValue());
    }

    public void testConvertMapMessageBody() throws Exception
    {
        final Map<String, Object> expected = Collections.<String, Object>singletonMap("key", "value");
        final byte[] messageBytes = getJmsMapMessageBytes(expected);

        final AMQMessage sourceMessage = getAmqMessage(messageBytes, "jms/map-message");

        final Message_1_0 convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        List<EncodingRetainingSection<?>> sections = getEncodingRetainingSections(content, 1);
        assertEquals(expected, sections.get(0).getValue());
    }

    public void testConvertObjectStreamMessageBody() throws Exception
    {
        final byte[] messageBytes = getObjectStreamMessageBytes(UUID.randomUUID());

        final AMQMessage sourceMessage = getAmqMessage(messageBytes, "application/java-object-stream");

        final Message_1_0 convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));

        final Collection<QpidByteBuffer> content = convertedMessage.getContent(0, (int) convertedMessage.getSize());

        List<EncodingRetainingSection<?>> sections = getEncodingRetainingSections(content, 1);
        final Binary value = (Binary) sections.get(0).getValue();
        assertArrayEquals(messageBytes, value.getArray());
    }

    private byte[] getObjectStreamMessageBytes(final Serializable o) throws Exception
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos))
        {
            oos.writeObject(o);
            return bos.toByteArray();
        }
    }

    private byte[] getJmsStreamMessageBytes(List<Object> objects) throws Exception
    {
        TypedBytesContentWriter writer = new TypedBytesContentWriter();
        for(Object o : objects)
        {
            writer.writeObject(o);
        }
        return getBytes(writer);
    }

    private byte[] getJmsMapMessageBytes(Map<String,Object> map) throws Exception
    {
        TypedBytesContentWriter writer = new TypedBytesContentWriter();
        writer.writeIntImpl(map.size());
        for(Map.Entry<String, Object> entry : map.entrySet())
        {
            writer.writeNullTerminatedStringImpl(entry.getKey());
            writer.writeObject(entry.getValue());
        }
        return getBytes(writer);
    }

    private byte[] getBytes(final TypedBytesContentWriter writer)
    {
        ByteBuffer buf = writer.getData();
        final byte[] expected = new byte[buf.remaining()];
        buf.get(expected);
        return expected;
    }

    private List<EncodingRetainingSection<?>> getEncodingRetainingSections(final Collection<QpidByteBuffer> content,
                                                                           final int expectedNumberOfSections)
            throws Exception
    {
        SectionDecoder sectionDecoder = new SectionDecoderImpl(_typeRegistry.getSectionDecoderRegistry());
        final List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(new ArrayList<>(content));
        assertEquals("Unexpected number of sections", expectedNumberOfSections, sections.size());
        return sections;

    }

    protected AMQMessage getAmqMessage(final byte[] expected, final String mimeType)
    {
        configureMessageContent(expected);
        configureMessageHeader(mimeType);

        return new AMQMessage(_handle);
    }

    private void configureMessageHeader(final String mimeType)
    {
        when(_header.getMimeType()).thenReturn(mimeType);
    }

    private void configureMessageContent(final byte[] section)
    {
        final QpidByteBuffer combined = QpidByteBuffer.wrap(section);
        when(_handle.getContentSize()).thenReturn((int) section.length);
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

}
