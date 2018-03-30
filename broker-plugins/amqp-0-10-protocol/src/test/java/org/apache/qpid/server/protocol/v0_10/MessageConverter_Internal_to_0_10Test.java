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
package org.apache.qpid.server.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.message.internal.InternalMessageMetaDataType;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.ListToAmqpListConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.MapToAmqpMapConverter;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.typedmessage.mimecontentconverter.ListToJmsStreamMessage;
import org.apache.qpid.server.typedmessage.mimecontentconverter.MapToJmsMapMessage;
import org.apache.qpid.test.utils.UnitTestBase;

public class MessageConverter_Internal_to_0_10Test extends UnitTestBase
{
    private final MessageConverter_Internal_to_v0_10 _converter = new MessageConverter_Internal_to_v0_10();
    private final StoredMessage<InternalMessageMetaData> _handle = mock(StoredMessage.class);
    private final AMQMessageHeader _amqpHeader = mock(AMQMessageHeader.class);

    @Before
    public void setUp() throws Exception
    {
    }


    @Test
    public void testStringMessage() throws Exception
    {
        String content = "testContent";
        final String mimeType = "text/plain";
        doTest(content, mimeType, content.getBytes(UTF_8), mimeType);
    }

    @Test
    public void testStringMessageWithUnknownMimeType() throws Exception
    {
        String content = "testContent";
        final String mimeType = "foo/bar";
        doTest(content, mimeType, content.getBytes(UTF_8), "text/plain");
    }

    @Test
    public void testStringMessageWithoutMimeType() throws Exception
    {
        String content = "testContent";
        doTest(content, null, content.getBytes(UTF_8), "text/plain");
    }

    @Test
    public void testListMessageWithMimeType() throws Exception
    {
        ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42);
        final ListToJmsStreamMessage listToJmsStreamMessage = new ListToJmsStreamMessage();
        final byte[] expectedContent = listToJmsStreamMessage.toMimeContent(content);
        doTest(content, "foo/bar", expectedContent, listToJmsStreamMessage.getMimeType());
    }

    @Test
    public void testListMessageWithoutMimeType() throws Exception
    {
        ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42);
        final ListToJmsStreamMessage listToJmsStreamMessage = new ListToJmsStreamMessage();
        final byte[] expectedContent = listToJmsStreamMessage.toMimeContent(content);
        doTest(content, null, expectedContent, listToJmsStreamMessage.getMimeType());
    }

    @Test
    public void testListMessageWithoutMimeTypeWithNonJmsContent() throws Exception
    {
        ArrayList<?> content = Lists.newArrayList("testItem", 37.5, 42, Lists.newArrayList());
        final ListToAmqpListConverter listToAmqpListConverter = new ListToAmqpListConverter();
        final byte[] expectedContent = listToAmqpListConverter.toMimeContent(content);
        doTest(content, null, expectedContent, listToAmqpListConverter.getMimeType());
    }

    @Test
    public void testListMessageWithoutMimeTypeWithNonConvertibleItem() throws Exception
    {
        ArrayList<?> content = Lists.newArrayList(new MySerializable());
        final InternalMessage sourceMessage = getAmqMessage(content, null);
        doTest(content, null, getObjectStreamMessageBytes(content), "application/java-object-stream");
    }

    @Test
    public void testByteArrayMessageWithoutMimeType() throws Exception
    {
        byte[] content = "testContent".getBytes(UTF_8);
        doTest(content, null, content, "application/octet-stream");
    }

    @Test
    public void testByteArrayMessageWithMimeType() throws Exception
    {
        byte[] content = "testContent".getBytes(UTF_8);
        final String mimeType = "foo/bar";
        doTest(content, mimeType, content, mimeType);
    }

    @Test
    public void testEmptyByteArrayMessageWithMimeType() throws Exception
    {
        byte[] content = new byte[0];
        final String mimeType = "foo/bar";
        doTest(content, mimeType, content, mimeType);
    }

    @Test
    public void testMapMessageWithMimeType() throws Exception
    {
        HashMap<Object, Object> content = new HashMap<>();
        content.put("key1", 37);
        content.put("key2", "foo");
        final String mimeType = "foo/bar";
        final MapToJmsMapMessage mapToJmsMapMessage = new MapToJmsMapMessage();
        final byte[] expectedContent = mapToJmsMapMessage.toMimeContent(content);
        doTest(content, mimeType, expectedContent, mapToJmsMapMessage.getMimeType());
    }

    @Test
    public void testMapMessageWithoutMimeType() throws Exception
    {
        HashMap<Object, Object> content = new HashMap<>();
        content.put("key1", 37);
        content.put("key2", "foo");
        final MapToJmsMapMessage mapToJmsMapMessage = new MapToJmsMapMessage();
        final byte[] expectedContent = mapToJmsMapMessage.toMimeContent(content);
        doTest(content, null, expectedContent, mapToJmsMapMessage.getMimeType());
    }

    @Test
    public void testMapMessageWithMimeTypeWithNonJmsContent() throws Exception
    {
        HashMap<Object, Object> content = new HashMap<>();
        content.put("key", Collections.singletonMap("foo", "bar"));
        final String mimeType = "foo/bar";
        final MapToAmqpMapConverter mapToAmqpMapConverter = new MapToAmqpMapConverter();
        final byte[] expectedContent = mapToAmqpMapConverter.toMimeContent(content);
        doTest(content, mimeType, expectedContent, mapToAmqpMapConverter.getMimeType());
    }

    @Test
    public void testMapMessageWithoutMimeTypeWithNonConvertibleEntry() throws Exception
    {
        HashMap<Object, Object> content = new HashMap<>();
        content.put(37, new MySerializable());

        doTest(content, null, getObjectStreamMessageBytes(content), "application/java-object-stream");
    }

    @Test
    public void testSerializableMessageWithMimeType() throws Exception
    {
        Serializable content = new MySerializable();
        final String mimeType = "foo/bar";
        doTest(content, mimeType, getObjectStreamMessageBytes(content), "application/java-object-stream");
    }

    @Test
    public void testSerializableMessageWithoutMimeType() throws Exception
    {
        Serializable content = new MySerializable();
        doTest(content, null, getObjectStreamMessageBytes(content), "application/java-object-stream");
    }

    @Test
    public void testNullMessageWithoutMimeType() throws Exception
    {
        doTest(null, null, null, null);
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


    protected InternalMessage getAmqMessage(final Serializable content, final String mimeType) throws Exception
    {
        final byte[] serializedContent = getObjectStreamMessageBytes(content);
        configureMessageContent(serializedContent);
        configureMessageHeader(mimeType);

        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(_amqpHeader);
        final int contentSize = serializedContent == null ? 0 : serializedContent.length;
        final InternalMessageMetaData metaData =
                new InternalMessageMetaData(false, internalMessageHeader, contentSize);
        when(_handle.getMetaData()).thenReturn(metaData);

        return ((InternalMessage) InternalMessageMetaDataType.INSTANCE.createMessage(_handle));
    }

    private void configureMessageHeader(final String mimeType)
    {
        when(_amqpHeader.getMimeType()).thenReturn(mimeType);
    }

    private void configureMessageContent(byte[] section)
    {
        if (section == null)
        {
            section = new byte[0];
        }
        final QpidByteBuffer combined = QpidByteBuffer.wrap(section);
        when(_handle.getContentSize()).thenReturn(section.length);
        final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
        final ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);

        when(_handle.getContent(offsetCaptor.capture(),
                                sizeCaptor.capture())).then(invocation -> combined.view(offsetCaptor.getValue(),
                                                                                        sizeCaptor.getValue()));
    }

    private void doTest(final Serializable messageBytes,
                        final String mimeType,
                        final byte[] expectedContent,
                        final String expectedContentType) throws Exception
    {
        final InternalMessage sourceMessage = getAmqMessage(messageBytes, mimeType);
        final MessageTransferMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        final QpidByteBuffer content = convertedMessage.getContent();

        assertArrayEquals("Unexpected content", expectedContent != null ? expectedContent : new byte[0], getBytes(content));
        assertEquals("Unexpected content type",
                            expectedContentType,
                            convertedMessage.getMessageHeader().getMimeType());
    }

    private byte[] getBytes(final QpidByteBuffer content) throws Exception
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             InputStream contentInputStream = content.asInputStream())
        {
            ByteStreams.copy(contentInputStream, bos);
            content.dispose();
            return bos.toByteArray();
        }
    }


    private static class MySerializable implements Serializable
    {
    }
}
