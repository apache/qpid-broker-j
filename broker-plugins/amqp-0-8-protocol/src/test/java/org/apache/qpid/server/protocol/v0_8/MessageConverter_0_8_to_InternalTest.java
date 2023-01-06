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
package org.apache.qpid.server.protocol.v0_8;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.ListToAmqpListConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.MapToAmqpMapConverter;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.typedmessage.TypedBytesContentWriter;
import org.apache.qpid.test.utils.UnitTestBase;

public class MessageConverter_0_8_to_InternalTest extends UnitTestBase
{
    private final MessageConverter_v0_8_to_Internal _converter = new MessageConverter_v0_8_to_Internal();

    private final StoredMessage<MessageMetaData> _handle = mock(StoredMessage.class);

    private final MessageMetaData _metaData = mock(MessageMetaData.class);
    private final AMQMessageHeader _header = mock(AMQMessageHeader.class);
    private final ContentHeaderBody _contentHeaderBody = mock(ContentHeaderBody.class);
    private final BasicContentHeaderProperties _basicContentHeaderProperties = mock(BasicContentHeaderProperties.class);

    @BeforeEach
    public void setUp() throws Exception
    {
        when(_handle.getMetaData()).thenReturn(_metaData);
        when(_metaData.getMessageHeader()).thenReturn(_header);
        when(_metaData.getMessagePublishInfo()).thenReturn(new MessagePublishInfo());
        when(_metaData.getContentHeaderBody()).thenReturn(_contentHeaderBody);
        when(_contentHeaderBody.getProperties()).thenReturn(_basicContentHeaderProperties);
    }

    @Test
    public void testConvertStringMessageBody()
    {
        doTestTextMessage("helloworld", "text/plain");
    }

    @Test
    public void testConvertEmptyStringMessageBody()
    {
        doTestTextMessage(null, "text/plain");
    }

    @Test
    public void testConvertStringXmlMessageBody()
    {
        doTestTextMessage("<helloworld></helloworld>", "text/xml");
    }

    @Test
    public void testConvertEmptyStringXmlMessageBody()
    {
        doTestTextMessage(null, "text/xml");
    }

    @Test
    public void testConvertEmptyStringApplicationXmlMessageBody()
    {
        doTestTextMessage(null, "application/xml");
    }

    @Test
    public void testConvertStringWithContentTypeText()
    {
        doTestTextMessage("foo","text/foobar");
    }

    @Test
    public void testConvertStringWithContentTypeApplicationXml()
    {
        doTestTextMessage("<helloworld></helloworld>","application/xml");
    }

    @Test
    public void testConvertStringWithContentTypeApplicationXmlDtd()
    {
        doTestTextMessage("<!DOCTYPE name []>","application/xml-dtd");
    }

    @Test
    public void testConvertStringWithContentTypeApplicationFooXml()
    {
        doTestTextMessage("<helloworld></helloworld>","application/foo+xml");
    }

    @Test
    public void testConvertStringWithContentTypeApplicationJson()
    {
        doTestTextMessage("[]","application/json");
    }

    @Test
    public void testConvertStringWithContentTypeApplicationFooJson()
    {
        doTestTextMessage("[]","application/foo+json");
    }

    @Test
    public void testConvertStringWithContentTypeApplicationJavascript()
    {
        doTestTextMessage("var foo","application/javascript");
    }

    @Test
    public void testConvertStringWithContentTypeApplicationEcmascript()
    {
        doTestTextMessage("var foo","application/ecmascript");
    }

    @Test
    public void testConvertBytesMessageBody() throws Exception
    {
        doTestBytesMessage("helloworld".getBytes());
    }

    @Test
    public void testConvertBytesMessageBodyNoContentType()
    {
        final byte[] messageContent = "helloworld".getBytes();
        doTest(messageContent, null, messageContent, null);
    }

    @Test
    public void testConvertMessageBodyUnknownContentType()
    {
        final byte[] messageContent = "helloworld".getBytes();
        final String mimeType = "my/bytes";
        doTest(messageContent, mimeType, messageContent, mimeType);
    }


    @Test
    public void testConvertEmptyBytesMessageBody() throws Exception
    {
        doTestBytesMessage(new byte[0]);
    }

    @Test
    public void testConvertJmsStreamMessageBody() throws Exception
    {
        final List<Object> expected = Lists.newArrayList("apple", 43, 31.42D);
        final byte[] messageBytes = getJmsStreamMessageBytes(expected);

        final String mimeType = "jms/stream-message";
        doTestStreamMessage(messageBytes, mimeType, expected);
    }

    @Test
    public void testConvertEmptyJmsStreamMessageBody()
    {
        final List<Object> expected = Lists.newArrayList();
        final String mimeType = "jms/stream-message";
        doTestStreamMessage(null, mimeType, expected);
    }

    @Test
    public void testConvertAmqpListMessageBody()
    {
        final List<Object> expected = Lists.newArrayList("apple", 43, 31.42D);
        final byte[] messageBytes = new ListToAmqpListConverter().toMimeContent(expected);

        doTestStreamMessage(messageBytes, "amqp/list", expected);
    }

    @Test
    public void testConvertEmptyAmqpListMessageBody()
    {
        final List<Object> expected = Lists.newArrayList();
        doTestStreamMessage(null, "amqp/list", expected);
    }

    @Test
    public void testConvertJmsMapMessageBody() throws Exception
    {
        final Map<String, Object> expected = Collections.singletonMap("key", "value");
        final byte[] messageBytes = getJmsMapMessageBytes(expected);

        doTestMapMessage(messageBytes, "jms/map-message", expected);
    }

    @Test
    public void testConvertEmptyJmsMapMessageBody()
    {
        doTestMapMessage(null, "jms/map-message", Collections.emptyMap());
    }

    @Test
    public void testConvertAmqpMapMessageBody()
    {
        final Map<String, Object> expected = Collections.singletonMap("key", "value");
        final byte[] messageBytes = new MapToAmqpMapConverter().toMimeContent(expected);

        doTestMapMessage(messageBytes, "amqp/map", expected);
    }

    @Test
    public void testConvertEmptyAmqpMapMessageBody()
    {
        doTestMapMessage(null, "amqp/map", Collections.emptyMap());
    }

    @Test
    public void testConvertObjectStreamMessageBody() throws Exception
    {
        final byte[] messageBytes = getObjectStreamMessageBytes(UUID.randomUUID());
        doTestObjectMessage(messageBytes, "application/java-object-stream", messageBytes);
    }

    @Test
    public void testConvertObjectStream2MessageBody() throws Exception
    {
        final byte[] messageBytes = getObjectStreamMessageBytes(UUID.randomUUID());
        doTestObjectMessage(messageBytes, "application/x-java-serialized-object", messageBytes);
    }

    @Test
    public void testConvertEmptyObjectStreamMessageBody()
    {
        doTestObjectMessage(null, "application/java-object-stream", new byte[0]);
    }

    @Test
    public void testConvertEmptyMessageWithoutContentType()
    {
        doTest(null, null, null, null);
    }

    @Test
    public void testConvertEmptyMessageWithUnknownContentType()
    {
        doTest(null, "foo/bar", new byte[0], "foo/bar");
    }

    @Test
    public void testConvertMessageWithoutContentType()
    {
        final byte[] expectedContent = "someContent".getBytes(UTF_8);
        doTest(expectedContent, null, expectedContent, null);
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
        for (Object o : objects)
        {
            writer.writeObject(o);
        }
        return getBytes(writer);
    }

    private byte[] getJmsMapMessageBytes(Map<String, Object> map) throws Exception
    {
        TypedBytesContentWriter writer = new TypedBytesContentWriter();
        writer.writeIntImpl(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet())
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

    protected AMQMessage getAmqMessage(final byte[] expected, final String mimeType)
    {
        configureMessageContent(expected);
        configureMessageHeader(mimeType);

        return new AMQMessage(_handle);
    }

    private void configureMessageHeader(final String mimeType)
    {
        when(_header.getMimeType()).thenReturn(mimeType);
        when(_basicContentHeaderProperties.getContentTypeAsString()).thenReturn(mimeType);
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

    private void doTestTextMessage(final String originalContent, final String mimeType)
    {

        final byte[] contentBytes;
        final String expectedContent;
        if (originalContent == null)
        {
            contentBytes = null;
            expectedContent = "";
        }
        else
        {
            contentBytes = originalContent.getBytes(UTF_8);
            expectedContent = originalContent;
        }
        doTest(contentBytes, mimeType, expectedContent, mimeType);
    }


    private void doTestMapMessage(final byte[] messageBytes,
                                  final String mimeType,
                                  final Map<String, Object> expected)
    {
        doTest(messageBytes, mimeType, expected, null);
    }

    private void doTestBytesMessage(final byte[] messageContent) throws Exception
    {
        doTest(messageContent,"application/octet-stream", messageContent, "application/octet-stream");
    }

    private void doTestStreamMessage(final byte[] messageBytes,
                                     final String mimeType,
                                     final List<Object> expected)
    {
        doTest(messageBytes, mimeType, expected, null);
    }

    private void doTestObjectMessage(final byte[] messageBytes,
                                     final String mimeType,
                                     final byte[] expectedBytes)
    {
        doTest(messageBytes, mimeType, expectedBytes, "application/x-java-serialized-object");
    }

    private void doTest(final byte[] messageBytes,
                        final String mimeType,
                        final Object expectedContent,
                        final String expectedMimeType)
    {
        final AMQMessage sourceMessage = getAmqMessage(messageBytes, mimeType);
        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        
        if (expectedContent instanceof byte[])
        {
            assertArrayEquals(((byte[]) expectedContent), ((byte[]) convertedMessage.getMessageBody()),
                    "Unexpected content");
        }
        else if (expectedContent instanceof List)
        {
            assertEquals(new ArrayList((Collection) expectedContent), new ArrayList((Collection) convertedMessage.getMessageBody()),
                    "Unexpected content");
        }
        else if (expectedContent instanceof Map)
        {
            assertEquals(new HashMap((Map) expectedContent), new HashMap((Map) convertedMessage.getMessageBody()),
                    "Unexpected content");
        }
        else
        {
            assertEquals(expectedContent, convertedMessage.getMessageBody(), "Unexpected content");
        }
        String convertedMimeType = convertedMessage.getMessageHeader().getMimeType();
        assertEquals(expectedMimeType, convertedMimeType, "Unexpected content type");
    }
}
