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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.ListToAmqpListConverter;
import org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter.MapToAmqpMapConverter;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.typedmessage.TypedBytesContentWriter;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes"})
class MessageConverter_0_10_to_InternalTest extends UnitTestBase
{
    private final MessageConverter_v0_10_to_Internal _converter = new MessageConverter_v0_10_to_Internal();
    private final StoredMessage<MessageMetaData_0_10> _handle = mock(StoredMessage.class);
    private final MessageMetaData_0_10 _metaData = mock(MessageMetaData_0_10.class);
    private final AMQMessageHeader _amqpHeader = mock(AMQMessageHeader.class);
    private final Header _header = mock(Header.class);

    private MessageProperties _messageProperties;

    @BeforeEach
    void setUp()
    {
        _messageProperties = new MessageProperties();

        when(_handle.getMetaData()).thenReturn(_metaData);
        when(_header.getMessageProperties()).thenReturn(_messageProperties);
        when(_metaData.getHeader()).thenReturn(_header);
        when(_metaData.getMessageHeader()).thenReturn(_amqpHeader);
        when(_metaData.getMessageProperties()).thenReturn(_messageProperties);
    }

    @Test
    void convertStringMessageBody()
    {
        doTestTextMessage("helloworld", "text/plain");
    }

    @Test
    void convertEmptyStringMessageBody()
    {
        doTestTextMessage(null, "text/plain");
    }

    @Test
    void convertStringXmlMessageBody()
    {
        doTestTextMessage("<helloworld></helloworld>", "text/xml");
    }

    @Test
    void convertEmptyStringXmlMessageBody()
    {
        doTestTextMessage(null, "text/xml");
    }

    @Test
    void convertEmptyStringApplicationXmlMessageBody()
    {
        doTestTextMessage(null, "application/xml");
    }

    @Test
    void convertStringWithContentTypeText()
    {
        doTestTextMessage("foo","text/foobar");
    }

    @Test
    void convertStringWithContentTypeApplicationXml()
    {
        doTestTextMessage("<helloworld></helloworld>","application/xml");
    }

    @Test
    void convertStringWithContentTypeApplicationXmlDtd()
    {
        doTestTextMessage("<!DOCTYPE name []>","application/xml-dtd");
    }

    @Test
    void convertStringWithContentTypeApplicationFooXml()
    {
        doTestTextMessage("<helloworld></helloworld>","application/foo+xml");
    }

    @Test
    void convertStringWithContentTypeApplicationJson()
    {
        doTestTextMessage("[]","application/json");
    }

    @Test
    void convertStringWithContentTypeApplicationFooJson()
    {
        doTestTextMessage("[]","application/foo+json");
    }

    @Test
    void convertStringWithContentTypeApplicationJavascript()
    {
        doTestTextMessage("var foo","application/javascript");
    }

    @Test
    void convertStringWithContentTypeApplicationEcmascript()
    {
        doTestTextMessage("var foo","application/ecmascript");
    }

    @Test
    void convertBytesMessageBody()
    {
        doTestBytesMessage("helloworld".getBytes());
    }

    @Test
    void convertBytesMessageBodyNoContentType()
    {
        final byte[] messageContent = "helloworld".getBytes();
        doTest(messageContent, null, messageContent, null);
    }

    @Test
    void convertMessageBodyUnknownContentType()
    {
        final byte[] messageContent = "helloworld".getBytes();
        final String mimeType = "my/bytes";
        doTest(messageContent, mimeType, messageContent, mimeType);
    }

    @Test
    void convertEmptyBytesMessageBody()
    {
        doTestBytesMessage(new byte[0]);
    }

    @Test
    void convertJmsStreamMessageBody() throws Exception
    {
        final List<Object> expected = List.of("apple", 43, 31.42D);
        final byte[] messageBytes = getJmsStreamMessageBytes(expected);

        final String mimeType = "jms/stream-message";
        doTestStreamMessage(messageBytes, mimeType, expected);
    }

    @Test
    void convertEmptyJmsStreamMessageBody()
    {
        final List<Object> expected = List.of();
        final String mimeType = "jms/stream-message";
        doTestStreamMessage(null, mimeType, expected);
    }

    @Test
    void convertAmqpListMessageBody()
    {
        final List<Object> expected = List.of("apple", 43, 31.42D);
        final byte[] messageBytes = new ListToAmqpListConverter().toMimeContent(expected);

        doTestStreamMessage(messageBytes, "amqp/list", expected);
    }

    @Test
    void convertEmptyAmqpListMessageBody()
    {
        final List<Object> expected = List.of();
        doTestStreamMessage(null, "amqp/list", expected);
    }

    @Test
    void convertJmsMapMessageBody() throws Exception
    {
        final Map<String, Object> expected = Map.of("key", "value");
        final byte[] messageBytes = getJmsMapMessageBytes(expected);

        doTestMapMessage(messageBytes, "jms/map-message", expected);
    }

    @Test
    void convertEmptyJmsMapMessageBody()
    {
        doTestMapMessage(null, "jms/map-message", Map.of());
    }

    @Test
    void convertAmqpMapMessageBody()
    {
        final Map<String, Object> expected = Map.of("key", "value");
        final byte[] messageBytes = new MapToAmqpMapConverter().toMimeContent(expected);

        doTestMapMessage(messageBytes, "amqp/map", expected);
    }

    @Test
    void convertEmptyAmqpMapMessageBody()
    {
        doTestMapMessage(null, "amqp/map", Map.of());
    }

    @Test
    void convertObjectStreamMessageBody() throws Exception
    {
        final byte[] messageBytes = getObjectStreamMessageBytes(UUID.randomUUID());
        doTestObjectMessage(messageBytes, "application/java-object-stream", messageBytes);
    }

    @Test
    void convertObjectStream2MessageBody() throws Exception
    {
        final byte[] messageBytes = getObjectStreamMessageBytes(UUID.randomUUID());
        doTestObjectMessage(messageBytes, "application/x-java-serialized-object", messageBytes);
    }

    @Test
    void convertEmptyObjectStreamMessageBody() throws Exception
    {
        doTestObjectMessage(null, "application/java-object-stream", new byte[0]);
    }

    @Test
    void convertEmptyMessageWithoutContentType()
    {
        doTest(null, null, null, null);
    }

    @Test
    void convertEmptyMessageWithUnknownContentType()
    {
        doTest(null, "foo/bar", new byte[0], "foo/bar");
    }

    @Test
    void convertMessageWithoutContentType()
    {
        final byte[] expectedContent = "someContent".getBytes(UTF_8);
        doTest(expectedContent, null, expectedContent, null);
    }

    private byte[] getObjectStreamMessageBytes(final Serializable o) throws Exception
    {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final ObjectOutputStream oos = new ObjectOutputStream(bos))
        {
            oos.writeObject(o);
            return bos.toByteArray();
        }
    }

    private byte[] getJmsStreamMessageBytes(final List<Object> objects) throws Exception
    {
        final TypedBytesContentWriter writer = new TypedBytesContentWriter();
        for (final Object o : objects)
        {
            writer.writeObject(o);
        }
        return getBytes(writer);
    }

    private byte[] getJmsMapMessageBytes(final Map<String, Object> map) throws Exception
    {
        final TypedBytesContentWriter writer = new TypedBytesContentWriter();
        writer.writeIntImpl(map.size());
        for (final Map.Entry<String, Object> entry : map.entrySet())
        {
            writer.writeNullTerminatedStringImpl(entry.getKey());
            writer.writeObject(entry.getValue());
        }
        return getBytes(writer);
    }

    private byte[] getBytes(final TypedBytesContentWriter writer)
    {
        final ByteBuffer buf = writer.getData();
        final byte[] expected = new byte[buf.remaining()];
        buf.get(expected);
        return expected;
    }

    private MessageTransferMessage getAmqMessage(final byte[] expected, final String mimeType)
    {
        configureMessageContent(expected);
        configureMessageHeader(mimeType);

        return new MessageTransferMessage(_handle, new Object());
    }

    private void configureMessageHeader(final String mimeType)
    {
        when(_amqpHeader.getMimeType()).thenReturn(mimeType);
        _messageProperties.setContentType(mimeType);
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

        when(_handle.getContent(offsetCaptor.capture(), sizeCaptor.capture()))
                .then(invocation -> combined.view(offsetCaptor.getValue(), sizeCaptor.getValue()));
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

    private void doTestBytesMessage(final byte[] messageContent)
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
        final MessageTransferMessage sourceMessage = getAmqMessage(messageBytes, mimeType);
        final InternalMessage convertedMessage = _converter.convert(sourceMessage, mock(NamedAddressSpace.class));
        
        if (expectedContent instanceof byte[])
        {
            assertArrayEquals(((byte[]) expectedContent), ((byte[]) convertedMessage.getMessageBody()),
                    "Unexpected content");
        }
        else if (expectedContent instanceof List)
        {
            assertEquals(new ArrayList<Object>((Collection) expectedContent),
                    new ArrayList<Object>((Collection) convertedMessage.getMessageBody()),
                    "Unexpected content");
        }
        else if (expectedContent instanceof Map)
        {
            assertEquals(new HashMap<Object,Object>((Map) expectedContent),
                    new HashMap<Object,Object>((Map) convertedMessage.getMessageBody()),
                    "Unexpected content");
        }
        else
        {
            assertEquals(expectedContent, convertedMessage.getMessageBody(), "Unexpected content");
        }
        final String convertedMimeType = convertedMessage.getMessageHeader().getMimeType();
        assertEquals(expectedMimeType, convertedMimeType, "Unexpected content type");
    }
}
