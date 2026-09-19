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
package org.apache.qpid.server.plugin;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.util.GZIPUtils;
import org.apache.qpid.server.util.GZIPUtils.GZIPInflationLimitException;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings("rawtypes")
class MessageConverterTest extends UnitTestBase
{
    private static final int MAXIMUM_DECOMPRESSION_SIZE = 1024;

    @Test
    void testLegacyConverterRejectsGzipWhenBoundedConversionIsRequested()
    {
        final ServerMessage source = mock(ServerMessage.class);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final LegacyMessageConverter converter = new LegacyMessageConverter(mock(ServerMessage.class));
        when(source.getMessageHeader()).thenReturn(header);
        when(header.getEncoding()).thenReturn(GZIPUtils.GZIP_CONTENT_ENCODING);

        final MessageConversionException exception = assertThrows(MessageConversionException.class, () ->
                converter.convert(source, mock(NamedAddressSpace.class), MAXIMUM_DECOMPRESSION_SIZE));

        assertInstanceOf(GZIPInflationLimitException.class, exception.getCause());
        assertFalse(converter.wasConverted());
    }

    @Test
    void testLegacyConverterHandlesUncompressedBoundedConversion()
    {
        final ServerMessage source = mock(ServerMessage.class);
        final ServerMessage expected = mock(ServerMessage.class);
        final LegacyMessageConverter converter = new LegacyMessageConverter(expected);
        when(source.getMessageHeader()).thenReturn(mock(AMQMessageHeader.class));

        final ServerMessage converted = converter.convert(source, mock(NamedAddressSpace.class),
                MAXIMUM_DECOMPRESSION_SIZE);

        assertSame(expected, converted);
        assertTrue(converter.wasConverted());
    }

    private static final class LegacyMessageConverter implements MessageConverter<ServerMessage, ServerMessage>
    {
        private final ServerMessage _convertedMessage;
        private boolean _converted;

        private LegacyMessageConverter(final ServerMessage convertedMessage)
        {
            _convertedMessage = convertedMessage;
        }

        @Override
        public Class<ServerMessage> getInputClass()
        {
            return ServerMessage.class;
        }

        @Override
        public Class<ServerMessage> getOutputClass()
        {
            return ServerMessage.class;
        }

        @Override
        public ServerMessage convert(final ServerMessage message, final NamedAddressSpace addressSpace)
        {
            _converted = true;
            return _convertedMessage;
        }

        @Override
        public void dispose(final ServerMessage message)
        {
        }

        @Override
        public String getType()
        {
            return "legacy";
        }

        private boolean wasConverted()
        {
            return _converted;
        }
    }
}
