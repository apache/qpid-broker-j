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
package org.apache.qpid.server.message.mimecontentconverter;

import static org.apache.qpid.server.message.mimecontentconverter.AmqpCompoundMimeContentToObjectConverter.toObject;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.test.utils.UnitTestBase;

class AmqpCompoundMimeContentToObjectConverterTest extends UnitTestBase
{
    @ParameterizedTest
    @MethodSource("decoderRejections")
    void testDecoderRejectionPreservesCause(final RuntimeException rejection)
    {
        final AmqpCompoundMimeContentToObjectConverter<?> converter =
                mock(AmqpCompoundMimeContentToObjectConverter.class);
        final byte[] content = new byte[0];
        when(converter.toObject(content, 0, 1)).thenThrow(rejection);

        final MessageConversionException exception = assertThrows(MessageConversionException.class, () ->
                toObject(converter, content, 0, 1));

        assertSame(rejection, exception.getCause());
    }

    @Test
    void testExistingConversionExceptionIsPreserved()
    {
        final AmqpCompoundMimeContentToObjectConverter<?> converter =
                mock(AmqpCompoundMimeContentToObjectConverter.class);
        final byte[] content = new byte[0];
        final MessageConversionException rejection = new MessageConversionException("Conversion failed");
        when(converter.toObject(content, 0, 1)).thenThrow(rejection);

        assertSame(rejection, assertThrows(MessageConversionException.class, () -> toObject(converter, content, 1)));
    }

    @Test
    void testNonCompoundConverterExceptionIsPreserved()
    {
        final MimeContentToObjectConverter<?> converter = mock(MimeContentToObjectConverter.class);
        final byte[] content = new byte[0];
        final IllegalArgumentException rejection = new IllegalArgumentException("Conversion failed");
        when(converter.toObject(content)).thenThrow(rejection);

        assertSame(rejection, assertThrows(IllegalArgumentException.class, () -> toObject(converter, content, 0, 1)));
    }

    private static Stream<RuntimeException> decoderRejections()
    {
        return Stream.of(new IllegalArgumentException("Compound limit exceeded"),
                new IllegalStateException("Invalid compound encoding"));
    }
}
