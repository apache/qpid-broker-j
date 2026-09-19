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

import org.apache.qpid.server.protocol.converter.MessageConversionException;

public interface AmqpCompoundMimeContentToObjectConverter<O> extends MimeContentToObjectConverter<O>
{
    int DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS = 0;

    O toObject(final byte[] data, final int maxZeroWidthArrayElements, final int maxNestedObjects);

    static Object toObject(final MimeContentToObjectConverter<?> converter,
                           final byte[] data,
                           final int maxNestedObjects)
    {
        return toObject(converter, data, DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, maxNestedObjects);
    }

    static Object toObject(final MimeContentToObjectConverter<?> converter,
                           final byte[] data,
                           final int maxZeroWidthArrayElements,
                           final int maxNestedObjects)
    {
        if (converter instanceof AmqpCompoundMimeContentToObjectConverter<?>)
        {
            try
            {
                return ((AmqpCompoundMimeContentToObjectConverter<?>) converter)
                        .toObject(data, maxZeroWidthArrayElements, maxNestedObjects);
            }
            catch (final IllegalArgumentException | IllegalStateException e)
            {
                throw new MessageConversionException("Cannot convert AMQP compound content", e);
            }
        }
        return converter.toObject(data);
    }
}
