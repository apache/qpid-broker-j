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
 */

package org.apache.qpid.server.protocol.v1_0.messaging;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class SectionDecoderImpl implements SectionDecoder
{

    private final ValueHandler _valueHandler;


    public SectionDecoderImpl(final SectionDecoderRegistry describedTypeRegistry)
    {
        _valueHandler = new ValueHandler(describedTypeRegistry);
    }

    @Override
    public List<EncodingRetainingSection<?>> parseAll(QpidByteBuffer buf) throws AmqpErrorException
    {

        List<EncodingRetainingSection<?>> obj = new ArrayList<>();
        try
        {
            while (buf.hasRemaining())
            {

                final Object parsedObject = _valueHandler.parse(buf);
                if (parsedObject instanceof EncodingRetainingSection)
                {
                    EncodingRetainingSection<?> section = (EncodingRetainingSection<?>) parsedObject;
                    obj.add(section);
                }
                else
                {
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                 String.format(
                                                         "Invalid Message: Expected type \"section\" but found \"%s\"",
                                                         parsedObject.getClass().getSimpleName()));
                }
            }
        }
        catch (AmqpErrorException e)
        {
            for (EncodingRetainingSection<?> encodingRetainingSection : obj)
            {
                encodingRetainingSection.dispose();
            }
            throw e;
        }
        return obj;
    }
}
