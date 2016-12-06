
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


package org.apache.qpid.server.protocol.v1_0.type.messaging.codec;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.TypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;

public class AmqpValueSectionConstructor implements DescribedTypeConstructor<AmqpValueSection>
{

    private static final Object[] DESCRIPTORS =
            {
                    Symbol.valueOf("amqp:amqp-value:*"),UnsignedLong.valueOf(0x0000000000000077L),
            };

    private static final AmqpValueSectionConstructor INSTANCE = new AmqpValueSectionConstructor();

    public static void register(DescribedTypeConstructorRegistry registry)
    {
        for(Object descriptor : DESCRIPTORS)
        {
            registry.register(descriptor, INSTANCE);
        }
    }


    @Override
    public TypeConstructor<AmqpValueSection> construct(final Object descriptor,
                                                        final List<QpidByteBuffer> in,
                                                        final int[] originalPositions,
                                                        final ValueHandler valueHandler)
            throws AmqpErrorException
    {
        return new LazyConstructor(originalPositions);
    }


    private class LazyConstructor implements TypeConstructor<AmqpValueSection>
    {

        private final int[] _originalPositions;

        public LazyConstructor(final int[] originalPositions)
        {

            _originalPositions = originalPositions;
        }

        @Override
        public AmqpValueSection construct(final List<QpidByteBuffer> in, final ValueHandler handler)
                throws AmqpErrorException
        {
            skipValue(in);

            List<QpidByteBuffer> encoding = new ArrayList<>();
            int offset = in.size() - _originalPositions.length;
            for (int i = offset; i < in.size(); i++)
            {
                QpidByteBuffer buf = in.get(i);
                if (buf.position() == _originalPositions[i - offset])
                {
                    if (buf.hasRemaining())
                    {
                        break;
                    }
                }
                else
                {
                    QpidByteBuffer dup = buf.duplicate();
                    dup.position(_originalPositions[i - offset]);
                    dup.limit(buf.position());
                    encoding.add(dup);
                }
            }
            AmqpValueSection object =
                    new AmqpValueSection(((SectionDecoderRegistry) handler.getDescribedTypeRegistry()).getUnderlyingRegistry());
            object.setEncodedForm(encoding);
            return object;
        }
    }

    private void skipValue(final List<QpidByteBuffer> in) throws AmqpErrorException
    {
        byte formatCode = QpidByteBufferUtils.get(in);

        if (formatCode == 0)
        {
            // This is only valid if the described value is not an array
            skipValue(in);
            skipValue(in);
        }
        else
        {
            int category = (formatCode >> 4) & 0x0F;
            switch (category)
            {
                case 0x04:
                    break;
                case 0x05:
                    QpidByteBufferUtils.skip(in, 1);
                    break;
                case 0x06:
                    QpidByteBufferUtils.skip(in, 2);
                    break;
                case 0x07:
                    QpidByteBufferUtils.skip(in, 4);
                    break;
                case 0x08:
                    QpidByteBufferUtils.skip(in, 8);
                    break;
                case 0x09:
                    QpidByteBufferUtils.skip(in, 16);
                    break;
                case 0x0a:
                case 0x0c:
                case 0x0e:
                    QpidByteBufferUtils.skip(in, ((int) QpidByteBufferUtils.get(in)) & 0xFF);
                    break;
                case 0x0b:
                case 0x0d:
                case 0x0f:
                    QpidByteBufferUtils.skip(in, QpidByteBufferUtils.getInt(in));
                    break;
                default:
                    throw new AmqpErrorException(ConnectionError.FRAMING_ERROR, "Unknown type");
            }
        }
    }

}
