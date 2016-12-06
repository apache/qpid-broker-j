
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;

public class DataSectionConstructor implements DescribedTypeConstructor<DataSection>
{

    private static final Object[] DESCRIPTORS =
            {
                    Symbol.valueOf("amqp:data:binary"), UnsignedLong.valueOf(0x0000000000000075L),
            };

    private static final DataSectionConstructor INSTANCE = new DataSectionConstructor();

    public static void register(DescribedTypeConstructorRegistry registry)
    {
        for(Object descriptor : DESCRIPTORS)
        {
            registry.register(descriptor, INSTANCE);
        }
    }


    @Override
    public TypeConstructor<DataSection> construct(final Object descriptor,
                                                        final List<QpidByteBuffer> in,
                                                        final int[] originalPositions,
                                                        final ValueHandler valueHandler)
            throws AmqpErrorException
    {
        int constructorByte = QpidByteBufferUtils.get(in) & 0xff;
        int sizeBytes;
        switch(constructorByte)
        {
            case 0xa0:
                sizeBytes = 1;
                break;
            case 0xb0:
                sizeBytes = 4;
                break;
            default:
                throw new AmqpErrorException(ConnectionError.FRAMING_ERROR,
                                             "The described section must always be binary");
        }

        return new LazyConstructor(sizeBytes, originalPositions);
    }


    private class LazyConstructor implements TypeConstructor<DataSection>
    {

        private final int _sizeBytes;
        private final int[] _originalPositions;

        public LazyConstructor(final int sizeBytes,
                               final int[] originalPositions)
        {

            _sizeBytes = sizeBytes;
            _originalPositions = originalPositions;
        }

        @Override
        public DataSection construct(final List<QpidByteBuffer> in, final ValueHandler handler)
                throws AmqpErrorException
        {
            int size;
            switch(_sizeBytes)
            {
                case 1:
                    size = QpidByteBufferUtils.get(in) & 0xff;
                    break;
                case 4:
                    size = QpidByteBufferUtils.getInt(in);
                    break;
                default:
                    throw new AmqpErrorException(AmqpError.INVALID_FIELD, "Unexpected constructor type, can only be 1 or 4");
            }
            QpidByteBufferUtils.skip(in, size);

            List<QpidByteBuffer> encoding = new ArrayList<>();
            int offset = in.size() - _originalPositions.length;
            for(int i = offset; i < in.size(); i++)
            {
                QpidByteBuffer buf = in.get(i);
                if(buf.position() == _originalPositions[i-offset])
                {
                    if(buf.hasRemaining())
                    {
                        break;
                    }
                }
                else
                {
                    QpidByteBuffer dup = buf.duplicate();
                    dup.position(_originalPositions[i-offset]);
                    dup.limit(buf.position());
                    encoding.add(dup);
                }
            }
            DataSection object = new DataSection(((SectionDecoderRegistry)handler.getDescribedTypeRegistry()).getUnderlyingRegistry());
            object.setEncodedForm(encoding);
            return object;

        }
    }
}
