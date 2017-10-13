
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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
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
                                                        final QpidByteBuffer in,
                                                        final int originalPosition,
                                                        final ValueHandler valueHandler)
            throws AmqpErrorException
    {
        if (!in.hasRemaining())
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode data section.");
        }
        int constructorByte = in.getUnsignedByte();
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

        return new LazyConstructor(sizeBytes, originalPosition);
    }


    private class LazyConstructor extends AbstractLazyConstructor<DataSection>
    {
        private final int _sizeBytes;

        public LazyConstructor(final int sizeBytes, final int originalPosition)
        {
            super(originalPosition);
            _sizeBytes = sizeBytes;
        }

        @Override
        protected DataSection createObject(final QpidByteBuffer encoding, final ValueHandler handler)
        {
            return new DataSection(encoding);
        }

        @Override
        protected void skipValue(final QpidByteBuffer in) throws AmqpErrorException
        {
            if (!in.hasRemaining(_sizeBytes))
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode data section.");
            }
            int size;
            switch(_sizeBytes)
            {
                case 1:
                    size = in.getUnsignedByte();
                    break;
                case 4:
                    size = in.getInt();
                    break;
                default:
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Unexpected constructor type, can only be 1 or 4");
            }
            if (!in.hasRemaining(size))
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode data section.");
            }
            in.position(in.position() + size);
        }
    }
}
