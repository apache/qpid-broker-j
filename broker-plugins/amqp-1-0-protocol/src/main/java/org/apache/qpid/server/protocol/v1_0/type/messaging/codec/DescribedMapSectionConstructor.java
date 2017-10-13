
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
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.TypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AbstractSection;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;

public abstract class DescribedMapSectionConstructor<S extends AbstractSection> implements DescribedTypeConstructor<S>
{

    @Override
    public TypeConstructor<S> construct(final Object descriptor,
                                        final QpidByteBuffer in,
                                        final int originalPosition,
                                        final ValueHandler valueHandler)
            throws AmqpErrorException
    {
        if (!in.hasRemaining())
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode section.");
        }
        int constructorByte = in.getUnsignedByte();
        int sizeBytes;
        switch(constructorByte)
        {
            case 0xc1:
                sizeBytes = 1;
                break;
            case 0xd1:
                sizeBytes = 4;
                break;
            default:
                throw new AmqpErrorException(ConnectionError.FRAMING_ERROR,
                                             "The described section must always be a map");
        }

        return new LazyConstructor(sizeBytes, originalPosition);
    }


    private class LazyConstructor extends AbstractLazyConstructor<S>
    {
        private final int _sizeBytes;

        LazyConstructor(final int sizeBytes, final int originalPosition)
        {
            super(originalPosition);
            _sizeBytes = sizeBytes;
        }

        @Override
        protected S createObject(final QpidByteBuffer encoding, final ValueHandler handler)
        {
            return DescribedMapSectionConstructor.this.createObject(((SectionDecoderRegistry) handler.getDescribedTypeRegistry())
                                                                            .getUnderlyingRegistry(), encoding);
        }

        @Override
        protected void skipValue(final QpidByteBuffer in) throws AmqpErrorException
        {
            if (!in.hasRemaining(_sizeBytes))
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode section.");
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
                throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Insufficient data to decode section.");
            }
            in.position(in.position() + size);
        }
    }

    protected abstract S createObject(final DescribedTypeConstructorRegistry describedTypeRegistry,
                                      final QpidByteBuffer encodedForm);
}
