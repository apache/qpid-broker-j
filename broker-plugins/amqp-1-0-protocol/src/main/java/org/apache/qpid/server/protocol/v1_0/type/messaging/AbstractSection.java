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
package org.apache.qpid.server.protocol.v1_0.type.messaging;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public abstract class AbstractSection<T, S extends NonEncodingRetainingSection<T>> implements EncodingRetainingSection<T>
{
    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
                                                                                            .registerTransportLayer()
                                                                                            .registerMessagingLayer()
                                                                                            .registerTransactionLayer()
                                                                                            .registerSecurityLayer();
    private T _value;

    private S _section;
    private QpidByteBuffer _encodedForm;
    // _encodedSize is valid only when _encodedForm is non-null
    private long _encodedSize = 0;

    protected AbstractSection(final QpidByteBuffer encodedForm)
    {
        _encodedForm = encodedForm.duplicate();
        _encodedSize = encodedForm.remaining();
    }

    protected AbstractSection(final S section)
    {
        _value = section.getValue();
        _section = section;
        encodeIfNecessary();
    }

    protected AbstractSection(final AbstractSection<T, S> otherAbstractSection)
    {
        _value = otherAbstractSection.getValue();
        _section = otherAbstractSection._section;
        _encodedForm = otherAbstractSection.getEncodedForm();
        _encodedSize = _encodedForm.remaining();
    }

    protected abstract DescribedTypeConstructor<S> createNonEncodingRetainingSectionConstructor();

    @Override
    public synchronized T getValue()
    {
        if(_value == null)
        {
            S section = decode(createNonEncodingRetainingSectionConstructor());
            _value = section.getValue();
        }
        return _value;
    }

    @Override
    public synchronized final QpidByteBuffer getEncodedForm()
    {
        encodeIfNecessary();
        return _encodedForm.duplicate();
    }

    @Override
    public synchronized final long getEncodedSize()
    {
        encodeIfNecessary();
        return _encodedSize;
    }

    @Override
    public synchronized void writeTo(final QpidByteBuffer dest)
    {
        encodeIfNecessary();
        dest.putCopyOf(_encodedForm);
    }

    @Override
    public synchronized void clearEncodedForm()
    {
        if (_encodedForm != null)
        {
            _section = decode(createNonEncodingRetainingSectionConstructor());
            _encodedForm.dispose();
            _encodedForm = null;
            _encodedSize = 0;
        }
    }

    @Override
    public synchronized final void reallocate()
    {
        _encodedForm = QpidByteBuffer.reallocateIfNecessary(_encodedForm);
    }

    @Override
    public synchronized final void dispose()
    {
        _section = null;
        _value = null;
        if (_encodedForm != null)
        {
            _encodedForm.dispose();
            _encodedForm = null;
            _encodedSize = 0;
        }
    }

    @Override
    public String toString()
    {
        if (_value == null)
        {
            return String.format("<Undecoded %s (%d bytes)>", getClass().getSimpleName(), _encodedSize);
        }
        else
        {
            return getValue().toString();
        }
    }

    private void encodeIfNecessary()
    {
        if (_encodedForm == null)
        {
            SectionEncoder sectionEncoder = new SectionEncoderImpl(TYPE_REGISTRY);
            _encodedForm = sectionEncoder.encodeObject(_section);
            _encodedSize = _encodedForm.remaining();
        }
    }

    private S decode(DescribedTypeConstructor<S> constructor)
    {
        try (QpidByteBuffer input = getEncodedForm())
        {

            int originalPosition = input.position();
            int describedByte = input.get();
            if (describedByte != ValueHandler.DESCRIBED_TYPE)
            {
                throw new ConnectionScopedRuntimeException("Cannot decode section",
                                                           new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                                                  "Not a described type."));
            }
            ValueHandler handler = new ValueHandler(TYPE_REGISTRY);
            try
            {
                Object descriptor = handler.parse(input);
                return constructor.construct(descriptor, input, originalPosition, handler).construct(input, handler);
            }
            catch (AmqpErrorException e)
            {
                throw new ConnectionScopedRuntimeException("Cannot decode section", e);
            }
        }
    }

}
