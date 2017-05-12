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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
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
    private List<QpidByteBuffer> _encodedForm;

    protected AbstractSection()
    {
    }

    protected AbstractSection(final S section)
    {
        _value = section.getValue();
        _section = section;
        _encodedForm = encode();
    }

    protected AbstractSection(final AbstractSection<T, S> otherAbstractSection)
    {
        _value = otherAbstractSection.getValue();
        _section = otherAbstractSection._section;
        _encodedForm = otherAbstractSection.getEncodedForm();
    }

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
    public synchronized final void setEncodedForm(final List<QpidByteBuffer> encodedForm)
    {
        clearEncodedForm();
        _encodedForm = new ArrayList<>();
        for(QpidByteBuffer encodedChunk : encodedForm)
        {
            _encodedForm.add(encodedChunk.duplicate());
        }
    }

    @Override
    public synchronized final List<QpidByteBuffer> getEncodedForm()
    {
        if (_encodedForm == null)
        {
            _encodedForm = encode();
        }
        List<QpidByteBuffer> returnVal = new ArrayList<>(_encodedForm.size());
        for(int i = 0; i < _encodedForm.size(); i++)
        {
            returnVal.add(_encodedForm.get(i).duplicate());
        }
        return returnVal;
    }

    @Override
    public synchronized void clearEncodedForm()
    {
        if (_encodedForm != null)
        {
            _section = decode(createNonEncodingRetainingSectionConstructor());
            for (int i = 0; i < _encodedForm.size(); i++)
            {
                _encodedForm.get(i).dispose();
            }
            _encodedForm = null;
        }
    }

    @Override
    public synchronized final void dispose()
    {
        clearEncodedForm();
    }

    @Override
    public synchronized final void reallocate()
    {
        _encodedForm = QpidByteBuffer.reallocateIfNecessary(_encodedForm);
    }

    @Override
    public synchronized final long getEncodedSize()
    {
        if (_encodedForm == null)
        {
            _encodedForm = encode();
        }
        return QpidByteBufferUtils.remaining(_encodedForm);
    }

    @Override
    public synchronized void writeTo(final QpidByteBuffer dest)
    {
        if (_encodedForm == null)
        {
            _encodedForm = encode();
        }
        for(QpidByteBuffer buf : _encodedForm)
        {
            dest.putCopyOf(buf);
        }
    }

    @Override
    public String toString()
    {
        return getValue().toString();
    }

    protected abstract AbstractDescribedTypeConstructor<S> createNonEncodingRetainingSectionConstructor();

    private List<QpidByteBuffer> encode()
    {
        SectionEncoder sectionEncoder = new SectionEncoderImpl(TYPE_REGISTRY);
        return Collections.singletonList(sectionEncoder.encodeObject(_section));
    }

    private S decode(AbstractDescribedTypeConstructor<S> constructor)
    {
        List<QpidByteBuffer> input = getEncodedForm();
        int[] originalPositions = new int[input.size()];
        for(int i = 0; i < input.size(); i++)
        {
            originalPositions[i] = input.get(i).position();
        }
        int describedByte = QpidByteBufferUtils.get(input);
        ValueHandler handler = new ValueHandler(TYPE_REGISTRY);
        try
        {
            Object descriptor = handler.parse(input);
            return constructor.construct(descriptor, input, originalPositions, handler).construct(input, handler);
        }
        catch (AmqpErrorException e)
        {
            throw new ConnectionScopedRuntimeException("Cannot decode section", e);
        }
        finally
        {
            for (QpidByteBuffer anInput : input)
            {
                anInput.dispose();
            }

        }
    }

}
