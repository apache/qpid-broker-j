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
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public abstract class AbstractSection<T, S extends NonEncodingRetainingSection<T>> implements EncodingRetainingSection<T>
{
    private final DescribedTypeConstructorRegistry _typeRegistry;
    private T _value;
    private List<QpidByteBuffer> _encodedForm;

    protected AbstractSection(final DescribedTypeConstructorRegistry registry)
    {
        _typeRegistry = registry;
    }

    protected AbstractSection(final S section, final SectionEncoder encoder)
    {
        _value = section.getValue();
        _typeRegistry = encoder.getRegistry();
        _encodedForm = Collections.singletonList(encoder.encodeObject(section));
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
        _encodedForm = new ArrayList<>();
        for(QpidByteBuffer encodedChunk : encodedForm)
        {
            _encodedForm.add(encodedChunk.duplicate());
        }
    }

    @Override
    public synchronized final List<QpidByteBuffer> getEncodedForm()
    {
        List<QpidByteBuffer> returnVal = new ArrayList<>(_encodedForm.size());
        for(int i = 0; i < _encodedForm.size(); i++)
        {
            returnVal.add(_encodedForm.get(i).duplicate());
        }
        return returnVal;
    }

    @Override
    public synchronized final void dispose()
    {
        for(int i = 0; i < _encodedForm.size(); i++)
        {
            _encodedForm.get(i).dispose();
        }
        _encodedForm = null;

    }

    @Override
    public synchronized final void reallocate(final long smallestAllowedBufferId)
    {
        _encodedForm = QpidByteBuffer.reallocateIfNecessary(smallestAllowedBufferId, _encodedForm);
    }

    @Override
    public synchronized final long getEncodedSize()
    {
        return QpidByteBufferUtils.remaining(_encodedForm);
    }

    @Override
    public synchronized void writeTo(final QpidByteBuffer dest)
    {
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

    private S decode(AbstractDescribedTypeConstructor<S> constructor)
    {
        List<QpidByteBuffer> input = getEncodedForm();
        int[] originalPositions = new int[input.size()];
        for(int i = 0; i < input.size(); i++)
        {
            originalPositions[i] = input.get(i).position();
        }
        int describedByte = QpidByteBufferUtils.get(input);
        ValueHandler handler = new ValueHandler(_typeRegistry);
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
