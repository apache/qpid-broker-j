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

import java.util.List;
import java.util.Map;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.codec.MessageAnnotationsConstructor;

public class MessageAnnotationsSection extends AbstractSection<Map<Symbol,Object>>
{
    private Map<Symbol,Object> _value;
    private final DescribedTypeConstructorRegistry _typeRegistry;

    public MessageAnnotationsSection(DescribedTypeConstructorRegistry registry)
    {
        _typeRegistry = registry;
    }

    public MessageAnnotationsSection(final MessageAnnotations messageAnnotations,
                                     final List<QpidByteBuffer> encodedForm,
                                     final DescribedTypeConstructorRegistry registry)
    {
        _value = messageAnnotations.getValue();
        _typeRegistry = registry;
        setEncodedForm(encodedForm);
    }

    @Override
    public synchronized Map<Symbol,Object> getValue()
    {
        if(_value == null)
        {
            decode();
        }
        return _value;
    }


    private void decode()
    {
        try
        {

            List<QpidByteBuffer> input = getEncodedForm();
            int[] originalPositions = new int[input.size()];
            for(int i = 0; i < input.size(); i++)
            {
                originalPositions[i] = input.get(i).position();
            }
            int describedByte = QpidByteBufferUtils.get(input);
            ValueHandler handler = new ValueHandler(_typeRegistry);
            Object descriptor = handler.parse(input);
            MessageAnnotationsConstructor constructor = new MessageAnnotationsConstructor();
            _value = constructor.construct(descriptor, input, originalPositions, handler).construct(input, handler).getValue();
            for(int i = 0; i < input.size(); i++)
            {
                input.get(i).dispose();
            }

        }
        catch (AmqpErrorException e)
        {
            // TODO
            e.printStackTrace();
        }
        // TODO
    }
}
