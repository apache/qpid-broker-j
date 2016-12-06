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
package org.apache.qpid.server.protocol.v1_0.codec;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;

public class CompoundTypeConstructor<T> extends VariableWidthTypeConstructor<T>
{
    private final CompoundTypeAssembler.Factory<T> _assemblerFactory;

    public static final CompoundTypeAssembler.Factory LIST_ASSEMBLER_FACTORY =
            new CompoundTypeAssembler.Factory()
            {

                public CompoundTypeAssembler newInstance()
                {
                    return new ListAssembler();
                }
            };



    private static class ListAssembler implements CompoundTypeAssembler<List>
    {
        private List _list;

        public void init(final int count) throws AmqpErrorException
        {
            _list = new ArrayList(count);
        }

        public void addItem(final Object obj) throws AmqpErrorException
        {
            _list.add(obj);
        }

        public List complete() throws AmqpErrorException
        {
            return _list;
        }

        @Override
        public String toString()
        {
            return "ListAssembler{" +
                   "_list=" + _list +
                   '}';
        }
    }


    public static final CompoundTypeAssembler.Factory MAP_ASSEMBLER_FACTORY =
            new CompoundTypeAssembler.Factory<Map>()
            {

                public CompoundTypeAssembler<Map> newInstance()
                {
                    return new MapAssembler();
                }
            };

    private static class MapAssembler implements CompoundTypeAssembler<Map>
    {
        private Map _map;
        private Object _lastKey;
        private static final Object NOT_A_KEY = new Object();


        public void init(final int count) throws AmqpErrorException
        {
            // Can't have an odd number of elements in a map
            if((count & 0x1) == 1)
            {
                Error error = new Error();
                error.setCondition(AmqpError.DECODE_ERROR);
                Formatter formatter = new Formatter();
                formatter.format("map cannot have odd number of elements: %d", count);
                error.setDescription(formatter.toString());
                throw new AmqpErrorException(error);
            }
            _map = new HashMap(count);
            _lastKey = NOT_A_KEY;
        }

        public void addItem(final Object obj) throws AmqpErrorException
        {
            if(_lastKey != NOT_A_KEY)
            {
                if(_map.put(_lastKey, obj) != null)
                {
                    Error error = new Error();
                    error.setCondition(AmqpError.DECODE_ERROR);
                    Formatter formatter = new Formatter();
                    formatter.format("map cannot have duplicate keys: %s has values (%s, %s)", _lastKey, _map.get(_lastKey), obj);
                    error.setDescription(formatter.toString());

                    throw new AmqpErrorException(error);
                }
                _lastKey = NOT_A_KEY;
            }
            else
            {
                _lastKey = obj;
            }

        }

        public Map complete() throws AmqpErrorException
        {
            return _map;
        }
    }


    public static <X> CompoundTypeConstructor<X> getInstance(int i,
                                                      CompoundTypeAssembler.Factory<X> assemblerFactory)
    {
        return new CompoundTypeConstructor<>(i, assemblerFactory);
    }


    private CompoundTypeConstructor(int size,
                                    final CompoundTypeAssembler.Factory<T> assemblerFactory)
    {
        super(size);
        _assemblerFactory = assemblerFactory;
    }

    @Override
    public T construct(final List<QpidByteBuffer> in, final ValueHandler handler) throws AmqpErrorException
    {
        int size;
        int count;

        if(getSize() == 1)
        {
            size = QpidByteBufferUtils.get(in) & 0xFF;
            count = QpidByteBufferUtils.get(in) & 0xFF;
        }
        else
        {
            size = QpidByteBufferUtils.getInt(in);
            count = QpidByteBufferUtils.getInt(in);
        }

        CompoundTypeAssembler<T> assembler = _assemblerFactory.newInstance();

        assembler.init(count);

        for(int i = 0; i < count; i++)
        {
            assembler.addItem(handler.parse(in));
        }

        return assembler.complete();
    }
}
