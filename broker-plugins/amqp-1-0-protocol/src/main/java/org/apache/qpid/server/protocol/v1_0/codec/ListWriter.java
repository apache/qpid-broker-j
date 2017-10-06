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

import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class ListWriter
{
    private static class NonEmptyListWriter extends AbstractListWriter<List>
    {
        private final List _list;
        private int _position = 0;

        public NonEmptyListWriter(final Registry registry, final List object)
        {
            super(registry, object);
            _list = object;
        }

        @Override
        protected int getCount()
        {
            return _list.size();
        }

        @Override
        protected boolean hasNext()
        {
            return _position < getCount();
        }

        @Override
        protected Object next()
        {
            return _list.get(_position++);
        }

        @Override
        protected void reset()
        {
            _position = 0;
        }

    }

    private static final byte ZERO_BYTE_FORMAT_CODE = (byte) 0x45;

    public static final ValueWriter<List> EMPTY_LIST_WRITER = new EmptyListValueWriter();

    public static class EmptyListValueWriter implements ValueWriter<List>
    {

        @Override
        public int getEncodedSize()
        {
            return 1;
        }

        @Override
        public void writeToBuffer(QpidByteBuffer buffer)
        {
            buffer.put(ZERO_BYTE_FORMAT_CODE);
        }
    }

    private static final ValueWriter.Factory<List> FACTORY =
            new ValueWriter.Factory<List>()
            {

                @Override
                public ValueWriter<List> newInstance(final ValueWriter.Registry registry,
                                                     final List object)
                {
                    return object.isEmpty() ? EMPTY_LIST_WRITER : new NonEmptyListWriter(registry, object);
                }
            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(List.class, FACTORY);
    }

}
