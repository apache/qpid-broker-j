
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

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeWriter;
import org.apache.qpid.server.protocol.v1_0.codec.AbstractListWriter;
import org.apache.qpid.server.protocol.v1_0.codec.UnsignedLongWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;

public class HeaderWriter extends AbstractDescribedTypeWriter<Header>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x70);

    public HeaderWriter(final Registry registry, final Header object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }


    private static class Writer extends AbstractListWriter<Header>
    {
        private final int _count;
        private final Header _value;
        private int _field;

        public Writer(final Registry registry, Header value)
        {
            super(registry);
            _value = value;
            _count = calculateCount();
        }

        private int calculateCount()
        {


            if( _value.getDeliveryCount() != null)
            {
                return 5;
            }

            if( _value.getFirstAcquirer() != null)
            {
                return 4;
            }

            if( _value.getTtl() != null)
            {
                return 3;
            }

            if( _value.getPriority() != null)
            {
                return 2;
            }

            if( _value.getDurable() != null)
            {
                return 1;
            }

            return 0;
        }

        @Override
        protected int getCount()
        {
            return _count;
        }

        @Override
        protected boolean hasNext()
        {
            return _field < _count;
        }

        @Override
        protected Object next()
        {
            switch(_field++)
            {

                case 0:
                    return _value.getDurable();

                case 1:
                    return _value.getPriority();

                case 2:
                    return _value.getTtl();

                case 3:
                    return _value.getFirstAcquirer();

                case 4:
                    return _value.getDeliveryCount();

                default:
                    return null;
            }
        }

        @Override
        protected void reset()
        {
            _field = 0;
        }
    }

    private static final Factory<Header> FACTORY = new Factory<Header>()
    {

        @Override
        public ValueWriter<Header> newInstance(final Registry registry, final Header object)
        {
            return new HeaderWriter(registry, object);
        }
    };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Header.class, FACTORY);
    }

}
