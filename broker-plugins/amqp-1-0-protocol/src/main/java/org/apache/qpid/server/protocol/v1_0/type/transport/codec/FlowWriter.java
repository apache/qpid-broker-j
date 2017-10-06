
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


package org.apache.qpid.server.protocol.v1_0.type.transport.codec;

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeWriter;
import org.apache.qpid.server.protocol.v1_0.codec.AbstractListWriter;
import org.apache.qpid.server.protocol.v1_0.codec.UnsignedLongWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;

public class FlowWriter extends AbstractDescribedTypeWriter<Flow>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x13);

    private FlowWriter(final Registry registry, final Flow object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));

    }

    private static class Writer extends AbstractListWriter<Flow>
    {
        private final Flow _value;
        private final int _count;
        private int _field;

        public Writer(final Registry registry, final Flow object)
        {
            super(registry);

            _value = object;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if( _value.getProperties() != null)
            {
                return 11;
            }

            if( _value.getEcho() != null)
            {
                return 10;
            }

            if( _value.getDrain() != null)
            {
                return 9;
            }

            if( _value.getAvailable() != null)
            {
                return 8;
            }

            if( _value.getLinkCredit() != null)
            {
                return 7;
            }

            if( _value.getDeliveryCount() != null)
            {
                return 6;
            }

            if( _value.getHandle() != null)
            {
                return 5;
            }

            if( _value.getOutgoingWindow() != null)
            {
                return 4;
            }

            if( _value.getNextOutgoingId() != null)
            {
                return 3;
            }

            if( _value.getIncomingWindow() != null)
            {
                return 2;
            }

            if( _value.getNextIncomingId() != null)
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
                    return _value.getNextIncomingId();

                case 1:
                    return _value.getIncomingWindow();

                case 2:
                    return _value.getNextOutgoingId();

                case 3:
                    return _value.getOutgoingWindow();

                case 4:
                    return _value.getHandle();

                case 5:
                    return _value.getDeliveryCount();

                case 6:
                    return _value.getLinkCredit();

                case 7:
                    return _value.getAvailable();

                case 8:
                    return _value.getDrain();

                case 9:
                    return _value.getEcho();

                case 10:
                    return _value.getProperties();

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

    private static final Factory<Flow> FACTORY = new Factory<Flow>()
    {

        @Override
        public ValueWriter<Flow> newInstance(final Registry registry, final Flow object)
        {
            return new FlowWriter(registry, object);
        }
    };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Flow.class, FACTORY);
    }

}
