
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
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;

public class OpenWriter extends AbstractDescribedTypeWriter<Open>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x10);

    private OpenWriter(final Registry registry, final Open object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<Open>
    {
        private final Open _value;
        private final int _count;

        private int _field;

        public Writer(final Registry registry, final Open object)
        {
            super(registry);
            _value = object;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if( _value.getProperties() != null)
            {
                return 10;
            }

            if( _value.getDesiredCapabilities() != null)
            {
                return 9;
            }

            if( _value.getOfferedCapabilities() != null)
            {
                return 8;
            }

            if( _value.getIncomingLocales() != null)
            {
                return 7;
            }

            if( _value.getOutgoingLocales() != null)
            {
                return 6;
            }

            if( _value.getIdleTimeOut() != null)
            {
                return 5;
            }

            if( _value.getChannelMax() != null)
            {
                return 4;
            }

            if( _value.getMaxFrameSize() != null)
            {
                return 3;
            }

            if( _value.getHostname() != null)
            {
                return 2;
            }

            if( _value.getContainerId() != null)
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
                    return _value.getContainerId();

                case 1:
                    return _value.getHostname();

                case 2:
                    return _value.getMaxFrameSize();

                case 3:
                    return _value.getChannelMax();

                case 4:
                    return _value.getIdleTimeOut();

                case 5:
                    return _value.getOutgoingLocales();

                case 6:
                    return _value.getIncomingLocales();

                case 7:
                    return _value.getOfferedCapabilities();

                case 8:
                    return _value.getDesiredCapabilities();

                case 9:
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

    private static final Factory<Open> FACTORY = new Factory<Open>()
    {

        @Override
        public ValueWriter<Open> newInstance(final Registry registry, final Open object)
        {
            return new OpenWriter(registry, object);
        }
    };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Open.class, FACTORY);
    }

}
