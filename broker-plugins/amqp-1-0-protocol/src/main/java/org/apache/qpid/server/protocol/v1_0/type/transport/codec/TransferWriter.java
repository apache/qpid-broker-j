
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
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class TransferWriter extends AbstractDescribedTypeWriter<Transfer>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x14);


    private TransferWriter(final Registry registry, final Transfer object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<Transfer>
    {
        private final Transfer _value;
        private final int _count;

        private int _field;

        public Writer(final Registry registry, final Transfer object)
        {
            super(registry);

            _value = object;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if( _value.getBatchable() != null)
            {
                return 11;
            }

            if( _value.getAborted() != null)
            {
                return 10;
            }

            if( _value.getResume() != null)
            {
                return 9;
            }

            if( _value.getState() != null)
            {
                return 8;
            }

            if( _value.getRcvSettleMode() != null)
            {
                return 7;
            }

            if( _value.getMore() != null)
            {
                return 6;
            }

            if( _value.getSettled() != null)
            {
                return 5;
            }

            if( _value.getMessageFormat() != null)
            {
                return 4;
            }

            if( _value.getDeliveryTag() != null)
            {
                return 3;
            }

            if( _value.getDeliveryId() != null)
            {
                return 2;
            }

            if( _value.getHandle() != null)
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
                    return _value.getHandle();

                case 1:
                    return _value.getDeliveryId();

                case 2:
                    return _value.getDeliveryTag();

                case 3:
                    return _value.getMessageFormat();

                case 4:
                    return _value.getSettled();

                case 5:
                    return _value.getMore();

                case 6:
                    return _value.getRcvSettleMode();

                case 7:
                    return _value.getState();

                case 8:
                    return _value.getResume();

                case 9:
                    return _value.getAborted();

                case 10:
                    return _value.getBatchable();

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

    private static final Factory<Transfer> FACTORY = new Factory<Transfer>()
    {

        @Override
        public ValueWriter<Transfer> newInstance(final Registry registry, final Transfer object)
        {
            return new TransferWriter(registry, object);
        }
    };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Transfer.class, FACTORY);
    }

}
