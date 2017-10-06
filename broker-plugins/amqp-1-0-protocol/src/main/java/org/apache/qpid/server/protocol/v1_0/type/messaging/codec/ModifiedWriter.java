
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Modified;

public class ModifiedWriter extends AbstractDescribedTypeWriter<Modified>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x27);

    public ModifiedWriter(final Registry registry, final Modified object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }


    private static class Writer extends AbstractListWriter<Modified>
    {
        private int _field;
        private final int _count;
        private final Modified _value;

        public Writer(final Registry registry, Modified value)
        {
            super(registry);
            _value = value;
            _count = calculateCount();
        }

        private int calculateCount()
        {

            if( _value.getMessageAnnotations() != null)
            {
                return 3;
            }

            if( _value.getUndeliverableHere() != null)
            {
                return 2;
            }

            if( _value.getDeliveryFailed() != null)
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
                    return _value.getDeliveryFailed();

                case 1:
                    return _value.getUndeliverableHere();

                case 2:
                    return _value.getMessageAnnotations();

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

    private static final Factory<Modified> FACTORY = new Factory<Modified>()
    {

        @Override
        public ValueWriter<Modified> newInstance(final Registry registry, final Modified object)
        {
            return new ModifiedWriter(registry, object);
        }
    };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Modified.class, FACTORY);
    }

}
