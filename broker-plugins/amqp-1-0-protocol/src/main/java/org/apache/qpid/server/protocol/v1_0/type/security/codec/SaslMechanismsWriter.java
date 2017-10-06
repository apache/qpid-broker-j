
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


package org.apache.qpid.server.protocol.v1_0.type.security.codec;

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeWriter;
import org.apache.qpid.server.protocol.v1_0.codec.AbstractListWriter;
import org.apache.qpid.server.protocol.v1_0.codec.UnsignedLongWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;

public class SaslMechanismsWriter extends AbstractDescribedTypeWriter<SaslMechanisms>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x40);

    private SaslMechanismsWriter(final Registry registry,
                                 final SaslMechanisms object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));

    }

    private static class Writer extends AbstractListWriter<SaslMechanisms>
    {
        private final SaslMechanisms _value;
        private final int _count;
        private int _field;

        public Writer(final Registry registry, SaslMechanisms object)
        {
            super(registry);
            _value = object;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if( _value.getSaslServerMechanisms() != null)
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
                    return _value.getSaslServerMechanisms();

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

    private static final Factory<SaslMechanisms> FACTORY = new Factory<SaslMechanisms>()
    {

        @Override
        public ValueWriter<SaslMechanisms> newInstance(final Registry registry, final SaslMechanisms object)
        {
            return new SaslMechanismsWriter(registry, object);
        }
    };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(SaslMechanisms.class, FACTORY);
    }

}
