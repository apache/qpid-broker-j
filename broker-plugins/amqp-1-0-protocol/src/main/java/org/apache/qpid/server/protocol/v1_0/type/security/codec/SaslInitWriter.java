
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
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;

public class SaslInitWriter extends AbstractDescribedTypeWriter<SaslInit>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x41);

    private SaslInitWriter(final Registry registry, final SaslInit object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<SaslInit>
    {
        private final SaslInit _value;
        private final int _count;
        private int _field;

        public Writer(final Registry registry, SaslInit value)
        {
            super(registry);
            _value = value;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if( _value.getHostname() != null)
            {
                return 3;
            }

            if( _value.getInitialResponse() != null)
            {
                return 2;
            }

            if( _value.getMechanism() != null)
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
                    return _value.getMechanism();

                case 1:
                    return _value.getInitialResponse();

                case 2:
                    return _value.getHostname();

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

    private static final Factory<SaslInit> FACTORY = new Factory<SaslInit>()
    {

        @Override
        public ValueWriter<SaslInit> newInstance(final Registry registry, final SaslInit object)
        {
            return new SaslInitWriter(registry, object);
        }
    };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(SaslInit.class, FACTORY);
    }

}
