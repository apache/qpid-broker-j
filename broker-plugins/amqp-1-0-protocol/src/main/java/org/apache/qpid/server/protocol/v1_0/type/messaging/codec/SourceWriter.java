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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;

public class SourceWriter extends AbstractDescribedTypeWriter<Source>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x28);
    private static final Factory<Source> FACTORY = SourceWriter::new;

    private SourceWriter(final Registry registry, final Source object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<Source>
    {
        private int _field;
        private final Source _value;
        private final int _count;

        public Writer(final Registry registry, Source value)
        {
            super(registry);
            _value = value;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if( _value.getCapabilities() != null)
            {
                return 11;
            }

            if( _value.getOutcomes() != null)
            {
                return 10;
            }

            if( _value.getDefaultOutcome() != null)
            {
                return 9;
            }

            if( _value.getFilter() != null)
            {
                return 8;
            }

            if( _value.getDistributionMode() != null)
            {
                return 7;
            }

            if( _value.getDynamicNodeProperties() != null)
            {
                return 6;
            }

            if( _value.getDynamic() != null)
            {
                return 5;
            }

            if( _value.getTimeout() != null)
            {
                return 4;
            }

            if( _value.getExpiryPolicy() != null)
            {
                return 3;
            }

            if( _value.getDurable() != null)
            {
                return 2;
            }

            if( _value.getAddress() != null)
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
                    return _value.getAddress();

                case 1:
                    return _value.getDurable();

                case 2:
                    return _value.getExpiryPolicy();

                case 3:
                    return _value.getTimeout();

                case 4:
                    return _value.getDynamic();

                case 5:
                    return _value.getDynamicNodeProperties();

                case 6:
                    return _value.getDistributionMode();

                case 7:
                    return _value.getFilter();

                case 8:
                    return _value.getDefaultOutcome();

                case 9:
                    return _value.getOutcomes();

                case 10:
                    return _value.getCapabilities();

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

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(Source.class, FACTORY);
    }
}
