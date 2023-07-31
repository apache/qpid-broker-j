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

package org.apache.qpid.server.protocol.v1_0.type.transaction.codec;

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeWriter;
import org.apache.qpid.server.protocol.v1_0.codec.AbstractListWriter;
import org.apache.qpid.server.protocol.v1_0.codec.UnsignedLongWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;

public class DischargeWriter extends AbstractDescribedTypeWriter<Discharge>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x32);
    private static final Factory<Discharge> FACTORY = DischargeWriter::new;

    private DischargeWriter(final Registry registry, final Discharge object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<Discharge>
    {
        private final Discharge _value;
        private int _count = -1;

        private int _field;

        public Writer(final Registry registry, final Discharge object)
        {
            super(registry);

            _value = object;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if( _value.getFail() != null)
            {
                return 2;
            }

            if( _value.getTxnId() != null)
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
                    return _value.getTxnId();

                case 1:
                    return _value.getFail();

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
        registry.register(Discharge.class, FACTORY);
    }
}
