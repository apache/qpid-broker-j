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


package org.apache.qpid.server.protocol.v1_0.type.transport;


import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;

public class ReceiverSettleMode implements RestrictedType<UnsignedByte>
{
    public static final ReceiverSettleMode FIRST = new ReceiverSettleMode(UnsignedByte.valueOf((byte) 0));
    public static final ReceiverSettleMode SECOND = new ReceiverSettleMode(UnsignedByte.valueOf((byte) 1));

    private final UnsignedByte _val;


    private ReceiverSettleMode(UnsignedByte val)
    {
        _val = val;
    }

    public static ReceiverSettleMode valueOf(Object obj)
    {
        if (obj instanceof UnsignedByte)
        {
            UnsignedByte val = (UnsignedByte) obj;
            if (FIRST._val.equals(val))
            {
                return FIRST;
            }

            if (SECOND._val.equals(val))
            {
                return SECOND;
            }
        }

        final String message = String.format("Cannot convert '%s' into 'receiver-settle-mode'", obj);
        throw new IllegalArgumentException(message);
    }

    @Override
    public UnsignedByte getValue()
    {
        return _val;
    }

    @Override
    public String toString()
    {

        if (this == FIRST)
        {
            return "first";
        }

        if (this == SECOND)
        {
            return "second";
        }

        else
        {
            return String.valueOf(_val);
        }
    }
}
