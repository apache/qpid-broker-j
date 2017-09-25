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


import org.apache.qpid.server.protocol.v1_0.ConnectionHandler;
import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

@CompositeType( symbolicDescriptor = "amqp:disposition:list", numericDescriptor = 0x0000000000000015L)
public class Disposition implements FrameBody
{

    @CompositeTypeField(index = 0, mandatory = true)
    private Role _role;

    @CompositeTypeField(index = 1, mandatory = true)
    private UnsignedInteger _first;

    @CompositeTypeField(index = 2)
    private UnsignedInteger _last;

    @CompositeTypeField(index = 3)
    private Boolean _settled;

    @CompositeTypeField(index = 4)
    private DeliveryState _state;

    @CompositeTypeField(index = 5)
    private Boolean _batchable;

    public Role getRole()
    {
        return _role;
    }

    public void setRole(Role role)
    {
        _role = role;
    }

    public UnsignedInteger getFirst()
    {
        return _first;
    }

    public void setFirst(UnsignedInteger first)
    {
        _first = first;
    }

    public UnsignedInteger getLast()
    {
        return _last;
    }

    public void setLast(UnsignedInteger last)
    {
        _last = last;
    }

    public Boolean getSettled()
    {
        return _settled;
    }

    public void setSettled(Boolean settled)
    {
        _settled = settled;
    }

    public DeliveryState getState()
    {
        return _state;
    }

    public void setState(DeliveryState state)
    {
        _state = state;
    }

    public Boolean getBatchable()
    {
        return _batchable;
    }

    public void setBatchable(Boolean batchable)
    {
        _batchable = batchable;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Disposition{");
        final int origLength = builder.length();

        if (_role != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("role=").append(_role);
        }

        if (_first != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("first=").append(_first);
        }

        if (_last != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("last=").append(_last);
        }

        if (_settled != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("settled=").append(_settled);
        }

        if (_state != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("state=").append(_state);
        }

        if (_batchable != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("batchable=").append(_batchable);
        }

        builder.append('}');
        return builder.toString();
    }

    @Override
    public void invoke(int channel, ConnectionHandler conn)
    {
        conn.receiveDisposition(channel, this);
    }
}
