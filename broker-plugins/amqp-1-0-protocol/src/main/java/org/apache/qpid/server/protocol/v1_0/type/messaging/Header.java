
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


package org.apache.qpid.server.protocol.v1_0.type.messaging;


import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

@CompositeType( symbolicDescriptor = "amqp:header:list", numericDescriptor = 0x0000000000000070L)
public class Header implements NonEncodingRetainingSection<Header>
{

    @CompositeTypeField(index = 0)
    private Boolean _durable;

    @CompositeTypeField(index = 1)
    private UnsignedByte _priority;

    @CompositeTypeField(index = 2)
    private UnsignedInteger _ttl;

    @CompositeTypeField(index = 3)
    private Boolean _firstAcquirer;

    @CompositeTypeField(index = 4)
    private UnsignedInteger _deliveryCount;

    public Boolean getDurable()
    {
        return _durable;
    }

    public void setDurable(Boolean durable)
    {
        _durable = durable;
    }

    public UnsignedByte getPriority()
    {
        return _priority;
    }

    public void setPriority(UnsignedByte priority)
    {
        _priority = priority;
    }

    public UnsignedInteger getTtl()
    {
        return _ttl;
    }

    public void setTtl(UnsignedInteger ttl)
    {
        _ttl = ttl;
    }

    public Boolean getFirstAcquirer()
    {
        return _firstAcquirer;
    }

    public void setFirstAcquirer(Boolean firstAcquirer)
    {
        _firstAcquirer = firstAcquirer;
    }

    public UnsignedInteger getDeliveryCount()
    {
        return _deliveryCount;
    }

    public void setDeliveryCount(UnsignedInteger deliveryCount)
    {
        _deliveryCount = deliveryCount;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Header{");
        final int origLength = builder.length();

        if (_durable != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("durable=").append(_durable);
        }

        if (_priority != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("priority=").append(_priority);
        }

        if (_ttl != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("ttl=").append(_ttl);
        }

        if (_firstAcquirer != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("firstAcquirer=").append(_firstAcquirer);
        }

        if (_deliveryCount != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("deliveryCount=").append(_deliveryCount);
        }

        builder.append('}');
        return builder.toString();
    }

    @Override
    public Header getValue()
    {
        return this;
    }

    @Override
    public HeaderSection createEncodingRetainingSection()
    {
        return new HeaderSection(this);
    }
}
