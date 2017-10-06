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


import java.util.Arrays;
import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.ConnectionHandler;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;

@CompositeType( symbolicDescriptor = "amqp:attach:list", numericDescriptor = 0x0000000000000012L)
public class Attach implements FrameBody
{

    @CompositeTypeField(index = 0, mandatory = true)
    private String _name;

    @CompositeTypeField(index = 1, mandatory = true)
    private UnsignedInteger _handle;

    @CompositeTypeField(index = 2, mandatory = true)
    private Role _role;

    @CompositeTypeField(index = 3)
    private SenderSettleMode _sndSettleMode;

    @CompositeTypeField(index = 4)
    private ReceiverSettleMode _rcvSettleMode;

    @CompositeTypeField(index = 5)
    private BaseSource _source;

    @CompositeTypeField(index = 6)
    private BaseTarget _target;

    @CompositeTypeField(index = 7)
    private Map<Binary, DeliveryState> _unsettled;

    @CompositeTypeField(index = 8)
    private Boolean _incompleteUnsettled;

    @CompositeTypeField(index = 9)
    private UnsignedInteger _initialDeliveryCount;

    @CompositeTypeField(index = 10)
    private UnsignedLong _maxMessageSize;

    @CompositeTypeField(index = 11)
    private Symbol[] _offeredCapabilities;

    @CompositeTypeField(index = 12)
    private Symbol[] _desiredCapabilities;

    @CompositeTypeField(index = 13)
    private Map<Symbol, Object> _properties;

    public String getName()
    {
        return _name;
    }

    public void setName(String name)
    {
        _name = name;
    }

    public UnsignedInteger getHandle()
    {
        return _handle;
    }

    public void setHandle(UnsignedInteger handle)
    {
        _handle = handle;
    }

    public Role getRole()
    {
        return _role;
    }

    public void setRole(Role role)
    {
        _role = role;
    }

    public SenderSettleMode getSndSettleMode()
    {
        return _sndSettleMode;
    }

    public void setSndSettleMode(SenderSettleMode sndSettleMode)
    {
        _sndSettleMode = sndSettleMode;
    }

    public ReceiverSettleMode getRcvSettleMode()
    {
        return _rcvSettleMode;
    }

    public void setRcvSettleMode(ReceiverSettleMode rcvSettleMode)
    {
        _rcvSettleMode = rcvSettleMode;
    }

    public BaseSource getSource()
    {
        return _source;
    }

    public void setSource(BaseSource source)
    {
        _source = source;
    }

    public BaseTarget getTarget()
    {
        return _target;
    }

    public void setTarget(BaseTarget target)
    {
        _target = target;
    }

    public Map<Binary, DeliveryState> getUnsettled()
    {
        return _unsettled;
    }

    public void setUnsettled(Map<Binary, DeliveryState> unsettled)
    {
        _unsettled = unsettled;
    }

    public Boolean getIncompleteUnsettled()
    {
        return _incompleteUnsettled;
    }

    public void setIncompleteUnsettled(Boolean incompleteUnsettled)
    {
        _incompleteUnsettled = incompleteUnsettled;
    }

    public UnsignedInteger getInitialDeliveryCount()
    {
        return _initialDeliveryCount;
    }

    public void setInitialDeliveryCount(UnsignedInteger initialDeliveryCount)
    {
        _initialDeliveryCount = initialDeliveryCount;
    }

    public UnsignedLong getMaxMessageSize()
    {
        return _maxMessageSize;
    }

    public void setMaxMessageSize(UnsignedLong maxMessageSize)
    {
        _maxMessageSize = maxMessageSize;
    }

    public Symbol[] getOfferedCapabilities()
    {
        return _offeredCapabilities;
    }

    public void setOfferedCapabilities(Symbol[] offeredCapabilities)
    {
        _offeredCapabilities = offeredCapabilities;
    }

    public Symbol[] getDesiredCapabilities()
    {
        return _desiredCapabilities;
    }

    public void setDesiredCapabilities(Symbol[] desiredCapabilities)
    {
        _desiredCapabilities = desiredCapabilities;
    }

    public Map<Symbol, Object> getProperties()
    {
        return _properties;
    }

    public void setProperties(Map<Symbol, Object> properties)
    {
        _properties = properties;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Attach{");
        final int origLength = builder.length();

        if (_name != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("name=").append(_name);
        }

        if (_handle != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("handle=").append(_handle);
        }

        if (_role != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("role=").append(_role);
        }

        if (_sndSettleMode != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("sndSettleMode=").append(_sndSettleMode);
        }

        if (_rcvSettleMode != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("rcvSettleMode=").append(_rcvSettleMode);
        }

        if (_source != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("source=").append(_source);
        }

        if (_target != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("target=").append(_target);
        }

        if (_unsettled != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("unsettled=").append(_unsettled);
        }

        if (_incompleteUnsettled != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("incompleteUnsettled=").append(_incompleteUnsettled);
        }

        if (_initialDeliveryCount != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("initialDeliveryCount=").append(_initialDeliveryCount);
        }

        if (_maxMessageSize != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("maxMessageSize=").append(_maxMessageSize);
        }

        if (_offeredCapabilities != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("offeredCapabilities=").append(Arrays.toString(_offeredCapabilities));
        }

        if (_desiredCapabilities != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("desiredCapabilities=").append(Arrays.toString(_desiredCapabilities));
        }

        if (_properties != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("properties=").append(_properties);
        }

        builder.append('}');
        return builder.toString();
    }

    @Override
    public void invoke(int channel, ConnectionHandler conn)
    {
        conn.receiveAttach(channel, this);
    }
}
