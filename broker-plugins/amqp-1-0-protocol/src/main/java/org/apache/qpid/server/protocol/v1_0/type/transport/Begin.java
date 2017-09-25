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
import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;

@CompositeType( symbolicDescriptor = "amqp:begin:list", numericDescriptor = 0x0000000000000011L)
public class Begin implements FrameBody
{

    @CompositeTypeField(index = 0)
    private UnsignedShort _remoteChannel;

    @CompositeTypeField(index = 1, mandatory = true)
    private UnsignedInteger _nextOutgoingId;

    @CompositeTypeField(index = 2, mandatory = true)
    private UnsignedInteger _incomingWindow;

    @CompositeTypeField(index = 3, mandatory = true)
    private UnsignedInteger _outgoingWindow;

    @CompositeTypeField(index = 4)
    private UnsignedInteger _handleMax;

    @CompositeTypeField(index = 5)
    private Symbol[] _offeredCapabilities;

    @CompositeTypeField(index = 6)
    private Symbol[] _desiredCapabilities;

    @CompositeTypeField(index = 7)
    private Map<Symbol, Object> _properties;

    public UnsignedShort getRemoteChannel()
    {
        return _remoteChannel;
    }

    public void setRemoteChannel(UnsignedShort remoteChannel)
    {
        _remoteChannel = remoteChannel;
    }

    public UnsignedInteger getNextOutgoingId()
    {
        return _nextOutgoingId;
    }

    public void setNextOutgoingId(UnsignedInteger nextOutgoingId)
    {
        _nextOutgoingId = nextOutgoingId;
    }

    public UnsignedInteger getIncomingWindow()
    {
        return _incomingWindow;
    }

    public void setIncomingWindow(UnsignedInteger incomingWindow)
    {
        _incomingWindow = incomingWindow;
    }

    public UnsignedInteger getOutgoingWindow()
    {
        return _outgoingWindow;
    }

    public void setOutgoingWindow(UnsignedInteger outgoingWindow)
    {
        _outgoingWindow = outgoingWindow;
    }

    public UnsignedInteger getHandleMax()
    {
        return _handleMax;
    }

    public void setHandleMax(UnsignedInteger handleMax)
    {
        _handleMax = handleMax;
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
        StringBuilder builder = new StringBuilder("Begin{");
        final int origLength = builder.length();

        if (_remoteChannel != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("remoteChannel=").append(_remoteChannel);
        }

        if (_nextOutgoingId != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("nextOutgoingId=").append(_nextOutgoingId);
        }

        if (_incomingWindow != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("incomingWindow=").append(_incomingWindow);
        }

        if (_outgoingWindow != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("outgoingWindow=").append(_outgoingWindow);
        }

        if (_handleMax != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("handleMax=").append(_handleMax);
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
        conn.receiveBegin(channel, this);
    }
}
