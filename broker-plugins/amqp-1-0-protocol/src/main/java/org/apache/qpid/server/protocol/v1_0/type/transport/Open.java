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
import org.apache.qpid.server.protocol.v1_0.constants.SymbolTexts;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;

@CompositeType( symbolicDescriptor = SymbolTexts.AMQP_OPEN, numericDescriptor = 0x0000000000000010L)
public class Open implements FrameBody
{

    @CompositeTypeField(index = 0, mandatory = true)
    private String _containerId;

    @CompositeTypeField(index = 1)
    private String _hostname;

    @CompositeTypeField(index = 2)
    private UnsignedInteger _maxFrameSize;

    @CompositeTypeField(index = 3)
    private UnsignedShort _channelMax;

    @CompositeTypeField(index = 4)
    private UnsignedInteger _idleTimeOut;

    @CompositeTypeField(index = 5)
    private Symbol[] _outgoingLocales;

    @CompositeTypeField(index = 6)
    private Symbol[] _incomingLocales;

    @CompositeTypeField(index = 7)
    private Symbol[] _offeredCapabilities;

    @CompositeTypeField(index = 8)
    private Symbol[] _desiredCapabilities;

    @CompositeTypeField(index = 9)
    private Map<Symbol, Object> _properties;

    public String getContainerId()
    {
        return _containerId;
    }

    public void setContainerId(String containerId)
    {
        _containerId = containerId;
    }

    public String getHostname()
    {
        return _hostname;
    }

    public void setHostname(String hostname)
    {
        _hostname = hostname;
    }

    public UnsignedInteger getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    public void setMaxFrameSize(UnsignedInteger maxFrameSize)
    {
        _maxFrameSize = maxFrameSize;
    }

    public UnsignedShort getChannelMax()
    {
        return _channelMax;
    }

    public void setChannelMax(UnsignedShort channelMax)
    {
        _channelMax = channelMax;
    }

    public UnsignedInteger getIdleTimeOut()
    {
        return _idleTimeOut;
    }

    public void setIdleTimeOut(UnsignedInteger idleTimeOut)
    {
        _idleTimeOut = idleTimeOut;
    }

    public Symbol[] getOutgoingLocales()
    {
        return _outgoingLocales;
    }

    public void setOutgoingLocales(Symbol... outgoingLocales)
    {
        _outgoingLocales = outgoingLocales;
    }

    public Symbol[] getIncomingLocales()
    {
        return _incomingLocales;
    }

    public void setIncomingLocales(Symbol... incomingLocales)
    {
        _incomingLocales = incomingLocales;
    }

    public Symbol[] getOfferedCapabilities()
    {
        return _offeredCapabilities;
    }

    public void setOfferedCapabilities(Symbol... offeredCapabilities)
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
        StringBuilder builder = new StringBuilder("Open{");
        final int origLength = builder.length();

        if (_containerId != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("containerId=").append(_containerId);
        }

        if (_hostname != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("hostname=").append(_hostname);
        }

        if (_maxFrameSize != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("maxFrameSize=").append(_maxFrameSize);
        }

        if (_channelMax != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("channelMax=").append(_channelMax);
        }

        if (_idleTimeOut != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("idleTimeOut=").append(_idleTimeOut);
        }

        if (_outgoingLocales != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("outgoingLocales=").append(Arrays.toString(_outgoingLocales));
        }

        if (_incomingLocales != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("incomingLocales=").append(Arrays.toString(_incomingLocales));
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
        conn.receiveOpen(channel, this);
    }
}
