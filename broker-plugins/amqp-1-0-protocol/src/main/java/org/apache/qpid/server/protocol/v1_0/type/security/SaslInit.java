
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


package org.apache.qpid.server.protocol.v1_0.type.security;


import org.apache.qpid.server.protocol.v1_0.SASLEndpoint;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.SaslFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

@CompositeType( symbolicDescriptor = "amqp:sasl-init:list", numericDescriptor = 0x0000000000000041L)
public class SaslInit implements SaslFrameBody
{

    @CompositeTypeField(index = 0, mandatory = true)
    private Symbol _mechanism;

    @CompositeTypeField(index = 1)
    private Binary _initialResponse;

    @CompositeTypeField(index = 2)
    private String _hostname;

    public Symbol getMechanism()
    {
        return _mechanism;
    }

    public void setMechanism(Symbol mechanism)
    {
        _mechanism = mechanism;
    }

    public Binary getInitialResponse()
    {
        return _initialResponse;
    }

    public void setInitialResponse(Binary initialResponse)
    {
        _initialResponse = initialResponse;
    }

    public String getHostname()
    {
        return _hostname;
    }

    public void setHostname(String hostname)
    {
        _hostname = hostname;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("SaslInit{");
        final int origLength = builder.length();

        if (_mechanism != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("mechanism=").append(_mechanism);
        }

        if (_initialResponse != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("initialResponse=").append(_initialResponse);
        }

        if (_hostname != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("hostname=").append(_hostname);
        }

        builder.append('}');
        return builder.toString();
    }

    @Override
    public void invoke(final int channel, SASLEndpoint conn)
    {
        conn.receiveSaslInit(this);
    }
}
