
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

@CompositeType( symbolicDescriptor = "amqp:sasl-challenge:list", numericDescriptor = 0x0000000000000042L)
public class SaslChallenge implements SaslFrameBody
{
    @CompositeTypeField(index = 0, mandatory = true)
    private Binary _challenge;

    public Binary getChallenge()
    {
        return _challenge;
    }

    public void setChallenge(Binary challenge)
    {
        _challenge = challenge;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("SaslChallenge{");
        final int origLength = builder.length();

        if (_challenge != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("challenge=").append(_challenge);
        }

        builder.append('}');
        return builder.toString();
    }

    @Override
    public void invoke(final int channel, SASLEndpoint conn)
    {
        conn.receiveSaslChallenge(this);
    }
}
