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
import org.apache.qpid.server.protocol.v1_0.type.ErrorCarryingFrameBody;

@CompositeType( symbolicDescriptor = "amqp:end:list", numericDescriptor = 0x0000000000000017L)
public class End implements ErrorCarryingFrameBody
{

    @CompositeTypeField(index = 0)
    private Error _error;

    @Override
    public Error getError()
    {
        return _error;
    }

    public void setError(Error error)
    {
        _error = error;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("End{");
        final int origLength = builder.length();

        if (_error != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("error=").append(_error);
        }

        builder.append('}');
        return builder.toString();
    }

    @Override
    public void invoke(int channel, ConnectionHandler conn)
    {
        conn.receiveEnd(channel, this);
    }
}
