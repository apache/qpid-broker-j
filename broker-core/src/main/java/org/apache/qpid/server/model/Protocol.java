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
package org.apache.qpid.server.model;

public enum Protocol
{
    AMQP_0_8(ProtocolType.AMQP, "0-8"),
    AMQP_0_9(ProtocolType.AMQP, "0-9"),
    AMQP_0_9_1(ProtocolType.AMQP, "0-9-1"),
    AMQP_0_10(ProtocolType.AMQP, "0-10"),
    AMQP_1_0(ProtocolType.AMQP, "1.0"),
    HTTP(ProtocolType.HTTP);

    private final ProtocolType _protocolType;

    private final String _protocolVersion;

    Protocol(ProtocolType type)
    {
        this(type, null);
    }

    Protocol(ProtocolType type, String version)
    {
        _protocolType =  type;
        _protocolVersion = version;
    }

    public ProtocolType getProtocolType()
    {
        return _protocolType;
    }

    public String getProtocolVersion()
    {
        return _protocolVersion;
    }

    public boolean isAMQP()
    {
        return _protocolType == ProtocolType.AMQP;
    }

    public enum ProtocolType
    {
        AMQP, HTTP
    }
}
