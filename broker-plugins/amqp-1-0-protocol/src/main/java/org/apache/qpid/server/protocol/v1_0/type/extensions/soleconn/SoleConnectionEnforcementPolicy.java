/*
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
 */
package org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn;

import java.util.Objects;

import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

public enum SoleConnectionEnforcementPolicy implements RestrictedType<UnsignedInteger>
{
    REFUSE_CONNECTION(0, "refuse-connection"),
    CLOSE_EXISTING(1, "close-existing");

    private final UnsignedInteger _val;
    private final String _description;

    SoleConnectionEnforcementPolicy(int val, String description)
    {
        _val = UnsignedInteger.valueOf(val);
        _description = Objects.requireNonNull(description);
    }

    @Override
    public UnsignedInteger getValue()
    {
        return _val;
    }

    public static SoleConnectionEnforcementPolicy valueOf(Object obj)
    {
        for (final SoleConnectionEnforcementPolicy policy : values())
        {
            if (policy._val.equals(obj))
            {
                return policy;
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert '%s' into 'sole-connection-enforcement-policy'", obj));
    }

    @Override
    public String toString()
    {
        return _description;
    }
}
