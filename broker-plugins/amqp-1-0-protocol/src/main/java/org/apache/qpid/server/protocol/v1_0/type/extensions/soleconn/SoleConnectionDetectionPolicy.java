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
 *
 */

package org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn;

import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

public class SoleConnectionDetectionPolicy implements RestrictedType<UnsignedInteger>
{
    public static final SoleConnectionDetectionPolicy
            STRONG = new SoleConnectionDetectionPolicy(UnsignedInteger.valueOf(0));
    public static final SoleConnectionDetectionPolicy
            WEAK = new SoleConnectionDetectionPolicy(UnsignedInteger.valueOf(1));

    private final UnsignedInteger _val;

    private SoleConnectionDetectionPolicy(final UnsignedInteger val)
    {
        _val = val;
    }

    @Override
    public UnsignedInteger getValue()
    {
        return _val;
    }

    public static SoleConnectionDetectionPolicy valueOf(Object obj)
    {
        if (obj instanceof UnsignedInteger)
        {
            UnsignedInteger val = (UnsignedInteger) obj;

            if (STRONG._val.equals(val))
            {
                return STRONG;
            }

            if (WEAK._val.equals(val))
            {
                return WEAK;
            }
        }

        final String message = String.format("Cannot convert '%s' into 'sole-connection-detection-policy'", obj);
        throw new IllegalArgumentException(message);
    }

    @Override
    public String toString()
    {

        if (this == STRONG)
        {
            return "strong";
        }

        if (this == WEAK)
        {
            return "weak";
        }

        return String.valueOf(_val);
    }
}
