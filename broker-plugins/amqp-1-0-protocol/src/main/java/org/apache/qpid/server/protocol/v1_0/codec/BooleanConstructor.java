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

package org.apache.qpid.server.protocol.v1_0.codec;

import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class BooleanConstructor
{
    private static final TypeConstructor<Boolean> TRUE_INSTANCE = (in, handler) -> Boolean.TRUE;
    private static final TypeConstructor<Boolean> FALSE_INSTANCE = (in, handler) -> Boolean.FALSE;
    private static final TypeConstructor<Boolean> BYTE_INSTANCE = (in, handler) ->
    {
        if (in.hasRemaining())
        {
            byte b = in.get();
            return b != (byte) 0;
        }
        else
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct boolean: insufficient input data");
        }
    };

    public static TypeConstructor<Boolean> getTrueInstance()
    {
        return TRUE_INSTANCE;
    }

    public static TypeConstructor<Boolean> getFalseInstance()
    {
        return FALSE_INSTANCE;
    }

    public static TypeConstructor<Boolean> getByteInstance()
    {
        return BYTE_INSTANCE;
    }
}
