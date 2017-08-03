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
package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public enum JmsMessageTypeAnnotation
{
    MESSAGE((byte) 0),
    OBJECT_MESSAGE((byte) 1),
    MAP_MESSAGE((byte) 2),
    BYTES_MESSAGE((byte) 3),
    STREAM_MESSAGE((byte) 4),
    TEXT_MESSAGE((byte) 5);

    public static final Symbol ANNOTATION_KEY = Symbol.valueOf("x-opt-jms-msg-type");
    private final byte _type;

    JmsMessageTypeAnnotation(byte value)
    {
        _type = value;
    }

    public byte getType()
    {
        return _type;
    }

    public static JmsMessageTypeAnnotation valueOf(byte type)
    {
        switch (type)
        {
            case 0:
                return MESSAGE;
            case 1:
                return OBJECT_MESSAGE;
            case 2:
                return MAP_MESSAGE;
            case 3:
                return BYTES_MESSAGE;
            case 4:
                return STREAM_MESSAGE;
            case 5:
                return TEXT_MESSAGE;
            default:
                throw new IllegalArgumentException(String.format("Unknown %s type %d",
                                                                 JmsMessageTypeAnnotation.class.getSimpleName(),
                                                                 type));
        }
    }
}
