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
package org.apache.qpid.server.protocol.v1_0.codec;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class CharTypeConstructor implements TypeConstructor<String>
{
    private static final CharTypeConstructor INSTANCE = new CharTypeConstructor();


    public static CharTypeConstructor getInstance()
    {
        return INSTANCE;
    }

    private CharTypeConstructor()
    {
    }

    @Override
    public String construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
    {
        if (in.hasRemaining(4))
        {
            int codePoint = in.getInt(); // TODO look wrong AMQP 1.0 type is actually UTF-32BE not a code point
            char[] chars = Character.toChars(codePoint);
            return new String(chars);
        }
        else
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct char: insufficient input data");
        }
    }
}
