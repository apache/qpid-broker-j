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

import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.bytebuffer.QpidByteBuffer;

public class BinaryTypeConstructor extends VariableWidthTypeConstructor
{
    private static final BinaryTypeConstructor INSTANCE_1 = new BinaryTypeConstructor(1);
    private static final BinaryTypeConstructor INSTANCE_4 = new BinaryTypeConstructor(4);

    public static BinaryTypeConstructor getInstance(int i)
    {
        return i == 1 ? INSTANCE_1 : INSTANCE_4;
    }


    private BinaryTypeConstructor(int size)
    {
        super(size);
    }

    @Override
    public Object construct(final QpidByteBuffer in, boolean isCopy, ValueHandler handler) throws AmqpErrorException
    {
        int size;

        if(getSize() == 1)
        {
            size = in.get() & 0xFF;
        }
        else
        {
            size = in.getInt();
        }

        byte[] buf = new byte[size];
        in.get(buf);
        return new Binary(buf);
    }

}
