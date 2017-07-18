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
package org.apache.qpid.server.protocol.v1_0.type;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Binary
{

    private final byte[] _data;
    private final int _hashCode;

    public Binary(final byte[] data)
    {
        _data = data;
        int hc = 0;
        for (int i = 0; i < _data.length; i++)
        {
            hc = 31*hc + (0xFF & data[i]);
        }
        _hashCode = hc;
    }

    public ByteBuffer asByteBuffer()
    {
        return ByteBuffer.wrap(_data);
    }

    @Override
    public final int hashCode()
    {
        return _hashCode;
    }

    @Override
    public final boolean equals(Object o)
    {
        if(o instanceof Binary)
        {
            Binary buf = (Binary) o;
            return Arrays.equals(_data, buf._data);
        }

        return false;
    }



    public byte[] getArray()
    {
        return _data;
    }


    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();


        for (int i = 0; i < _data.length; i++)
        {
            byte c = _data[i];

            if (c > 31 && c < 127 && c != '\\')
            {
                str.append((char)c);
            }
            else
            {
                str.append(String.format("\\x%02x", c));
            }
        }

        return str.toString();

    }
}
