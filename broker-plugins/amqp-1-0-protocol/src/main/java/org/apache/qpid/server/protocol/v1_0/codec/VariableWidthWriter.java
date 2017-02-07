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

public abstract class VariableWidthWriter<V> implements ValueWriter<V>
{
    private final int _length;
    private final int _size;

    public VariableWidthWriter(int length)
    {
        _length = length;
        _size = (_length & 0xFFFFFF00) == 0 ? 1 : 4;
    }

    @Override
    public final void writeToBuffer(QpidByteBuffer buffer)
    {

        final int length = getLength();
        boolean singleOctetSize = _size == 1;
        if(singleOctetSize)
        {
            buffer.put(getSingleOctetEncodingCode());
            buffer.put((byte)length);
        }
        else
        {

            buffer.put(getFourOctetEncodingCode());
            buffer.putInt(length);
        }
        writeBytes(buffer, 0,length);
    }

    @Override
    public final int getEncodedSize()
    {
        return 1 + _size + getLength();
    }

    protected abstract byte getFourOctetEncodingCode();

    protected abstract byte getSingleOctetEncodingCode();

    protected final int getLength()
    {
        return _length;
    }

    protected abstract void writeBytes(QpidByteBuffer buf, int offset, int length);
}
