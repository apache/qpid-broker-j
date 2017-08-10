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
package org.apache.qpid.server.message.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class InternalMessageMetaData implements StorableMessageMetaData
{
    private final boolean _isPersistent;
    private final InternalMessageHeader _header;
    private final int _contentSize;
    private volatile byte[] _headerBytes;

    public InternalMessageMetaData(final boolean isPersistent, final InternalMessageHeader header, final int contentSize)
    {
        _isPersistent = isPersistent;
        _header = header;
        _contentSize = contentSize;
    }

    @Override
    public InternalMessageMetaDataType getType()
    {
        return InternalMessageMetaDataType.INSTANCE;
    }

    @Override
    public int getStorableSize()
    {
        ensureHeaderIsEncoded();
        return _headerBytes.length;
    }

    @Override
    public void writeToBuffer(final QpidByteBuffer dest)
    {
        ensureHeaderIsEncoded();
        dest.put(_headerBytes);
    }

    @Override
    public int getContentSize()
    {
        return _contentSize;
    }

    @Override
    public boolean isPersistent()
    {
        return _isPersistent;
    }

    @Override
    public void dispose()
    {

    }

    InternalMessageHeader getHeader()
    {
        return _header;
    }

    @Override
    public void clearEncodedForm()
    {

    }

    @Override
    public void reallocate()
    {

    }

    static InternalMessageMetaData create(boolean persistent, final InternalMessageHeader header, int contentSize)
    {
        return new InternalMessageMetaData(persistent, header, contentSize);
    }

    private void ensureHeaderIsEncoded()
    {
        if (_headerBytes == null)
        {
            try(ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                ObjectOutputStream os = new ObjectOutputStream(bytesOut))
            {
                os.writeInt(_contentSize);
                os.writeObject(_header);
                os.close();
                _headerBytes = bytesOut.toByteArray();
            }
            catch (IOException e)
            {
                throw new ConnectionScopedRuntimeException("Unexpected IO Exception on in memory operation", e);
            }
        }
    }
}
