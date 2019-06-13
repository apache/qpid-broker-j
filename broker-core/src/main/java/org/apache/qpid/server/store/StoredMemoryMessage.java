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

package org.apache.qpid.server.store;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class StoredMemoryMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T>
{
    private final long _messageNumber;
    private final int _contentSize;
    private final int _metadataSize;
    private QpidByteBuffer _content = null;
    private volatile T _metaData;

    public StoredMemoryMessage(long messageNumber, T metaData)
    {
        _messageNumber = messageNumber;
        _metaData = metaData;
        _contentSize = _metaData.getContentSize();
        _metadataSize = _metaData.getStorableSize();
    }

    @Override
    public long getMessageNumber()
    {
        return _messageNumber;
    }

    @Override
    public synchronized void addContent(QpidByteBuffer src)
    {
        try (QpidByteBuffer content = _content)
        {
            if (content == null)
            {
                _content = src.slice();
            }
            else
            {
                _content = QpidByteBuffer.concatenate(content, src);
            }
        }
    }

    @Override
    public synchronized StoredMessage<T> allContentAdded()
    {
        return this;
    }


    @Override
    public synchronized QpidByteBuffer getContent(int offset, int length)
    {
        if (_content == null)
        {
            return QpidByteBuffer.emptyQpidByteBuffer();
        }

        try (QpidByteBuffer combined = QpidByteBuffer.concatenate(_content))
        {
            if (length == Integer.MAX_VALUE)
            {
                length = combined.remaining();
            }
            return combined.view(offset, length);
        }
    }

    @Override
    public int getContentSize()
    {
        return _contentSize;
    }

    @Override
    public int getMetadataSize()
    {
        return _metadataSize;
    }

    @Override
    public T getMetaData()
    {
        return _metaData;
    }

    @Override
    public synchronized void remove()
    {
        _metaData.dispose();
        _metaData = null;
        if (_content != null)
        {
            _content.dispose();
            _content = null;
        }
    }

    @Override
    public boolean isInContentInMemory()
    {
        return true;
    }

    @Override
    public long getInMemorySize()
    {
        return getContentSize() + getMetadataSize();
    }

    @Override
    public boolean flowToDisk()
    {
        return false;
    }

    @Override
    public synchronized void reallocate()
    {
        _metaData.reallocate();
        _content = QpidByteBuffer.reallocateIfNecessary(_content);
    }

    public void clear()
    {
        remove();
    }
}
