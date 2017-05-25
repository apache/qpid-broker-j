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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class StoredMemoryMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T>
{
    private final long _messageNumber;
    private final int _contentSize;
    private final int _metadataSize;
    private final Queue<QpidByteBuffer> _content = new LinkedList<>();
    private volatile T _metaData;

    public StoredMemoryMessage(long messageNumber, T metaData)
    {
        _messageNumber = messageNumber;
        _metaData = metaData;
        _contentSize = _metaData.getContentSize();
        _metadataSize = _metaData.getStorableSize();
    }

    public long getMessageNumber()
    {
        return _messageNumber;
    }

    public synchronized void addContent(QpidByteBuffer src)
    {
        _content.add(src.slice());
    }

    @Override
    public synchronized StoredMessage<T> allContentAdded()
    {
        return this;
    }


    @Override
    public synchronized Collection<QpidByteBuffer> getContent(int offset, int length)
    {
        Collection<QpidByteBuffer> content = new ArrayList<>(_content.size());
        int pos = 0;
        for (QpidByteBuffer buf : _content)
        {
            if (length > 0)
            {
                int bufRemaining = buf.remaining();
                if (pos + bufRemaining <= offset)
                {
                    pos += bufRemaining;
                }
                else if (pos >= offset)
                {
                    buf = buf.duplicate();
                    if (bufRemaining <= length)
                    {
                        length -= bufRemaining;
                    }
                    else
                    {
                        buf.limit(length);
                        length = 0;
                    }
                    content.add(buf);
                    pos += buf.remaining();
                }
                else
                {
                    int offsetInBuf = offset - pos;
                    int limit = length < bufRemaining - offsetInBuf ? length : bufRemaining - offsetInBuf;
                    final QpidByteBuffer bufView = buf.view(offsetInBuf, limit);
                    content.add(bufView);
                    length -= limit;
                    pos += limit + offsetInBuf;
                }
            }
        }
        return content;
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

    public T getMetaData()
    {
        return _metaData;
    }

    public synchronized void remove()
    {
        _metaData.dispose();
        _metaData = null;
        if (_content != null)
        {
            for (QpidByteBuffer content : _content)
            {
                content.dispose();
            }
            _content.clear();
        }
    }

    @Override
    public boolean isInMemory()
    {
        return true;
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
        List<QpidByteBuffer> newContent = new ArrayList<>(_content.size());
        for (Iterator<QpidByteBuffer> iterator = _content.iterator(); iterator.hasNext(); )
        {
            final QpidByteBuffer buffer = iterator.next();
            newContent.add(QpidByteBuffer.reallocateIfNecessary(buffer));
            iterator.remove();
        }
        _content.addAll(newContent);
    }

    public void clear()
    {
        remove();
    }
}
