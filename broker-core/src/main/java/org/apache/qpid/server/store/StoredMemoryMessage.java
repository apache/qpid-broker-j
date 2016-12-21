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

import java.util.Collection;
import java.util.Collections;

import org.apache.qpid.bytebuffer.QpidByteBuffer;

public class StoredMemoryMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T>
{
    private final long _messageNumber;
    private final int _contentSize;
    private QpidByteBuffer _content;
    private T _metaData;

    public StoredMemoryMessage(long messageNumber, T metaData)
    {
        _messageNumber = messageNumber;
        _metaData = metaData;
        _contentSize = _metaData.getContentSize();
    }

    public long getMessageNumber()
    {
        return _messageNumber;
    }

    public void addContent(QpidByteBuffer src)
    {
        if(_content == null)
        {
            _content = src.slice();
            _content.position(_content.limit());
        }
        else
        {
            if(_content.remaining() >= src.remaining())
            {
                _content.putCopyOf(src);
            }
            else
            {
                final int contentSize = _metaData.getContentSize();
                int size = (contentSize < _content.position() + src.remaining())
                        ? _content.position() + src.remaining()
                        : contentSize;
                QpidByteBuffer oldContent = _content;
                oldContent.flip();
                _content = QpidByteBuffer.allocateDirect(size);
                _content.put(oldContent);
                _content.putCopyOf(src);
                oldContent.dispose();
            }

        }
    }

    @Override
    public StoredMessage<T> allContentAdded()
    {
        if(_content != null)
        {
            _content.flip();
        }
        return this;
    }


    @Override
    public Collection<QpidByteBuffer> getContent(int offset, int length)
    {
        if(_content == null)
        {
            return Collections.emptyList();
        }
        return Collections.singleton(_content.view(offset, length));
    }

    @Override
    public int getContentSize()
    {
        return _contentSize;
    }

    public T getMetaData()
    {
        return _metaData;
    }

    public void remove()
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
    public boolean isInMemory()
    {
        return true;
    }

    @Override
    public boolean flowToDisk()
    {
        return false;
    }

}
