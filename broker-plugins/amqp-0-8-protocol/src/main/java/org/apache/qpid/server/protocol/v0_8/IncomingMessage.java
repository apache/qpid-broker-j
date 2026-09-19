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
package org.apache.qpid.server.protocol.v0_8;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;

public class IncomingMessage
{

    private final MessagePublishInfo _messagePublishInfo;
    private final List<QpidByteBuffer> _contentChunks = new ArrayList<>();

    private ContentHeaderBody _contentHeaderBody;
    private MessageDestination _messageDestination;
    private int _contentBodyFrameCount;

    /**
     * Keeps a track of how many bytes we have received in body frames
     */
    private long _bodyLengthReceived = 0;

    public IncomingMessage(MessagePublishInfo info)
    {
        _messagePublishInfo = info;
    }

    public void setContentHeaderBody(final ContentHeaderBody contentHeaderBody)
    {
        _contentHeaderBody = contentHeaderBody;
    }

    public MessagePublishInfo getMessagePublishInfo()
    {
        return _messagePublishInfo;
    }

    public boolean addContentBodyFrame(final QpidByteBuffer contentChunk)
    {
        final int contentSize = contentChunk.remaining();
        if (contentSize > getSize() - _bodyLengthReceived)
        {
            return false;
        }

        if (contentSize > 0)
        {
            _contentChunks.add(contentChunk.duplicate());
        }
        _bodyLengthReceived += contentSize;
        _contentBodyFrameCount++;
        return true;
    }

    public boolean allContentReceived()
    {
        return (_bodyLengthReceived == getContentHeader().getBodySize());
    }

    public AMQShortString getExchangeName()
    {
        return _messagePublishInfo.getExchange();
    }

    public MessageDestination getDestination()
    {
        return _messageDestination;
    }

    public ContentHeaderBody getContentHeader()
    {
        return _contentHeaderBody;
    }

    public long getSize()
    {
        return getContentHeader().getBodySize();
    }

    public void setMessageDestination(final MessageDestination e)
    {
        _messageDestination = e;
    }

    public int getContentBodyFrameCount()
    {
        return _contentBodyFrameCount;
    }

    public int getContentChunkCount()
    {
        return _contentChunks.size();
    }

    public QpidByteBuffer getContentChunk(final int index)
    {
        return _contentChunks.get(index);
    }

    public void dispose()
    {
        final ContentHeaderBody contentHeaderBody = _contentHeaderBody;
        _contentHeaderBody = null;
        if (contentHeaderBody != null)
        {
            contentHeaderBody.dispose();
        }

        for (final QpidByteBuffer contentChunk : _contentChunks)
        {
            contentChunk.dispose();
        }
        _contentChunks.clear();
    }

}
