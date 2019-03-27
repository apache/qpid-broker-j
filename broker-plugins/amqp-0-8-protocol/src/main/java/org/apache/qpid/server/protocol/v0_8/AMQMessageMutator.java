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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.ServerMessageMutator;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;

public class AMQMessageMutator implements ServerMessageMutator<AMQMessage>
{
    private final AMQMessage _message;
    private final MessageStore _messageStore;
    private final BasicContentHeaderProperties _basicContentHeaderProperties;

    AMQMessageMutator(final AMQMessage message, final MessageStore messageStore)
    {
        _message = message;
        _messageStore = messageStore;
        _basicContentHeaderProperties =
                new BasicContentHeaderProperties(_message.getContentHeaderBody().getProperties());
    }

    @Override
    public void setPriority(final byte priority)
    {
        _basicContentHeaderProperties.setPriority(priority);
    }

    @Override
    public byte getPriority()
    {
        return _basicContentHeaderProperties.getPriority();
    }

    @Override
    public AMQMessage create()
    {
        final long contentSize = _message.getSize();
        final QpidByteBuffer content = _message.getContent();
        final ContentHeaderBody contentHeader = new ContentHeaderBody(_basicContentHeaderProperties, contentSize);
        final MessageMetaData messageMetaData =
                new MessageMetaData(_message.getMessagePublishInfo(), contentHeader, _message.getArrivalTime());
        final MessageHandle<MessageMetaData> handle = _messageStore.addMessage(messageMetaData);
        if (content != null)
        {
            handle.addContent(content);
        }
        return new AMQMessage(handle.allContentAdded(), _message.getConnectionReference());
    }
}
