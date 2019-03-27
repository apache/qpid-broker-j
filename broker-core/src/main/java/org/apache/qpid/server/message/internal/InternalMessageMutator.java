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

import java.util.HashMap;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.ServerMessageMutator;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoredMessage;

public class InternalMessageMutator implements ServerMessageMutator<InternalMessage>
{
    private final InternalMessage _message;
    private final MessageStore _messageStore;
    private byte _priority;

    InternalMessageMutator(final InternalMessage message, final MessageStore messageStore)
    {
        _message = message;
        _messageStore = messageStore;
        final InternalMessageHeader messageHeader = _message.getMessageHeader();
        _priority = messageHeader.getPriority();
    }

    @Override
    public void setPriority(final byte priority)
    {
        _priority = priority;
    }

    @Override
    public byte getPriority()
    {
        return _priority;
    }

    @Override
    public InternalMessage create()
    {
        final InternalMessageHeader messageHeader = _message.getMessageHeader();
        final InternalMessageHeader newHeader = new InternalMessageHeader(new HashMap<>(messageHeader.getHeaderMap()),
                                                                          messageHeader.getCorrelationId(),
                                                                          messageHeader.getExpiration(),
                                                                          messageHeader.getUserId(),
                                                                          messageHeader.getAppId(),
                                                                          messageHeader.getMessageId(),
                                                                          messageHeader.getMimeType(),
                                                                          messageHeader.getEncoding(),
                                                                          _priority,
                                                                          messageHeader.getTimestamp(),
                                                                          messageHeader.getNotValidBefore(),
                                                                          messageHeader.getType(),
                                                                          messageHeader.getReplyTo(),
                                                                          _message.getArrivalTime());

        final long contentSize = _message.getSize();
        final InternalMessageMetaData metaData =
                InternalMessageMetaData.create(_message.isPersistent(), newHeader, (int) contentSize);
        final MessageHandle<InternalMessageMetaData> handle = _messageStore.addMessage(metaData);
        final QpidByteBuffer content = _message.getContent();
        if (content != null)
        {
            handle.addContent(content);
        }
        final StoredMessage<InternalMessageMetaData> storedMessage = handle.allContentAdded();
        return new InternalMessage(storedMessage, newHeader, _message.getMessageBody(), _message.getTo());
    }
}
