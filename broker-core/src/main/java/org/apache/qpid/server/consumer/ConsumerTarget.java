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
package org.apache.qpid.server.consumer;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.session.AMQPSession;

public interface ConsumerTarget<T extends ConsumerTarget<T>>
{
    void acquisitionRemoved(MessageInstance node);

    boolean processPending();

    String getTargetAddress();

    boolean isMultiQueue();

    void notifyWork();

    void updateNotifyWorkDesired();

    boolean isNotifyWorkDesired();

    enum State
    {
        OPEN, CLOSED
    }

    State getState();

    void consumerAdded(MessageInstanceConsumer<T> sub);

    ListenableFuture<Void> consumerRemoved(MessageInstanceConsumer<T> sub);

    long getUnacknowledgedBytes();

    long getUnacknowledgedMessages();

    AMQPSession<?,T> getSession();

    void send(final MessageInstanceConsumer<T> consumer, MessageInstance entry, boolean batch);

    boolean sendNextMessage();

    void flushBatched();

    void noMessagesAvailable();

    boolean allocateCredit(ServerMessage msg);

    void restoreCredit(ServerMessage queueEntry);

    boolean isSuspended();

    boolean close();

    void queueDeleted(Queue queue, MessageInstanceConsumer sub);
}
