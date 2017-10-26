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
package org.apache.qpid.server.queue;

import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.session.AMQPSession;

public interface QueueConsumer<X extends QueueConsumer<X,T>, T extends ConsumerTarget> extends Consumer<X, T>
{
    void flushBatched();

    void noMessagesAvailable();

    boolean hasInterest(QueueEntry node);

    boolean allocateCredit(QueueEntry entry);

    void restoreCredit(QueueEntry entry);

    void acquisitionRemoved(QueueEntry node);

    QueueConsumerNode getQueueConsumerNode();

    void queueDeleted();

    Queue<?> getQueue();

    MessageInstance.StealableConsumerAcquiredState<X> getOwningState();

    QueueContext getQueueContext();

    void awaitCredit(QueueEntry entry);

    boolean isNotifyWorkDesired();

    void notifyWork();

    void setQueueConsumerNode(QueueConsumerNode node);

    @Override
    AMQPSession<?,?> getSession();
}
