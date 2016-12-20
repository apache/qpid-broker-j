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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

final class QueueConsumerNodeListEntry
{
    private static final AtomicIntegerFieldUpdater<QueueConsumerNodeListEntry> DELETED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(QueueConsumerNodeListEntry.class, "_deleted");
    @SuppressWarnings("unused")
    private volatile int _deleted;

    private static final AtomicReferenceFieldUpdater<QueueConsumerNodeListEntry, QueueConsumerNodeListEntry> NEXT_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(QueueConsumerNodeListEntry.class, QueueConsumerNodeListEntry.class, "_next");
    @SuppressWarnings("unused")
    private volatile QueueConsumerNodeListEntry _next;
    private volatile QueueConsumerNode _queueConsumerNode;
    private final QueueConsumerNodeList _list;

    QueueConsumerNodeListEntry(final QueueConsumerNodeList list, final QueueConsumerNode queueConsumerNode)
    {
        _list = list;
        _queueConsumerNode = queueConsumerNode;
    }

    public QueueConsumerNodeListEntry(QueueConsumerNodeList list)
    {
        _list = list;
        //used for sentinel head and dummy node construction
        _queueConsumerNode = null;
        DELETED_UPDATER.set(this, 1);
    }

    public QueueConsumerNode getQueueConsumerNode()
    {
        return _queueConsumerNode;
    }


    /**
     * Retrieves the first non-deleted node following the current node.
     * Any deleted non-tail nodes encountered during the search are unlinked.
     *
     * @return the next non-deleted node, or null if none was found.
     */
    public QueueConsumerNodeListEntry findNext()
    {
        QueueConsumerNodeListEntry next = nextNode();
        while(next != null && next.isDeleted())
        {
            final QueueConsumerNodeListEntry newNext = next.nextNode();
            if(newNext != null)
            {
                //try to move our _next reference forward to the 'newNext'
                //node to unlink the deleted node
                NEXT_UPDATER.compareAndSet(this, next, newNext);
                next = nextNode();
            }
            else
            {
                //'newNext' is null, meaning 'next' is the current tail. Can't unlink
                //the tail node for thread safety reasons, just use the null.
                next = null;
            }
        }

        return next;
    }

    /**
     * Gets the immediately next referenced node in the structure.
     *
     * @return the immediately next node in the structure, or null if at the tail.
     */
    protected QueueConsumerNodeListEntry nextNode()
    {
        return _next;
    }

    /**
     * Used to initialise the 'next' reference. Will only succeed if the reference was not previously set.
     *
     * @param node the ConsumerNode to set as 'next'
     * @return whether the operation succeeded
     */
    boolean setNext(final QueueConsumerNodeListEntry node)
    {
        return NEXT_UPDATER.compareAndSet(this, null, node);
    }

    public void remove()
    {
        _list.removeEntry(this);
    }

    public boolean isDeleted()
    {
        return _deleted == 1;
    }

    boolean setDeleted()
    {
        final boolean deleted = DELETED_UPDATER.compareAndSet(this, 0, 1);
        if (deleted)
        {
            _queueConsumerNode = null;
        }
        return deleted;
    }

}
