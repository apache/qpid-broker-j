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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class ConsumerNode
{
    private final AtomicBoolean _deleted = new AtomicBoolean();
    private final AtomicReference<ConsumerNode> _next = new AtomicReference<ConsumerNode>();
    private final QueueConsumer<?> _sub;

    public ConsumerNode()
    {
        //used for sentinel head and dummy node construction
        _sub = null;
        _deleted.set(true);
    }

    public ConsumerNode(final QueueConsumer<?> sub)
    {
        //used for regular node construction
        _sub = sub;
    }

    /**
     * Retrieves the first non-deleted node following the current node.
     * Any deleted non-tail nodes encountered during the search are unlinked.
     *
     * @return the next non-deleted node, or null if none was found.
     */
    public ConsumerNode findNext()
    {
        ConsumerNode next = nextNode();
        while(next != null && next.isDeleted())
        {
            final ConsumerNode newNext = next.nextNode();
            if(newNext != null)
            {
                //try to move our _next reference forward to the 'newNext'
                //node to unlink the deleted node
                _next.compareAndSet(next, newNext);
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
    protected ConsumerNode nextNode()
    {
        return _next.get();
    }

    /**
     * Used to initialise the 'next' reference. Will only succeed if the reference was not previously set.
     *
     * @param node the ConsumerNode to set as 'next'
     * @return whether the operation succeeded
     */
    boolean setNext(final ConsumerNode node)
    {
        return _next.compareAndSet(null, node);
    }

    public boolean isDeleted()
    {
        return _deleted.get();
    }

    public boolean delete()
    {
        return _deleted.compareAndSet(false,true);
    }

    public QueueConsumer<?> getConsumer()
    {
        return _sub;
    }
}
