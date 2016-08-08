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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class QueueConsumerList
{
    private final ConsumerNode _head = new ConsumerNode();

    private final AtomicReference<ConsumerNode> _tail = new AtomicReference<ConsumerNode>(_head);
    private final AtomicReference<ConsumerNode> _subNodeMarker = new AtomicReference<ConsumerNode>(_head);
    private final AtomicInteger _size = new AtomicInteger();

    private void insert(final ConsumerNode node, final boolean count)
    {
        for (;;)
        {
            ConsumerNode tail = _tail.get();
            ConsumerNode next = tail.nextNode();
            if (tail == _tail.get())
            {
                if (next == null)
                {
                    if (tail.setNext(node))
                    {
                        _tail.compareAndSet(tail, node);
                        if(count)
                        {
                            _size.incrementAndGet();
                        }
                        return;
                    }
                }
                else
                {
                    _tail.compareAndSet(tail, next);
                }
            }
        }
    }

    public void add(final QueueConsumer<?> sub)
    {
        ConsumerNode node = new ConsumerNode(sub);
        insert(node, true);
    }

    public boolean remove(final QueueConsumer<?> sub)
    {
        ConsumerNode prevNode = _head;
        ConsumerNode node = _head.nextNode();

        while(node != null)
        {
            if(sub.equals(node.getConsumer()) && node.delete())
            {
                _size.decrementAndGet();

                ConsumerNode tail = _tail.get();
                if(node == tail)
                {
                    //we cant remove the last node from the structure for
                    //correctness reasons, however we have just 'deleted'
                    //the tail. Inserting an empty dummy node after it will
                    //let us scavenge the node containing the Consumer.
                    insert(new ConsumerNode(), false);
                }

                //advance the next node reference in the 'prevNode' to scavenge
                //the newly 'deleted' node for the Consumer.
                prevNode.findNext();

                nodeMarkerCleanup(node);

                return true;
            }

            prevNode = node;
            node = node.findNext();
        }

        return false;
    }

    private void nodeMarkerCleanup(final ConsumerNode node)
    {
        ConsumerNode markedNode = _subNodeMarker.get();
        if(node == markedNode)
        {
            //if the marked node is the one we are removing, then
            //replace it with a dummy pointing at the next node.
            //this is OK as the marked node is only used to index
            //into the list and find the next node to use.
            //Because we inserted a dummy if node was the
            //tail, markedNode.nextNode() can never be null.
            ConsumerNode dummy = new ConsumerNode();
            dummy.setNext(markedNode.nextNode());

            //if the CAS fails the marked node has changed, thus
            //we don't care about the dummy and just forget it
            _subNodeMarker.compareAndSet(markedNode, dummy);
        }
        else if(markedNode != null)
        {
            //if the marked node was already deleted then it could
            //hold subsequently removed nodes after it in the list 
            //in memory. Scavenge it to ensure their actual removal.
            if(markedNode != _head && markedNode.isDeleted())
            {
                markedNode.findNext();
            }
        }
    }

    public boolean updateMarkedNode(final ConsumerNode expected, final ConsumerNode nextNode)
    {
        return _subNodeMarker.compareAndSet(expected, nextNode);
    }

    /**
     * Get the current marked ConsumerNode. This should only be used only to index into the list and find the next node
     * after the mark, since if the previously marked node was subsequently deleted the item returned may be a dummy node
     * with reference to the next node.
     *
     * @return the previously marked node (or a dummy if it was subsequently deleted)
     */
    public ConsumerNode getMarkedNode()
    {
        return _subNodeMarker.get();
    }


    public ConsumerNodeIterator iterator()
    {
        return new ConsumerNodeIterator(_head);
    }

    public ConsumerNode getHead()
    {
        return _head;
    }

    public int size()
    {
        return _size.get();
    }
}



