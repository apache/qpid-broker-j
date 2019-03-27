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

import java.util.List;

import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.model.Queue;

@ManagedObject( category = false, type="priority",
        amqpName = "org.apache.qpid.PriorityQueue" )
public interface PriorityQueue<X extends PriorityQueue<X>> extends Queue<X>
{
    String PRIORITIES = "priorities";

    @ManagedContextDefault( name = "queue.priorities")
    int DEFAULT_PRIORITY_LEVELS = 10;

    @ManagedAttribute( defaultValue = "${queue.priorities}")
    int getPriorities();

    /**
     * Re-enqueue the message with given id as a new message with priority changed to specified one.
     * <br>
     * The operation results in a deletion of original message and creation of new message
     * which is a copy of original one except for different message id and priority.
     *
     * @param messageId message id
     * @param newPriority new priority
     * @return  new message id, or -1 if message is not found or priority is not changed
     */
    @ManagedOperation(description = "Change the priority of the message with given message ID",
                      nonModifying = true,
                      changesConfiguredObjectState = false)
    long reenqueueMessageForPriorityChange(@Param(name = "messageId", description = "A message ID") long messageId,
                                           @Param(name = "newPriority", description = "the new priority") int newPriority);

    /**
     * Re-enqueue the messages matching given selector expression  as a new messages having priority changed to specified one.
     * <br>
     * Using {@code null} or an empty filter will change <em>all</em> messages from this queue.
     * <br>
     * The operation results in a deletion of original messages and creation of new messages
     * having the same properties and content as original ones except for different message id and priority.
     *
     * @param selector selector expression
     * @param newPriority new priority
     * @return the list containing ids of re-enqueed message s
     */
    @ManagedOperation(description = "Change the priority of the messages matching the given selector expression",
                      nonModifying = true,
                      changesConfiguredObjectState = false)
    List<Long> reenqueueMessagesForPriorityChange(@Param(name = "selector", description = "A message selector (can be empty)") String selector,
                                                  @Param(name = "newPriority", description = "the new priority") int newPriority);

}
