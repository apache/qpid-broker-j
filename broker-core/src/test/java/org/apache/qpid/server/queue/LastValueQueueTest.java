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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Queue;

public class LastValueQueueTest extends AbstractQueueTestBase
{
    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> arguments = Map.of(LastValueQueue.LVQ_KEY, "lvqKey",
                Queue.TYPE, LastValueQueue.LAST_VALUE_QUEUE_TYPE);
        setArguments(arguments);
        super.setUp();
    }

    @Override
    @Test
    public void testOldestMessage()
    {
        final Queue<?> queue = getQueue();
        queue.enqueue(createMessage(1L, (byte)1, Map.of("lvqKey", "Z"), 10L), null, null);
        assertEquals(10L, queue.getOldestMessageArrivalTime());
        queue.enqueue(createMessage(2L, (byte)4, Map.of("lvqKey", "M"), 100L), null, null);
        assertEquals(10L, queue.getOldestMessageArrivalTime());
        queue.enqueue(createMessage(3L, (byte)9, Map.of("lvqKey", "Z"), 1000L), null, null);
        assertEquals(100L, queue.getOldestMessageArrivalTime());
    }
}
