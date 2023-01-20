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

package org.apache.qpid.server.protocol.v0_10.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;


/**
 * GenTest
 */
public class GenTest extends UnitTestBase
{
    @Test
    public void testBooleans()
    {
        QueueDeclare qd = new QueueDeclare().queue("test-queue").durable(false);
        assertEquals(qd.getQueue(), "test-queue");
        assertFalse(qd.getDurable(), "durable should be false");
        qd.setDurable(true);
        assertTrue(qd.getDurable(), "durable should be true");
        qd.setDurable(false);
        assertFalse(qd.getDurable(), "durable should be false again");
    }

}
