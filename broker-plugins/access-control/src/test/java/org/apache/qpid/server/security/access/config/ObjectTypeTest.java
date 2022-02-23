/*
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
 */
package org.apache.qpid.server.security.access.config;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.EnumSet;

public class ObjectTypeTest extends TestCase
{
    @Test
    public void testIsSupported()
    {
        for (LegacyOperation operation : LegacyOperation.values())
        {
            assertTrue(ObjectType.ALL.isSupported(operation));
        }

        for (ObjectType objectType : ObjectType.values())
        {
            for (LegacyOperation operation : objectType.getOperations())
            {
                assertTrue(objectType.isSupported(operation));
            }
            final EnumSet<LegacyOperation> legacyOperations = EnumSet.allOf(LegacyOperation.class);
            legacyOperations.removeAll(objectType.getOperations());
            for (LegacyOperation operation : legacyOperations)
            {
                assertFalse(objectType.isSupported(operation));
            }
        }
    }

    @Test
    public void testTestToString()
    {
        assertEquals("All", ObjectType.ALL.toString());
        assertEquals("Broker", ObjectType.BROKER.toString());
        assertEquals("Exchange", ObjectType.EXCHANGE.toString());
        assertEquals("Group", ObjectType.GROUP.toString());
        assertEquals("Management", ObjectType.MANAGEMENT.toString());
        assertEquals("Method", ObjectType.METHOD.toString());
        assertEquals("Queue", ObjectType.QUEUE.toString());
        assertEquals("User", ObjectType.USER.toString());
        assertEquals("Virtualhost", ObjectType.VIRTUALHOST.toString());
        assertEquals("Virtualhostnode", ObjectType.VIRTUALHOSTNODE.toString());
    }
}
