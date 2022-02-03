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

public class LegacyOperationTest extends TestCase
{
    @Test
    public void testToString() {
        assertEquals("Access", LegacyOperation.ACCESS.toString());
        assertEquals("Access_logs", LegacyOperation.ACCESS_LOGS.toString());
        assertEquals("All", LegacyOperation.ALL.toString());
        assertEquals("Bind", LegacyOperation.BIND.toString());
        assertEquals("Create", LegacyOperation.CREATE.toString());
        assertEquals("Configure", LegacyOperation.CONFIGURE.toString());
        assertEquals("Consume", LegacyOperation.CONSUME.toString());
        assertEquals("Delete", LegacyOperation.DELETE.toString());
        assertEquals("Invoke", LegacyOperation.INVOKE.toString());
        assertEquals("Publish", LegacyOperation.PUBLISH.toString());
        assertEquals("Purge", LegacyOperation.PURGE.toString());
        assertEquals("Shutdown", LegacyOperation.SHUTDOWN.toString());
        assertEquals("Unbind", LegacyOperation.UNBIND.toString());
        assertEquals("Update", LegacyOperation.UPDATE.toString());
    }
}
