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
package org.apache.qpid.server.store.jdbc.hikaricp;

import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProviderFactory.MAXIMUM_POOLSIZE;
import static org.apache.qpid.server.store.jdbc.hikaricp.HikariCPConnectionProviderFactory.MINIMUM_IDLE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class HikariCPConnectionProviderFactoryTest extends UnitTestBase
{
    @Test
    public void testGetProviderAttributeNames()
    {
        HikariCPConnectionProviderFactory factory = new HikariCPConnectionProviderFactory();
        Set<String> supported = factory.getProviderAttributeNames();
        assertFalse(supported.isEmpty(), "Supported settings cannot be empty");

        assertTrue(supported.contains(MAXIMUM_POOLSIZE), "maximumPoolSize is not found");
        assertTrue(supported.contains(MINIMUM_IDLE), "minimumIdle is not found");
    }
}
