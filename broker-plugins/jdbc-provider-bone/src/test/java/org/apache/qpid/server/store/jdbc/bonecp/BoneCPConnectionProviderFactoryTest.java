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
package org.apache.qpid.server.store.jdbc.bonecp;

import static org.apache.qpid.server.store.jdbc.bonecp.BoneCPConnectionProviderFactory.MAX_CONNECTIONS_PER_PARTITION;
import static org.apache.qpid.server.store.jdbc.bonecp.BoneCPConnectionProviderFactory.MIN_CONNECTIONS_PER_PARTITION;
import static org.apache.qpid.server.store.jdbc.bonecp.BoneCPConnectionProviderFactory.PARTITION_COUNT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class BoneCPConnectionProviderFactoryTest extends UnitTestBase
{
    @Test
    public void testGetProviderAttributeNames()
    {
        BoneCPConnectionProviderFactory factory = new BoneCPConnectionProviderFactory();
        Set<String> supported = factory.getProviderAttributeNames();
        assertFalse("Supported settings cannot be empty", supported.isEmpty());

        assertTrue("partitionCount is not found", supported.contains(PARTITION_COUNT));
        assertTrue("maxConnectionsPerPartition is not found", supported.contains(MAX_CONNECTIONS_PER_PARTITION));
        assertTrue("minConnectionsPerPartition is not found",supported.contains(MIN_CONNECTIONS_PER_PARTITION));
    }
}
