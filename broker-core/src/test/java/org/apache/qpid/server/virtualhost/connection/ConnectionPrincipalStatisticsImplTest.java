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

package org.apache.qpid.server.virtualhost.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.qpid.server.virtualhost.ConnectionPrincipalStatistics;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConnectionPrincipalStatisticsImplTest extends UnitTestBase
{

    @Test
    public void getOpenConnectionCount()
    {
        final ConnectionPrincipalStatistics stats =
                new ConnectionPrincipalStatisticsImpl(1, Collections.singletonList(System.currentTimeMillis()));

        assertEquals(1, stats.getConnectionCount());
    }

    @Test
    public void getOpenConnectionFrequency()
    {
        final long connectionCreatedTime = System.currentTimeMillis();
        final ConnectionPrincipalStatistics stats =
                new ConnectionPrincipalStatisticsImpl(1,
                                                      Arrays.asList(connectionCreatedTime - 1000, connectionCreatedTime));
        assertEquals(2, stats.getConnectionFrequency());
    }

    @Test
    public void getLatestConnectionCreatedTimes()
    {
        final long connectionCreatedTime = System.currentTimeMillis();
        final List<Long> connectionCreatedTimes = Arrays.asList(connectionCreatedTime - 1000, connectionCreatedTime);
        final ConnectionPrincipalStatisticsImpl stats = new ConnectionPrincipalStatisticsImpl(1, connectionCreatedTimes);
        assertEquals(connectionCreatedTimes, stats.getLatestConnectionCreatedTimes());
    }

    @Test
    public void equals()
    {
        final long connectionCreatedTime = System.currentTimeMillis();
        final ConnectionPrincipalStatistics stats1 =
                new ConnectionPrincipalStatisticsImpl(1, Collections.singletonList(connectionCreatedTime));
        final ConnectionPrincipalStatistics stats2 =
                new ConnectionPrincipalStatisticsImpl(1, Collections.singletonList(connectionCreatedTime));
        assertEquals(stats1, stats2);

        final long connectionCreatedTime2 = System.currentTimeMillis();
        final ConnectionPrincipalStatistics stats3 =
                new ConnectionPrincipalStatisticsImpl(2, Arrays.asList(connectionCreatedTime, connectionCreatedTime2));

        assertNotEquals(stats2, stats3);
        assertNotEquals(stats1, stats3);
    }

    @Test
    public void testHashCode()
    {
        final long connectionCreatedTime = System.currentTimeMillis();
        final ConnectionPrincipalStatistics stats1 =
                new ConnectionPrincipalStatisticsImpl(1, Collections.singletonList(connectionCreatedTime));
        final ConnectionPrincipalStatistics stats2 =
                new ConnectionPrincipalStatisticsImpl(1, Collections.singletonList(connectionCreatedTime));
        assertEquals(stats1.hashCode(), stats2.hashCode());

        final long connectionCreatedTime2 = System.currentTimeMillis();
        final ConnectionPrincipalStatistics stats3 =
                new ConnectionPrincipalStatisticsImpl(2, Arrays.asList(connectionCreatedTime, connectionCreatedTime2));

        assertNotEquals(stats2.hashCode(), stats3.hashCode());
        assertNotEquals(stats1.hashCode(), stats3.hashCode());
    }
}
