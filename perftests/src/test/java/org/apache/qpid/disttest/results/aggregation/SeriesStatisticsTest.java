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
package org.apache.qpid.disttest.results.aggregation;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;

import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SeriesStatisticsTest extends UnitTestBase
{
    public static Collection<Long> SERIES = Arrays.asList(new Long[] {2L, 4L, 4L, 4L, 5L, 5L, 7L, 9L, 5L});

    @Test
    public void testAggregate()
    {
        SeriesStatistics results = new SeriesStatistics();
        results.addMessageLatencies(SERIES);
        results.aggregate();
        assertEquals("Unexpected average", 5.0, results.getAverage(), 0.01);
        assertEquals("Unexpected min", (long) 2, results.getMinimum());
        assertEquals("Unexpected max", (long) 9, results.getMaximum());
        assertEquals("Unexpected standard deviation", 2.0, results.getStandardDeviation(), 0.01);
    }

}
