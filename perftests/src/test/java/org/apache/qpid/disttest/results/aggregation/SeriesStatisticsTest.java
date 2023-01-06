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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class SeriesStatisticsTest extends UnitTestBase
{
    public static Collection<Long> SERIES = Arrays.asList(2L, 4L, 4L, 4L, 5L, 5L, 7L, 9L, 5L);

    @Test
    public void testAggregate()
    {
        SeriesStatistics results = new SeriesStatistics();
        results.addMessageLatencies(SERIES);
        results.aggregate();
        assertEquals(5.0, results.getAverage(), 0.01, "Unexpected average");
        assertEquals(2, results.getMinimum(), "Unexpected min");
        assertEquals(9, results.getMaximum(), "Unexpected max");
        assertEquals(2.0, results.getStandardDeviation(), 0.01, "Unexpected standard deviation");
    }

}
