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
package org.apache.qpid.disttest.results.aggregation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Assert;

import org.apache.qpid.disttest.controller.ResultsForAllTests;

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

public class AggregatorTest extends UnitTestBase
{
    private final Aggregator _aggregator = new Aggregator();
    private final TestResultAggregator _testResultAggregator = mock(TestResultAggregator.class);

    @Before
    public void setUp() throws Exception
    {
        _aggregator.setTestResultAggregator(_testResultAggregator);
    }

    @Test
    public void testAggregrateManyTestResults() throws Exception
    {
        ResultsForAllTests resultsForAllTests = mock(ResultsForAllTests.class);
        ITestResult testResult1 = mock(ITestResult.class);
        ITestResult testResult2 = mock(ITestResult.class);

        when(resultsForAllTests.getTestResults()).thenReturn(Arrays.asList(testResult1, testResult2));
        when(_testResultAggregator.aggregateTestResult(testResult1)).thenReturn(mock(AggregatedTestResult.class));
        when(_testResultAggregator.aggregateTestResult(testResult2)).thenReturn(mock(AggregatedTestResult.class));

        ResultsForAllTests aggregatedResultsForAllTests = _aggregator.aggregateResults(resultsForAllTests);
        assertEquals((long) 2, (long) aggregatedResultsForAllTests.getTestResults().size());

        verify(_testResultAggregator).aggregateTestResult(testResult1);
        verify(_testResultAggregator).aggregateTestResult(testResult2);

    }

}
