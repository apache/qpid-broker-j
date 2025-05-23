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
package org.apache.qpid.disttest.results;

import static org.apache.qpid.test.utils.JvmVendor.IBM;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;

import org.apache.qpid.disttest.controller.ResultsForAllTests;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.results.aggregation.ITestResult;
import org.apache.qpid.test.utils.TestFileUtils;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ResultsXmlWriterTest extends UnitTestBase
{
    private final File _outputDir = TestFileUtils.createTestDirectory();

    private final ResultsWriter _resultsFileWriter = new ResultsXmlWriter(_outputDir);

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(is(not(equalTo(IBM))).matches(getJvmVendor()), "Transformer on IBM JDK has different whitespace behaviour");
    }

    @Test
    public void testResultForNoTests() throws Exception
    {
        ResultsForAllTests resultsForAllTests = mock(ResultsForAllTests.class);

        String expectedXmlContent = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n"
                + "<testsuite tests=\"0\"/>%n");

        _resultsFileWriter.writeResults(resultsForAllTests, "config.json");

        File resultsFile = new File(_outputDir, "config.xml");
        final String result = new String(Files.readAllBytes(resultsFile.toPath()), StandardCharsets.UTF_8);
        assertEquals(expectedXmlContent, result);
    }

    @Test
    public void testResultForOneTest() throws Exception
    {
        ITestResult test = mock(ITestResult.class);
        when(test.getName()).thenReturn("mytest");

        ResultsForAllTests resultsForAllTests = mock(ResultsForAllTests.class);
        when(resultsForAllTests.getTestResults()).thenReturn(Collections.singletonList(test));

        String expectedXmlContent = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n"
                + "<testsuite tests=\"1\">%n"
                + "  <testcase classname=\"config.json\" name=\"mytest\"/>%n"
                + "</testsuite>%n");

        _resultsFileWriter.writeResults(resultsForAllTests, "config.json");

        File resultsFile = new File(_outputDir, "config.xml");
        final String result = new String(Files.readAllBytes(resultsFile.toPath()), StandardCharsets.UTF_8);
        assertEquals(expectedXmlContent, result);
    }

    @Test
    public void testResultForOneTestWithError() throws Exception
    {
        ParticipantResult resultWithError = mock(ParticipantResult.class);
        when(resultWithError.hasError()).thenReturn(true);
        when(resultWithError.getErrorMessage()).thenReturn("something went wrong");

        ITestResult test = mock(ITestResult.class);
        when(test.getName()).thenReturn("mytest");
        when(test.hasErrors()).thenReturn(true);
        when(test.getParticipantResults()).thenReturn(Collections.singletonList(resultWithError));

        ResultsForAllTests resultsForAllTests = mock(ResultsForAllTests.class);
        when(resultsForAllTests.getTestResults()).thenReturn(Collections.singletonList(test));

        String expectedXmlContent = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n"
                + "<testsuite tests=\"1\">%n"
                + "  <testcase classname=\"config.json\" name=\"mytest\">%n"
                + "    <error message=\"something went wrong\"/>%n"
                + "  </testcase>%n"
                + "</testsuite>%n");

        _resultsFileWriter.writeResults(resultsForAllTests, "config.json");

        File resultsFile = new File(_outputDir, "config.xml");
        final String result = new String(Files.readAllBytes(resultsFile.toPath()), StandardCharsets.UTF_8);
        assertEquals(expectedXmlContent, result);
    }
}
