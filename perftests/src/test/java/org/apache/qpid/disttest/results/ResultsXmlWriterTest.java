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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.qpid.disttest.controller.ResultsForAllTests;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.results.aggregation.ITestResult;
import org.apache.qpid.disttest.results.aggregation.TestResultAggregator;
import org.apache.qpid.disttest.results.formatting.CSVFormatter;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.util.FileUtils;

public class ResultsXmlWriterTest extends QpidTestCase
{
    private File _outputDir = TestFileUtils.createTestDirectory();

    private ResultsWriter _resultsFileWriter = new ResultsXmlWriter(_outputDir);

    public void testResultForNoTests()
    {
        ResultsForAllTests resultsForAllTests = mock(ResultsForAllTests.class);

        String expectedXmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                    + "<testsuite tests=\"0\"/>\n";

        _resultsFileWriter.writeResults(resultsForAllTests, "config.json");

        File resultsFile = new File(_outputDir, "config.xml");

        assertEquals(expectedXmlContent, FileUtils.readFileAsString(resultsFile));
    }

    public void testResultForOneTest()
    {
        ITestResult test = mock(ITestResult.class);
        when(test.getName()).thenReturn("mytest");

        ResultsForAllTests resultsForAllTests = mock(ResultsForAllTests.class);
        when(resultsForAllTests.getTestResults()).thenReturn(Collections.singletonList(test));

        String expectedXmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                    + "<testsuite tests=\"1\">\n"
                                    + "  <testcase classname=\"config.json\" name=\"mytest\"/>\n"
                                    + "</testsuite>\n";

        _resultsFileWriter.writeResults(resultsForAllTests, "config.json");

        File resultsFile = new File(_outputDir, "config.xml");

        assertEquals(expectedXmlContent, FileUtils.readFileAsString(resultsFile));
    }

    public void testResultForOneTestWithError()
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

        String expectedXmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                     + "<testsuite tests=\"1\">\n"
                                     + "  <testcase classname=\"config.json\" name=\"mytest\">\n"
                                     + "    <error message=\"something went wrong\"/>\n"
                                     + "  </testcase>\n"
                                     + "</testsuite>\n";

        _resultsFileWriter.writeResults(resultsForAllTests, "config.json");

        File resultsFile = new File(_outputDir, "config.xml");

        assertEquals(expectedXmlContent, FileUtils.readFileAsString(resultsFile));
    }

}
