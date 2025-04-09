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
package org.apache.qpid.disttest.results;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.apache.qpid.disttest.controller.ResultsForAllTests;
import org.apache.qpid.disttest.results.aggregation.ITestResult;
import org.apache.qpid.disttest.results.formatting.CSVFormatter;
import org.apache.qpid.test.utils.TestFileUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ResultsCsvWriterTest extends UnitTestBase
{
    private final CSVFormatter _csvFormater = mock(CSVFormatter.class);

    private final File _outputDir = TestFileUtils.createTestDirectory();

    private final ResultsCsvWriter _resultsFileWriter = new ResultsCsvWriter(_outputDir);

    @BeforeEach
    public void setUp() throws Exception
    {
        _resultsFileWriter.setCsvFormater(_csvFormater);
    }

    @Test
    public void testWriteResultsToFile() throws Exception
    {
        List<ITestResult> testResult1 = mock(List.class);
        ResultsForAllTests results1 = mock(ResultsForAllTests.class);
        when(results1.getTestResults()).thenReturn(testResult1);


        List<ITestResult> testResult2 = mock(List.class);
        ResultsForAllTests results2 = mock(ResultsForAllTests.class);
        when(results2.getTestResults()).thenReturn(testResult2);

        String expectedCsvContents1 = "expected-csv-contents1";
        String expectedCsvContents2 = "expected-csv-contents2";
        String expectedSummaryFileContents = "expected-summary-file";
        when(_csvFormater.format(testResult1)).thenReturn(expectedCsvContents1);
        when(_csvFormater.format(testResult2)).thenReturn(expectedCsvContents2);

        _resultsFileWriter.begin();
        _resultsFileWriter.writeResults(results1, "config1.json");

        File resultsFile1 = new File(_outputDir, "config1.csv");
        final String result1 = new String(Files.readAllBytes(resultsFile1.toPath()), StandardCharsets.UTF_8);
        assertEquals(expectedCsvContents1, result1);

        _resultsFileWriter.writeResults(results2, "config2.json");

        File resultsFile2 = new File(_outputDir, "config2.csv");
        final String result2 = new String(Files.readAllBytes(resultsFile2.toPath()), StandardCharsets.UTF_8);
        assertEquals(expectedCsvContents2, result2);

        when(_csvFormater.format(any(List.class))).thenReturn(expectedSummaryFileContents);

        _resultsFileWriter.end();

        File summaryFile = new File(_outputDir, ResultsCsvWriter.TEST_SUMMARY_FILE_NAME);
        final String result = new String(Files.readAllBytes(summaryFile.toPath()), StandardCharsets.UTF_8);
        assertEquals(expectedSummaryFileContents, result);
    }
}
