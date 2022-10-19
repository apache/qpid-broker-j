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
package org.apache.qpid.disttest.results.formatting;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Assert;

import org.apache.qpid.disttest.controller.ResultsForAllTests;
import org.apache.qpid.disttest.results.ResultsTestFixture;

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

public class CSVFormatterTest extends UnitTestBase
{
    private final CSVFormatter _formatter = new CSVFormatter();

    @Test
    public void testResultsFileWithWithOneRow() throws Exception
    {
        ResultsTestFixture resultsTestFixture = new ResultsTestFixture();
        ResultsForAllTests resultsForAllTests = resultsTestFixture.createResultsForAllTests();

        String output = _formatter.format(resultsForAllTests.getTestResults());

        String expectedOutput = readCsvOutputFileAsString("expectedOutput.csv");

        assertEquals(expectedOutput, output);
    }

    private String readCsvOutputFileAsString(String filename) throws Exception
    {
        InputStream is = getClass().getResourceAsStream(filename);
        assertNotNull(is);

        StringBuilder output = new StringBuilder();

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while((line = br.readLine()) != null)
        {
            output.append(line);
            output.append("\n");
        }

        return output.toString();
    }
}
