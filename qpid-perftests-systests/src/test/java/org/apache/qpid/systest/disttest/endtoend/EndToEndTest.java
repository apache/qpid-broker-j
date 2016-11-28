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
package org.apache.qpid.systest.disttest.endtoend;

import static org.apache.qpid.disttest.AbstractRunner.JNDI_CONFIG_PROP;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMB;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_CONSUMPTION_TO_PRODUCTION_RATIO_SUCCESS_THRESHOLD;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_MINIMUM_DELTA;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_MAX_NUMBER_OF_RUNS;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_START_TARGET_RATE;
import static org.apache.qpid.disttest.ControllerRunner.OUTPUT_DIR_PROP;
import static org.apache.qpid.disttest.ControllerRunner.TEST_CONFIG_PROP;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.disttest.ControllerRunner;
import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.jms.QpidQueueCreatorFactory;
import org.apache.qpid.disttest.jms.QpidRestAPIQueueCreator;
import org.apache.qpid.disttest.message.ParticipantAttribute;
import org.apache.qpid.disttest.results.aggregation.TestResultAggregator;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.util.FileUtils;

public class EndToEndTest extends QpidBrokerTestCase
{
    private static final String TEST_CONFIG_ITERATIONS = "qpid-perftests-systests/src/test/resources/org/apache/qpid/systest/disttest/endtoend/iterations.json";
    private static final String TEST_CONFIG_MANYPARTICIPANTS = "qpid-perftests-systests/src/test/resources/org/apache/qpid/systest/disttest/endtoend/manyparticipants.json";
    private static final String TEST_CONFIG_HILLCLIMBING = "qpid-perftests-systests/src/test/resources/org/apache/qpid/systest/disttest/endtoend/hillclimbing.js";
    private static final String TEST_CONFIG_ERROR = "qpid-perftests-systests/src/test/resources/org/apache/qpid/systest/disttest/endtoend/error.json";
    private static final String JNDI_CONFIG_FILE = "qpid-perftests-systests/src/test/resources/org/apache/qpid/systest/disttest/perftests.systests.properties";
    private static final int NUMBER_OF_HEADERS = 1;
    private static final int NUMBER_OF_SUMMARIES = 3;

    private File _outputDir;

    @Override
    public void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
        super.setUp();
        setSystemProperty("perftests.manangement-url", String.format("http://localhost:%d", getDefaultBroker().getHttpPort()));
        setSystemProperty("perftests.broker-virtualhostnode", "test");
        setSystemProperty("perftests.broker-virtualhost", "test");
        setSystemProperty(QpidQueueCreatorFactory.QUEUE_CREATOR_CLASS_NAME_SYSTEM_PROPERTY, QpidRestAPIQueueCreator.class.getName());
        _outputDir = createTemporaryOutputDirectory();
        assertTrue("Output dir must not exist", _outputDir.isDirectory());
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            if (_outputDir != null && _outputDir.exists())
            {
               FileUtils.delete(_outputDir, true);
            }
        }
        finally
        {
            super.tearDown();
        }
    }

    public void testIterations() throws Exception
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(TEST_CONFIG_PROP, TEST_CONFIG_ITERATIONS);
        arguments.put(JNDI_CONFIG_PROP, JNDI_CONFIG_FILE);
        arguments.put(OUTPUT_DIR_PROP, _outputDir.getAbsolutePath());
        arguments.put(HILL_CLIMB, "false");

        runController(arguments);
        checkXmlFile(arguments);
        String[] csvLines = getCsvResults(arguments);

        int numberOfParticipants = 2;
        int numberOfIterations = 2;
        int dataRowsPerIteration = numberOfParticipants + NUMBER_OF_SUMMARIES;

        int numberOfExpectedRows = NUMBER_OF_HEADERS + dataRowsPerIteration * numberOfIterations;
        assertEquals("Unexpected number of lines in CSV", numberOfExpectedRows, csvLines.length);

        final String testName = "Iterations";
        assertDataRowsForIterationArePresent(csvLines, testName, 0, dataRowsPerIteration);
        assertDataRowsForIterationArePresent(csvLines, testName, 1, dataRowsPerIteration);

        assertDataRowHasCorrectTestAndClientName(testName, "", TestResultAggregator.ALL_PARTICIPANTS_NAME, csvLines[4 + dataRowsPerIteration]);
    }

    public void testManyParticipants() throws Exception
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(TEST_CONFIG_PROP, TEST_CONFIG_MANYPARTICIPANTS);
        arguments.put(JNDI_CONFIG_PROP, JNDI_CONFIG_FILE);
        arguments.put(OUTPUT_DIR_PROP, _outputDir.getAbsolutePath());
        arguments.put(HILL_CLIMB, "false");

        runController(arguments);
        checkXmlFile(arguments);
        String[] csvLines = getCsvResults(arguments);

        int numberOfParticipants = 4;
        int dataRowsPerIteration = numberOfParticipants + NUMBER_OF_SUMMARIES;

        int numberOfExpectedRows = NUMBER_OF_HEADERS + dataRowsPerIteration;
        assertEquals("Unexpected number of lines in CSV", numberOfExpectedRows, csvLines.length);

        int actualMessagesSent = 0;
        int reportedTotalMessagesSent = 0;
        int actualMessagesReceived = 0;
        int reportedTotalMessagesReceived = 0;

        for(String csvLine : Arrays.copyOfRange(csvLines, 1, csvLines.length))
        {
            String[] cells = splitCsvCells(csvLine);

            final String participantName = cells[ParticipantAttribute.PARTICIPANT_NAME.ordinal()];
            final int numberOfMessages = Integer.valueOf(cells[ParticipantAttribute.NUMBER_OF_MESSAGES_PROCESSED.ordinal()]);
            if (participantName.equals(TestResultAggregator.ALL_PARTICIPANTS_NAME))
            {
                // ignore
            }
            else if (participantName.equals(TestResultAggregator.ALL_PRODUCER_PARTICIPANTS_NAME))
            {
                reportedTotalMessagesSent = numberOfMessages;
            }
            else if (participantName.equals(TestResultAggregator.ALL_CONSUMER_PARTICIPANTS_NAME))
            {
                reportedTotalMessagesReceived = numberOfMessages;
            }
            else if (participantName.contains("Producer"))
            {
                actualMessagesSent += numberOfMessages;
            }
            else if (participantName.contains("Consumer"))
            {
                actualMessagesReceived += numberOfMessages;
            }
        }

        assertEquals("Reported total messages sent does not match total sent by producers",
                     reportedTotalMessagesSent,
                     actualMessagesSent);
        assertEquals("Reported total messages received does not match total received by consumers",
                     reportedTotalMessagesReceived,
                     actualMessagesReceived);

    }

    public void testHillClimbing() throws Exception
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(TEST_CONFIG_PROP, TEST_CONFIG_HILLCLIMBING);
        arguments.put(JNDI_CONFIG_PROP, JNDI_CONFIG_FILE);
        arguments.put(OUTPUT_DIR_PROP, _outputDir.getAbsolutePath());
        arguments.put(HILL_CLIMB, "true");

        // Change default performance test settings to have it executed more quickly.
        // We are not interested in an accurate repeatable value in this test.
        arguments.put(HILL_CLIMBER_START_TARGET_RATE, "131073");
        arguments.put(HILL_CLIMBER_MINIMUM_DELTA, "64000");
        arguments.put(HILL_CLIMBER_MAX_NUMBER_OF_RUNS, "1");
        arguments.put(HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD, "0.01");
        arguments.put(HILL_CLIMBER_CONSUMPTION_TO_PRODUCTION_RATIO_SUCCESS_THRESHOLD, "0.01");

        runController(arguments);
        checkXmlFile(arguments);
        String[] csvLines = getCsvResults(arguments);

        int numberOfParticipants = 2;

        int numberOfExpectedRows = NUMBER_OF_HEADERS + numberOfParticipants + NUMBER_OF_SUMMARIES;
        assertEquals("Unexpected number of lines in CSV", numberOfExpectedRows, csvLines.length);

        final String testName = "HillClimbing";
        assertDataRowHasCorrectTestAndClientName(testName, "producingClient", "Producer1", csvLines[1]);
        assertDataRowHasCorrectTestAndClientName(testName, "consumingClient", "Consumer1", csvLines[3]);

        assertDataRowHasCorrectTestAndClientName(testName, "", TestResultAggregator.ALL_PARTICIPANTS_NAME, csvLines[4]);
        assertDataRowHasCorrectTestAndClientName(testName, "", TestResultAggregator.ALL_CONSUMER_PARTICIPANTS_NAME, csvLines[2]);
        assertDataRowHasCorrectTestAndClientName(testName,
                                                 "",
                                                 TestResultAggregator.ALL_PRODUCER_PARTICIPANTS_NAME,
                                                 csvLines[5]);

        assertDataRowHasThroughputValues(csvLines[4]);

    }

    public void testTestScriptCausesError() throws Exception
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(TEST_CONFIG_PROP, TEST_CONFIG_ERROR);
        arguments.put(JNDI_CONFIG_PROP, JNDI_CONFIG_FILE);
        arguments.put(OUTPUT_DIR_PROP, _outputDir.getAbsolutePath());

        try
        {
            runController(arguments);
            fail("Exception not thrown");
        }
        catch (DistributedTestException e)
        {
            // PASS
        }
    }

    private void runController(Map<String, String> args) throws Exception
    {
        String[] argsAsArray = new String[args.size()];
        int i = 0;
        for(Map.Entry<String, String> entry : args.entrySet())
        {
            argsAsArray[i++] = entry.getKey() + "=" + entry.getValue();
        }

        ControllerRunner runner = new ControllerRunner();
        runner.parseArgumentsIntoConfig(argsAsArray);
        runner.runController();
    }

    private String[] getCsvResults(final Map<String, String> args)
    {
        String testConfig = args.get(TEST_CONFIG_PROP);
        String expectedCsvFilename = buildOutputFilename(testConfig, ".csv");

        File expectedCsvOutputFile = new File(_outputDir, expectedCsvFilename);
        assertTrue("CSV output file must exist", expectedCsvOutputFile.exists());
        final String csvContents = FileUtils.readFileAsString(expectedCsvOutputFile);
        return csvContents.split("\n");
    }

    private void checkXmlFile(final Map<String, String> args)
    {
        String testConfig = args.get(TEST_CONFIG_PROP);
        String expectedXmlFilename = buildOutputFilename(testConfig, ".xml");

        File expectedXmlOutputFile = new File(_outputDir, expectedXmlFilename);
        assertTrue("XML output file must exist", expectedXmlOutputFile.exists());
    }

    private String buildOutputFilename(final String testConfig, final String extension)
    {
        String filename = Paths.get(testConfig).getFileName().toString();
        return filename.replaceAll("\\.[^.]*$", extension);
    }

    private void assertDataRowsForIterationArePresent(String[] csvLines, String testName, int iterationNumber, int expectedCount)
    {
        String itrAsString = String.valueOf(iterationNumber);

        int actualCount = 0;
        for(String csvLine : csvLines)
        {
            String[] cells = splitCsvCells(csvLine);

            if (testName.equals(cells[ParticipantAttribute.TEST_NAME.ordinal()])
                && itrAsString.equals(cells[ParticipantAttribute.ITERATION_NUMBER.ordinal()]))
            {
                actualCount++;
            }
        }

        assertEquals("Unexpected number of data rows for test name " + testName + " iteration nunber " + iterationNumber, expectedCount, actualCount);
    }

    private void assertDataRowHasThroughputValues(String csvLine)
    {
        String[] cells = splitCsvCells(csvLine);

        double throughput = Double.valueOf(cells[ParticipantAttribute.THROUGHPUT.ordinal()]);
        int messageThroughput = Integer.valueOf(cells[ParticipantAttribute.MESSAGE_THROUGHPUT.ordinal()]);
        assertTrue("Throughput in line " + csvLine + " is not greater than zero : " + throughput, throughput > 0);
        assertTrue("Message throughput in line " + csvLine + " is not greater than zero : " + messageThroughput, messageThroughput > 0);

    }

    private void assertDataRowHasCorrectTestAndClientName(String testName, String clientName, String participantName, String csvLine)
    {
        String[] cells = splitCsvCells(csvLine);

        // All attributes become cells in the CSV, so this will be true
        assertEquals("Unexpected number of cells in CSV line " + csvLine, ParticipantAttribute.values().length, cells.length);
        assertEquals("Unexpected test name in CSV line " + csvLine, testName, cells[ParticipantAttribute.TEST_NAME.ordinal()]);
        assertEquals("Unexpected client name in CSV line " + csvLine, clientName, cells[ParticipantAttribute.CONFIGURED_CLIENT_NAME.ordinal()]);
        assertEquals("Unexpected participant name in CSV line " + csvLine, participantName, cells[ParticipantAttribute.PARTICIPANT_NAME.ordinal()]);

    }

    private String[] splitCsvCells(String csvLine)
    {
        int DONT_STRIP_EMPTY_LAST_FIELD_FLAG = -1;
        return csvLine.split(",", DONT_STRIP_EMPTY_LAST_FIELD_FLAG);
    }

    private File createTemporaryOutputDirectory() throws IOException
    {
        String tmpDir = System.getProperty("java.io.tmpdir");
        File csvDir = new File(tmpDir, getTestName() + "_" + System.currentTimeMillis());
        csvDir.mkdir();
        csvDir.deleteOnExit();
        return csvDir;
    }

}
