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
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_MAX_NUMBER_OF_RUNS;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_MINIMUM_DELTA;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_START_TARGET_RATE;
import static org.apache.qpid.disttest.ControllerRunner.OUTPUT_DIR_PROP;
import static org.apache.qpid.disttest.ControllerRunner.TEST_CONFIG_PROP;
import static org.apache.qpid.tests.http.HttpTestBase.DEFAULT_BROKER_CONFIG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.disttest.ControllerRunner;
import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.QueueConfig;
import org.apache.qpid.disttest.jms.QpidQueueCreatorFactory;
import org.apache.qpid.disttest.jms.QpidRestAPIQueueCreator;
import org.apache.qpid.disttest.message.ParticipantAttribute;
import org.apache.qpid.disttest.results.aggregation.TestResultAggregator;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.ConfigItem;
import org.apache.qpid.test.utils.TestFileUtils;


@ConfigItem(name = "qpid.initialConfigurationLocation", value = DEFAULT_BROKER_CONFIG )
public class EndToEndTest extends HttpTestBase
{
    private static final String TEST_CONFIG_ITERATIONS = "/org/apache/qpid/systest/disttest/endtoend/iterations.json";
    private static final String TEST_CONFIG_MANYPARTICIPANTS = "/org/apache/qpid/systest/disttest/endtoend/manyparticipants.json";
    private static final String TEST_CONFIG_HILLCLIMBING = "/org/apache/qpid/systest/disttest/endtoend/hillclimbing.js";
    private static final String TEST_CONFIG_ERROR = "/org/apache/qpid/systest/disttest/endtoend/error.json";

    private static final int NUMBER_OF_HEADERS = 1;
    private static final int NUMBER_OF_SUMMARIES = 3;

    private File _outputDir;
    private File _jndiConfigFile;

    @Before
    public void setUp() throws Exception
    {
        System.setProperty("perftests.manangement-url", String.format("http://localhost:%d", getBrokerAdmin().getBrokerAddress(
                BrokerAdmin.PortType.HTTP).getPort()));
        System.setProperty("perftests.broker-virtualhostnode", getVirtualHost());
        System.setProperty("perftests.broker-virtualhost", getVirtualHost());
        System.setProperty(QpidQueueCreatorFactory.QUEUE_CREATOR_CLASS_NAME_SYSTEM_PROPERTY, QpidRestAPIQueueCreator.class.getName());
        _outputDir = createTemporaryOutputDirectory();
        assumeThat("Output dir must not exist", _outputDir.isDirectory(), is(equalTo(true)));
        _jndiConfigFile = getJNDIPropertiesFile();
        QpidRestAPIQueueCreator queueCreator = new QpidRestAPIQueueCreator();
        QueueConfig queueConfig = new QueueConfig("controllerqueue", true, Collections.<String, Object>emptyMap());
        queueCreator.createQueues(null, null, Collections.<QueueConfig>singletonList(queueConfig));
    }

    @After
    public void tearDown()
    {
        try
        {
            if (_outputDir != null && _outputDir.exists())
            {
                TestFileUtils.delete(_outputDir, true);
            }
            if (_jndiConfigFile != null)
            {
                TestFileUtils.delete(_jndiConfigFile, true);
            }
        }
        finally
        {
            System.clearProperty("perftests.manangement-url");
            System.clearProperty("perftests.broker-virtualhostnode");
            System.clearProperty("perftests.broker-virtualhost");
            System.clearProperty(QpidQueueCreatorFactory.QUEUE_CREATOR_CLASS_NAME_SYSTEM_PROPERTY);
        }
    }

    @Test
    public void testIterations() throws Exception
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(TEST_CONFIG_PROP, getTestConfigurationPath(TEST_CONFIG_ITERATIONS));
        arguments.put(JNDI_CONFIG_PROP, _jndiConfigFile.getAbsolutePath());
        arguments.put(OUTPUT_DIR_PROP, _outputDir.getAbsolutePath());
        arguments.put(HILL_CLIMB, "false");

        runController(arguments);
        checkXmlFile(arguments);
        String[] csvLines = getCsvResults(arguments);

        int numberOfParticipants = 2;
        int numberOfIterations = 2;
        int dataRowsPerIteration = numberOfParticipants + NUMBER_OF_SUMMARIES;

        int numberOfExpectedRows = NUMBER_OF_HEADERS + dataRowsPerIteration * numberOfIterations;
        assertThat("Unexpected number of lines in CSV", csvLines.length, is(equalTo(numberOfExpectedRows)));

        final String testName = "Iterations";
        assertDataRowsForIterationArePresent(csvLines, testName, 0, dataRowsPerIteration);
        assertDataRowsForIterationArePresent(csvLines, testName, 1, dataRowsPerIteration);

        assertDataRowHasCorrectTestAndClientName(testName, "", TestResultAggregator.ALL_PARTICIPANTS_NAME, csvLines[4 + dataRowsPerIteration]);
    }

    @Test
    public void testManyParticipants() throws Exception
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(TEST_CONFIG_PROP, getTestConfigurationPath(TEST_CONFIG_MANYPARTICIPANTS));
        arguments.put(JNDI_CONFIG_PROP, _jndiConfigFile.getAbsolutePath());
        arguments.put(OUTPUT_DIR_PROP, _outputDir.getAbsolutePath());
        arguments.put(HILL_CLIMB, "false");

        runController(arguments);
        checkXmlFile(arguments);
        String[] csvLines = getCsvResults(arguments);

        int numberOfParticipants = 4;
        int dataRowsPerIteration = numberOfParticipants + NUMBER_OF_SUMMARIES;

        int numberOfExpectedRows = NUMBER_OF_HEADERS + dataRowsPerIteration;
        assertThat("Unexpected number of lines in CSV", csvLines.length, is(equalTo(numberOfExpectedRows)));

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

        assertThat("Reported total messages sent does not match total sent by producers",
                     actualMessagesSent, is(equalTo(reportedTotalMessagesSent)));
        assertThat("Reported total messages received does not match total received by consumers",
                     actualMessagesReceived, is(equalTo(reportedTotalMessagesReceived)));

    }

    @Test
    public void testHillClimbing() throws Exception
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(TEST_CONFIG_PROP, getTestConfigurationPath(TEST_CONFIG_HILLCLIMBING));
        arguments.put(JNDI_CONFIG_PROP, _jndiConfigFile.getAbsolutePath());
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
        assertThat("Unexpected number of lines in CSV", csvLines.length, is(equalTo(numberOfExpectedRows)));

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

    @Test
    public void testTestScriptCausesError() throws Exception
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(TEST_CONFIG_PROP, getTestConfigurationPath(TEST_CONFIG_ERROR));
        arguments.put(JNDI_CONFIG_PROP, _jndiConfigFile.getAbsolutePath());
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

    public String getTestConfigurationPath(String resource) throws URISyntaxException
    {
        URL url = getClass().getResource(resource);
        assertThat(String.format("Configuration at '%s' is not found", resource), url, is(notNullValue()));
        return new File(url.toURI()).getAbsolutePath();
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

    private String[] getCsvResults(final Map<String, String> args) throws IOException
    {
        String testConfig = args.get(TEST_CONFIG_PROP);
        String expectedCsvFilename = buildOutputFilename(testConfig, ".csv");

        File expectedCsvOutputFile = new File(_outputDir, expectedCsvFilename);
        assertThat("CSV output file must exist", expectedCsvOutputFile.exists(), is(equalTo(true)));
        final String csvContents = new String(Files.readAllBytes(expectedCsvOutputFile.toPath()));
        return csvContents.split("\n");
    }

    private void checkXmlFile(final Map<String, String> args)
    {
        String testConfig = args.get(TEST_CONFIG_PROP);
        String expectedXmlFilename = buildOutputFilename(testConfig, ".xml");

        File expectedXmlOutputFile = new File(_outputDir, expectedXmlFilename);
        assertThat("XML output file must exist", expectedXmlOutputFile.exists(), is(equalTo(true)));
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

        assertThat(String.format("Unexpected number of data rows for test name %s iteration number %d",
                                 testName,
                                 iterationNumber), actualCount, is(equalTo(expectedCount)));
    }

    private void assertDataRowHasThroughputValues(String csvLine)
    {
        String[] cells = splitCsvCells(csvLine);

        double throughput = Double.valueOf(cells[ParticipantAttribute.THROUGHPUT.ordinal()]);
        int messageThroughput = Integer.valueOf(cells[ParticipantAttribute.MESSAGE_THROUGHPUT.ordinal()]);
        assertThat("Throughput in line " + csvLine + " is not greater than zero : " + throughput,
                   throughput > 0,
                   is(equalTo(true)));
        assertThat("Message throughput in line " + csvLine + " is not greater than zero : " + messageThroughput,
                   messageThroughput > 0,
                   is(equalTo(true)));
    }

    private void assertDataRowHasCorrectTestAndClientName(String testName,
                                                          String clientName,
                                                          String participantName,
                                                          String csvLine)
    {
        String[] cells = splitCsvCells(csvLine);

        // All attributes become cells in the CSV, so this will be true
        assertThat("Unexpected number of cells in CSV line " + csvLine,
                   cells.length,
                   is(equalTo(ParticipantAttribute.values().length)));
        assertThat("Unexpected test name in CSV line " + csvLine,
                   cells[ParticipantAttribute.TEST_NAME.ordinal()],
                   is(equalTo(testName)));
        assertThat("Unexpected client name in CSV line " + csvLine,
                   cells[ParticipantAttribute.CONFIGURED_CLIENT_NAME.ordinal()],
                   is(equalTo(clientName)));
        assertThat("Unexpected participant name in CSV line " + csvLine,
                   cells[ParticipantAttribute.PARTICIPANT_NAME.ordinal()],
                   is(equalTo(participantName)));
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

    private File getJNDIPropertiesFile() throws Exception
    {
        String connectionUrl = getConnectionBuilder().setClientId(null).buildConnectionURL();
        String factoryClass = getProtocol() == Protocol.AMQP_1_0
                ? "org.apache.qpid.jms.jndi.JmsInitialContextFactory"
                : "org.apache.qpid.jndi.PropertiesFileInitialContextFactory";

        Properties properties = new Properties();
        properties.put("connectionfactory.connectionfactory", connectionUrl);
        properties.put("java.naming.factory.initial", factoryClass);
        properties.put("queue.controllerqueue", "controllerqueue");

        File propertiesFile = Files.createTempFile("perftests", ".jndi.properties").toFile();
        try (OutputStream os = new FileOutputStream(propertiesFile))
        {
            properties.store(os, null);
        }
        return propertiesFile;
    }

}
