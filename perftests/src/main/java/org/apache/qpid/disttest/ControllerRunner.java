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
 *
 */
package org.apache.qpid.disttest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.naming.Context;

import org.apache.qpid.disttest.controller.Controller;
import org.apache.qpid.disttest.controller.ResultsForAllTests;
import org.apache.qpid.disttest.controller.config.Config;
import org.apache.qpid.disttest.controller.config.ConfigReader;
import org.apache.qpid.disttest.db.ResultsDbWriter;
import org.apache.qpid.disttest.jms.ControllerJmsDelegate;
import org.apache.qpid.disttest.results.CompositeResultsWriter;
import org.apache.qpid.disttest.results.ResultsCsvWriter;
import org.apache.qpid.disttest.results.ResultsXmlWriter;
import org.apache.qpid.disttest.results.aggregation.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerRunner extends AbstractRunner
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRunner.class);

    public static final String TEST_CONFIG_PROP = "test-config";
    public static final String DISTRIBUTED_PROP = "distributed";
    public static final String OUTPUT_DIR_PROP = "outputdir";
    public static final String WRITE_TO_DB = "writeToDb";
    public static final String RUN_ID = "runId";
    public static final String REGISTRATION_TIMEOUT = "registrationTimeout";
    public static final String COMMAND_RESPONSE_TIMEOUT = "commandResponseTimeout";
    public static final String HILL_CLIMB = "hill-climb";
    public static final String HILL_CLIMBER_MAX_NUMBER_OF_RUNS = "hill-climber.max-runs";
    public static final String HILL_CLIMBER_START_TARGET_RATE = "hill-climber.start-target-rate";
    public static final String HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD = "hill-climber.production-to-target-ratio-success-threshold";
    public static final String HILL_CLIMBER_CONSUMPTION_TO_PRODUCTION_RATIO_SUCCESS_THRESHOLD = "hill-climber.consumption-to-production-ratio-success-threshold";
    public static final String HILL_CLIMBER_MINIMUM_DELTA = "hill-climber.minimum-delta";
    public static final String HILL_CLIMBER_BIAS = "hill-climber.bias";

    private static final String TEST_CONFIG_DEFAULT = "perftests-config.json";
    private static final String DISTRIBUTED_DEFAULT = "false";
    private static final String OUTPUT_DIR_DEFAULT = ".";
    private static final String WRITE_TO_DB_DEFAULT = "false";
    private static final String REGISTRATION_TIMEOUT_DEFAULT = String.valueOf(60 * 1000);
    private static final String COMMAND_RESPONSE_TIMEOUT_DEFAULT = String.valueOf(120 * 1000);
    private static final String HILL_CLIMB_DEFAULT = "false";
    private static final String HILL_CLIMBER_MAX_NUMBER_OF_RUNS_DEFAULT = "3";
    private static final String HILL_CLIMBER_START_TARGET_RATE_DEFAULT = "1025";
    private static final String HILL_CLIMBER_CONSUMPTION_TO_PRODUCTION_RATIO_SUCCESS_THRESHOLD_DEFAULT = "0.95";
    private static final String HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD_DEFAULT = "0.95";
    private static final String HILL_CLIMBER_MINIMUM_DELTA_DEFAULT = "1";
    private static final String HILL_CLIMBER_BIAS_DEFAULT = "-1";

    private final Aggregator _aggregator = new Aggregator();

    private final ConfigFileHelper _configFileHelper = new ConfigFileHelper();

    private final CompositeResultsWriter _resultsWriter = new CompositeResultsWriter();


    public ControllerRunner()
    {
        getCliOptions().put(TEST_CONFIG_PROP, TEST_CONFIG_DEFAULT);
        getCliOptions().put(DISTRIBUTED_PROP, DISTRIBUTED_DEFAULT);
        getCliOptions().put(OUTPUT_DIR_PROP, OUTPUT_DIR_DEFAULT);
        getCliOptions().put(WRITE_TO_DB, WRITE_TO_DB_DEFAULT);
        getCliOptions().put(RUN_ID, null);
        getCliOptions().put(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT_DEFAULT);
        getCliOptions().put(COMMAND_RESPONSE_TIMEOUT, COMMAND_RESPONSE_TIMEOUT_DEFAULT);
        getCliOptions().put(HILL_CLIMB, HILL_CLIMB_DEFAULT);
        getCliOptions().put(HILL_CLIMBER_MAX_NUMBER_OF_RUNS,
                            HILL_CLIMBER_MAX_NUMBER_OF_RUNS_DEFAULT);
        getCliOptions().put(HILL_CLIMBER_START_TARGET_RATE,
                            HILL_CLIMBER_START_TARGET_RATE_DEFAULT);
        getCliOptions().put(HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD,
                            HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD_DEFAULT);
        getCliOptions().put(HILL_CLIMBER_CONSUMPTION_TO_PRODUCTION_RATIO_SUCCESS_THRESHOLD,
                            HILL_CLIMBER_CONSUMPTION_TO_PRODUCTION_RATIO_SUCCESS_THRESHOLD_DEFAULT);
        getCliOptions().put(HILL_CLIMBER_MINIMUM_DELTA,
                            HILL_CLIMBER_MINIMUM_DELTA_DEFAULT);
        getCliOptions().put(HILL_CLIMBER_BIAS,
                            HILL_CLIMBER_BIAS_DEFAULT);
    }

    public static void main(String[] args) throws Exception
    {
        ControllerRunner runner = new ControllerRunner();
        runner.parseArgumentsIntoConfig(args);
        runner.runController();
    }

    public void runController() throws Exception
    {
        Context context = getContext();
        setUpResultsWriters();

        ControllerJmsDelegate jmsDelegate = new ControllerJmsDelegate(context);

        try
        {
            Controller controller = new Controller(jmsDelegate, getCliOptions());

            String testConfigPath = getCliOptions().get(ControllerRunner.TEST_CONFIG_PROP);
            List<String> testConfigFiles = _configFileHelper.getTestConfigFiles(testConfigPath);
            Collection<ClientRunner> clients = createClientsIfNotDistributed(testConfigFiles);

            try
            {
                runTests(controller, testConfigFiles);
            }
            finally
            {
                controller.stopAllRegisteredClients();
                awaitClientShutdown(clients);
            }
        }
        finally
        {
            tearDownResultsWriters();
            jmsDelegate.closeConnections();
        }
    }

    private void setUpResultsWriters()
    {
        setUpResultFileWriters();
        setUpResultsDbWriter();
        _resultsWriter.begin();
    }

    private void setUpResultsDbWriter()
    {
        String writeToDbStr = getCliOptions().get(WRITE_TO_DB);
        if(Boolean.valueOf(writeToDbStr))
        {
            String runId = getCliOptions().get(RUN_ID);
            _resultsWriter.addWriter(new ResultsDbWriter(getContext(), runId));
        }
    }

    void setUpResultFileWriters()
    {
        String outputDirString = getCliOptions().get(ControllerRunner.OUTPUT_DIR_PROP);
        File outputDir = new File(outputDirString);
        _resultsWriter.addWriter(new ResultsCsvWriter(outputDir));
        _resultsWriter.addWriter(new ResultsXmlWriter(outputDir));
    }

    private void tearDownResultsWriters()
    {
        _resultsWriter.end();
    }

    private void runTests(Controller controller, List<String> testConfigFiles)
    {
        boolean testError = false;

        try
        {
            List<ResultsForAllTests> results = new ArrayList<>();

            for (String testConfigFile : testConfigFiles)
            {
                final Config testConfig = buildTestConfigFrom(testConfigFile);
                controller.setConfig(testConfig);

                controller.awaitClientRegistrations();

                LOGGER.info("Running test : {} ", testConfigFile);
                ResultsForAllTests testResult = runTest(controller, testConfigFile);
                if (testResult.hasErrors())
                {
                    testError = true;
                }
                results.add(testResult);
            }
        }
        catch(Exception e)
        {
            LOGGER.error("Problem running test", e);
            throw new DistributedTestException("Problem running tests", e);
        }

        if (testError)
        {
            throw new DistributedTestException("One or more tests ended in error");
        }
    }

    private ResultsForAllTests runTest(Controller controller, String testConfigFile)
    {
        final Config testConfig = buildTestConfigFrom(testConfigFile);
        controller.setConfig(testConfig);

        ResultsForAllTests rawResultsForAllTests = controller.runAllTests();
        ResultsForAllTests resultsForAllTests = _aggregator.aggregateResults(rawResultsForAllTests);

        _resultsWriter.writeResults(resultsForAllTests, testConfigFile);

        return resultsForAllTests;
    }

    private Collection<ClientRunner> createClientsIfNotDistributed(final List<String> testConfigFiles)
    {
        if(!isDistributed())
        {
            int maxNumberOfClients = 0;
            for (String testConfigFile : testConfigFiles)
            {
                final Config testConfig = buildTestConfigFrom(testConfigFile);
                final int numClients = testConfig.getTotalNumberOfClients();
                maxNumberOfClients = Math.max(numClients, maxNumberOfClients);
            }

            //we must create the required test clients, running in single-jvm mode
            Collection<ClientRunner> runners = new ArrayList<>(maxNumberOfClients);
            for (int i = 1; i <= maxNumberOfClients; i++)
            {
                ClientRunner clientRunner = new ClientRunner();
                clientRunner.setJndiPropertiesFileLocation(getJndiConfig());
                clientRunner.runClients();
                runners.add(clientRunner);

            }
            return runners;
        }
        return Collections.emptyList();
    }

    private void awaitClientShutdown(final Collection<ClientRunner> clients)
    {
        for(ClientRunner client: clients)
        {
            client.awaitShutdown();
        }
    }

    private Config buildTestConfigFrom(String testConfigFile)
    {
        ConfigReader configReader = new ConfigReader();
        Config testConfig;
        try
        {
            testConfig = configReader.getConfigFromFile(testConfigFile);
        }
        catch (IOException e)
        {
            throw new DistributedTestException("Exception while loading test config from '" + testConfigFile + "'", e);
        }
        return testConfig;
    }

    private boolean isDistributed()
    {
        return Boolean.valueOf(getCliOptions().get(ControllerRunner.DISTRIBUTED_PROP));
    }


}
