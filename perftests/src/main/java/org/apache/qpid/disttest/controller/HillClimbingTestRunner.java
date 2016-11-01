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
package org.apache.qpid.disttest.controller;

import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_BIAS;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_START_TARGET_RATE;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_MAX_NUMBER_OF_RUNS;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_CONSUMPTION_TO_PRODUCTION_RATIO_SUCCESS_THRESHOLD;
import static org.apache.qpid.disttest.ControllerRunner.HILL_CLIMBER_MINIMUM_DELTA;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.TestInstance;
import org.apache.qpid.disttest.jms.ControllerJmsDelegate;
import org.apache.qpid.disttest.message.Command;
import org.apache.qpid.disttest.message.CreateParticipantCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


public class HillClimbingTestRunner extends AbstractTestRunner
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HillClimbingTestRunner.class);
    private final Map<String, String> _options;

    public HillClimbingTestRunner(ParticipatingClients participatingClients,
                                  TestInstance testInstance,
                                  ControllerJmsDelegate jmsDelegate,
                                  long commandResponseTimeout,
                                  long testResultTimeout, Map<String, String> options)
    {
        super(participatingClients, commandResponseTimeout, testInstance, jmsDelegate, testResultTimeout);
        _options = options;
    }

    @Override
    public TestResult run()
    {
        final double consumptionToProductionRatioSuccessThreshold = Double.valueOf(_options.get(HILL_CLIMBER_PRODUCTION_TO_TARGET_RATIO_SUCCESS_THRESHOLD));
        final double productionToTargetRatioSuccessThreshold = Double.valueOf(_options.get(HILL_CLIMBER_CONSUMPTION_TO_PRODUCTION_RATIO_SUCCESS_THRESHOLD));
        final double hillClimberMinimumDelta = Double.valueOf(_options.get(HILL_CLIMBER_MINIMUM_DELTA));
        final int maxNumberOfRuns = Integer.valueOf(_options.get(HILL_CLIMBER_MAX_NUMBER_OF_RUNS));
        double rate = Double.valueOf(_options.get(HILL_CLIMBER_START_TARGET_RATE));
        double bias = Double.valueOf(_options.get(HILL_CLIMBER_BIAS));
        if (bias < 0)
        {
            bias = 1. / (maxNumberOfRuns + 1.);
        }
        double initialDelta = rate - 1;
        HillClimber hillClimber = new HillClimber(rate, initialDelta, bias);
        TestResult bestSuccessfulTestResult = null;
        int iteration = 1;
        do
        {
            boolean success;
            int runNumber = 1;
            do
            {
                LOGGER.info("Running test : {}, iteration : {}, run number : {}/{}, rate: {}",
                        getTestInstance(), iteration, runNumber, maxNumberOfRuns, rate);
                getTestInstance().setProducerRate(rate);

                TestResult testResult = doIt();

                double producedMessageRate = testResult.getProducedMessageRate();
                double consumedMessageRate = testResult.getConsumedMessageRate();

                double productionToTargetRatio = producedMessageRate / rate;
                double consumptionToProductionRatio = consumedMessageRate / producedMessageRate;

                success = ((consumptionToProductionRatio > consumptionToProductionRatioSuccessThreshold)
                        && (productionToTargetRatio > productionToTargetRatioSuccessThreshold));
                if (!success)
                {
                    LOGGER.debug("Target throughput ({}) missed, retrying.", rate);
                }
                else
                {
                    bestSuccessfulTestResult = testResult;
                }
                LOGGER.info(String.format("Completed test %s, run number %d/%d, result %s," +
                                " target rate : %.1f, production to target ratio : %.1f%%," +
                                " consumption to production ratio : %.1f%%.",
                        (success ? "successfully" : "unsuccessfully"),
                        runNumber, maxNumberOfRuns, testResult,
                        rate, productionToTargetRatio * 100.0, consumptionToProductionRatio * 100.0));
                runNumber++;
            }
            while (!success && runNumber <= maxNumberOfRuns);

            if (success)
            {
                LOGGER.info("Target throughput ({}) reached.", rate);
                rate = hillClimber.nextHigher();
            }
            else
            {
                LOGGER.info("Target throughput ({}) missed.", rate);
                rate = hillClimber.nextLower();
            }
            ++iteration;
        }
        while (hillClimber.getCurrentDelta() >= hillClimberMinimumDelta && rate > 0);

        return bestSuccessfulTestResult;
    }

    @Override
    protected void validateCommands(List<CommandForClient> commandsForAllClients)
    {
        super.validateCommands(commandsForAllClients);
        for (CommandForClient commandForClient : commandsForAllClients)
        {
            Command command = commandForClient.getCommand();
            if (command instanceof CreateParticipantCommand)
            {
                final CreateParticipantCommand createParticipantCommand = (CreateParticipantCommand) command;
                if (createParticipantCommand.getNumberOfMessages() > 0)
                {
                    throw new DistributedTestException("HillClimbingTestRunner does not support specifying numberOfMessages");
                }
            }
        }
    }
}
