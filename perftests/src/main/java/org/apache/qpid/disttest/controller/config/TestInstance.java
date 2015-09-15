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
package org.apache.qpid.disttest.controller.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.CommandForClient;
import org.apache.qpid.disttest.message.Command;
import org.apache.qpid.disttest.message.CreateParticipantCommand;
import org.apache.qpid.disttest.message.CreateProducerCommand;

public class TestInstance
{
    private static final IterationValue EMPTY_ITERATION_VALUES = new IterationValue();

    private TestConfig _testConfig;
    private IterationValue _iterationValue;
    private int _iterationNumber;
    private double _producerRate;

    public TestInstance(TestConfig testConfig, int iterationNumber, IterationValue iterationValue)
    {
        _testConfig = testConfig;
        _iterationNumber = iterationNumber;
        _iterationValue = iterationValue;
    }

    public TestInstance(TestConfig testConfig)
    {
        this(testConfig, 0, EMPTY_ITERATION_VALUES);
    }

    public List<CommandForClient> createCommands()
    {
        List<CommandForClient> commands = _testConfig.createCommands();
        List<CommandForClient> newCommands = new ArrayList<CommandForClient>(commands.size());

        double ratePerProducer = calculateRatePerProducer(_producerRate, commands);
        for (CommandForClient commandForClient : commands)
        {
            String clientName = commandForClient.getClientName();
            Command command = commandForClient.getCommand();

            _iterationValue.applyToCommand(command);

            if (command instanceof CreateProducerCommand)
            {
                CreateProducerCommand producerCommand = (CreateProducerCommand) command;
                producerCommand.setRate(ratePerProducer);
            }

            if (command instanceof CreateParticipantCommand)
            {
                CreateParticipantCommand participantCommand = (CreateParticipantCommand) command;
                if ((participantCommand.getNumberOfMessages() <= 0 && participantCommand.getMaximumDuration() <= 0))
                {
                    throw new DistributedTestException("Test '" + getName()
                            + "' must specify a positive integer value for numberOfMessages or maximumDuration");
                }
            }

            newCommands.add(new CommandForClient(clientName, command));
        }

        return newCommands;

    }

    public String getName()
    {
        return _testConfig.getName();
    }

    public int getIterationNumber()
    {
        return _iterationNumber;
    }

    public int getTotalNumberOfParticipants()
    {
        return _testConfig.getTotalNumberOfParticipants();
    }

    public List<QueueConfig> getQueues()
    {
        return _testConfig.getQueues();
    }

    public List<String> getClientNames()
    {
        return _testConfig.getClientNames();
    }

    @Override
    public String toString()
    {
        return "TestInstance[" +
               "testName=" + _testConfig.getName() +
               ", iterationNumber=" + _iterationNumber +
               ']';
    }

    public void setProducerRate(double producerRate)
    {
        _producerRate = producerRate;
    }

    private double calculateRatePerProducer(final double totalRate, final List<CommandForClient> commands)
    {
        int numberOfProducers = 0;
        for (CommandForClient commandForClient : commands)
        {
            Command command = commandForClient.getCommand();
            if (command instanceof CreateProducerCommand)
            {
                numberOfProducers++;
            }
        }
        return totalRate / numberOfProducers;
    }
}
