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
package org.apache.qpid.disttest.controller;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.disttest.ControllerRunner;
import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.Config;
import org.apache.qpid.disttest.controller.config.TestInstance;
import org.apache.qpid.disttest.jms.ControllerJmsDelegate;
import org.apache.qpid.disttest.message.*;
import org.apache.qpid.disttest.results.aggregation.ITestResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Controller
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);

    private final long _registrationTimeout;
    private final long _commandResponseTimeout;

    private final ControllerJmsDelegate _jmsDelegate;

    private final TestRunnerFactory _testRunnerFactory;
    private final Map<String, String> _options;

    private volatile CountDownLatch _stopClientsResponseLatch = null;
    private Config _config;
    private ClientRegistry _clientRegistry;


    public Controller(final ControllerJmsDelegate jmsDelegate,
                      final Map<String, String> options)
    {
        _jmsDelegate = jmsDelegate;
        _registrationTimeout = Integer.parseInt(options.get(ControllerRunner.REGISTRATION_TIMEOUT));
        _commandResponseTimeout = Integer.parseInt(options.get(ControllerRunner.COMMAND_RESPONSE_TIMEOUT));
        _testRunnerFactory = new TestRunnerFactory();
        _clientRegistry = new ClientRegistry();
        _options = options;

        _jmsDelegate.addCommandListener(new RegisterClientCommandListener());
        _jmsDelegate.addCommandListener(new StopClientResponseListener());
        _jmsDelegate.start();
    }

    public void setConfig(Config config)
    {
        _config = config;
        validateConfiguration();
    }

    public void awaitClientRegistrations()
    {
        LOGGER.info("Awaiting client registrations");

        final int numberOfAbsentClients = _clientRegistry.awaitClients(_config.getTotalNumberOfClients(), _registrationTimeout);
        if (numberOfAbsentClients > 0)
        {
            String formattedMessage = String.format("Timed out waiting for registrations. Expecting %d more registrations", numberOfAbsentClients);
            throw new DistributedTestException(formattedMessage);
        }

    }

    private void validateConfiguration()
    {
        if (_config == null || _config.getTotalNumberOfClients() == 0)
        {
            throw new DistributedTestException("No controller config or no clients specified in test config");
        }
    }

    private void awaitStopResponses(CountDownLatch latch, long timeout)
    {
        String message = "Timed out after %d waiting for stop command responses. Expecting %d more responses.";

        try
        {
            boolean countedDownOK = latch.await(timeout, TimeUnit.MILLISECONDS);
            if (!countedDownOK)
            {
                long latchCount = latch.getCount();
                String formattedMessage = String.format(message, timeout, latchCount);
                LOGGER.error(formattedMessage);
                throw new DistributedTestException(formattedMessage);
            }
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    public void registerClient(final RegisterClientCommand registrationCommand)
    {
        final String clientName = registrationCommand.getClientName();

        _jmsDelegate.registerClient(registrationCommand);
        _clientRegistry.registerClient(clientName);
    }

    void processStopClientResponse(final Response response)
    {
        // TODO clientRegistry should expose a deregisterClient
        _stopClientsResponseLatch.countDown();
        if (response.hasError())
        {
            LOGGER.error("Client " + response.getRegisteredClientName() + " reported exception in response to command : " +
                    response.getErrorMessage());
        }
    }

    public void stopAllRegisteredClients()
    {
        Collection<String> registeredClients = _clientRegistry.getClients();

        LOGGER.info("Stopping all clients");
        _stopClientsResponseLatch = new CountDownLatch(registeredClients.size());
        Command command = new StopClientCommand();
        for (final String clientName : registeredClients)
        {
            _jmsDelegate.sendCommandToClient(clientName, command);
        }

        awaitStopResponses(_stopClientsResponseLatch, _commandResponseTimeout);

        LOGGER.info("Stopped all clients");
    }


    public ResultsForAllTests runAllTests()
    {
        LOGGER.info("Running all tests");

        ResultsForAllTests resultsForAllTests = new ResultsForAllTests();

        for (TestInstance testInstance : _config.getTests())
        {
            ParticipatingClients participatingClients = new ParticipatingClients(_clientRegistry, testInstance.getClientNames());

            ITestRunner runner = _testRunnerFactory.createTestRunner(participatingClients,
                    testInstance,
                    _jmsDelegate,
                    _commandResponseTimeout,
                    AbstractTestRunner.WAIT_FOREVER,
                    _options);
            LOGGER.info("Running test {}. Participating clients: {}",
                    testInstance, participatingClients.getRegisteredNames());

            ITestResult result = runner.run();
            if (result != null)
            {
                LOGGER.info("Finished test {}, result {}", testInstance, result);
                resultsForAllTests.add(result);
            }
            else
            {
                LOGGER.warn("Finished test {} without producing a result.", testInstance);
            }
        }

        return resultsForAllTests;
    }

    private final class StopClientResponseListener implements CommandListener
    {
        @Override
        public boolean supports(Command command)
        {
            return command.getType() == CommandType.RESPONSE && ((Response)command).getInReplyToCommandType() == CommandType.STOP_CLIENT;
        }

        @Override
        public void processCommand(Command command)
        {
            processStopClientResponse((Response)command);
        }
    }

    private final class RegisterClientCommandListener implements
            CommandListener
    {
        @Override
        public boolean supports(Command command)
        {
            return command.getType() == CommandType.REGISTER_CLIENT;
        }

        @Override
        public void processCommand(Command command)
        {
            registerClient((RegisterClientCommand)command);
        }
    }

    void setClientRegistry(ClientRegistry clientRegistry)
    {
        _clientRegistry = clientRegistry;
    }

}
