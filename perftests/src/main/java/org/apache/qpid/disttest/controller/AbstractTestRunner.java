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

package org.apache.qpid.disttest.controller;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.QueueConfig;
import org.apache.qpid.disttest.controller.config.TestInstance;
import org.apache.qpid.disttest.jms.ControllerJmsDelegate;
import org.apache.qpid.disttest.message.Command;
import org.apache.qpid.disttest.message.CommandType;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.message.Response;
import org.apache.qpid.disttest.message.StartDataCollectionCommand;
import org.apache.qpid.disttest.message.StartTestCommand;
import org.apache.qpid.disttest.message.TearDownTestCommand;

public abstract class AbstractTestRunner implements ITestRunner
{
    public static final long WAIT_FOREVER = -1;

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTestRunner.class);
    private static final long PARTICIPANT_RESULTS_LOG_INTERVAL = 60000;
    private final long _commandResponseTimeout;
    private final ParticipatingClients _participatingClients;
    private final TestInstance _testInstance;
    private final AtomicInteger _errorResponseCount = new AtomicInteger();
    private final Set<CommandType> _setOfResponsesToExpect = Collections.synchronizedSet(new HashSet<CommandType>());

    /** Length of time to await test results or {@value #WAIT_FOREVER} */
    private final long _testResultTimeout;
    private CountDownLatch _testResultsLatch;
    protected ControllerJmsDelegate _jmsDelegate;
    private volatile SettableFuture<Object> _testCleanupComplete;
    private final Thread _removeQueuesShutdownHook = new Thread("shutdown-hook")
    {
        @Override
        public void run()
        {
            LOGGER.info("Shutdown intercepted: deleting test queues");
            SettableFuture cleanupComplete = _testCleanupComplete;
            try
            {
                cleanupComplete.get(30, TimeUnit.SECONDS);
                LOGGER.info("Test queues deleted");
            }
            catch (InterruptedException| TimeoutException | ExecutionException e)
            {
                LOGGER.warn("Failed to delete test queues during shutdown."
                            + "Manual clean-up of queues may be required.", e);
            }
        }
    };
    private volatile CountDownLatch _commandResponseLatch = null;

    protected AbstractTestRunner(ParticipatingClients participatingClients,
                                 long commandResponseTimeout,
                                 TestInstance testInstance,
                                 ControllerJmsDelegate jmsDelegate,
                                 long testResultTimeout)
    {
        _participatingClients = participatingClients;
        _commandResponseTimeout = commandResponseTimeout;
        _testInstance = testInstance;
        _jmsDelegate = jmsDelegate;
        _testResultTimeout = testResultTimeout;
    }

    private void createQueues()
    {
        List<QueueConfig> queues = _testInstance.getQueues();
        if (!queues.isEmpty())
        {
            _jmsDelegate.createQueues(queues);
        }
    }

    public TestInstance getTestInstance()
    {
        return _testInstance;
    }

    private void sendTestSetupCommands()
    {
        List<CommandForClient> commandsForAllClients = _testInstance.createCommands();
        validateCommands(commandsForAllClients);
        final int numberOfCommandsToSend = commandsForAllClients.size();
        _commandResponseLatch = new CountDownLatch(numberOfCommandsToSend);

        LOGGER.debug("About to send {} command(s)", numberOfCommandsToSend);

        for (CommandForClient commandForClient : commandsForAllClients)
        {
            String configuredClientName = commandForClient.getClientName();
            String registeredClientName = _participatingClients.getRegisteredNameFromConfiguredName(configuredClientName);

            Command command = commandForClient.getCommand();

            LOGGER.debug("Sending command : {} ", command);

            sendCommandInternal(registeredClientName, command);
        }


    }

    protected void validateCommands(List<CommandForClient> commandsForAllClients)
    {
        // pass
    }

    private void awaitCommandResponses()
    {
        awaitLatch(_commandResponseLatch, _commandResponseTimeout, "Timed out waiting for command responses");

        if (_errorResponseCount.get() > 0)
        {
            throw new DistributedTestException("One or more clients were unable to successfully process commands. "
                                               + _errorResponseCount + " command(s) generated an error response.");
        }
    }

    private void processCommandResponse(final Response response)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Received response for command " + response);
        }

        if (response.hasError())
        {
            _errorResponseCount.incrementAndGet();
        }
        checkForResponseError(response);
        _commandResponseLatch.countDown();
    }

    private void awaitTestResults()
    {
        long timeout = _testResultTimeout;
        DistributedTestException lastException = null;

        boolean waitForever = _testResultTimeout == WAIT_FOREVER;
        final long interval = waitForever ? PARTICIPANT_RESULTS_LOG_INTERVAL : Math.min(PARTICIPANT_RESULTS_LOG_INTERVAL, _testResultTimeout);

        while(_testResultsLatch.getCount() > 0 && (waitForever || timeout > 0))
        {
            try
            {
                awaitLatch(_testResultsLatch, interval, "still waiting for participant results");
            }
            catch (DistributedTestException e)
            {
                lastException = e;
                LOGGER.info(e.getMessage());
            }

            if (!waitForever)
            {
                timeout =- interval;
            }
        }

        if (_testResultsLatch.getCount() > 0)
        {
            throw lastException;
        }
    }

    private void deleteQueues()
    {
        List<QueueConfig> queues = _testInstance.getQueues();
        if (!queues.isEmpty())
        {
            _jmsDelegate.deleteQueues(queues);
        }
    }

    private void sendCommandToParticipatingClients(final Command command)
    {
        Collection<String> participatingRegisteredClients = _participatingClients.getRegisteredNames();
        final int numberOfClients = participatingRegisteredClients.size();
        _commandResponseLatch = new CountDownLatch(numberOfClients);

        LOGGER.debug("About to send command {} to {} clients", command, numberOfClients);

        for (final String clientName : participatingRegisteredClients)
        {
            LOGGER.debug("Sending command : {} ", command);
            sendCommandInternal(clientName, command);
        }
    }

    private void processParticipantResult(ParticipantResult result, TestResult testResult)
    {
        setOriginalTestDetailsOn(result);

        testResult.addParticipantResult(result);
        LOGGER.debug("Received result {}", result);

        _testResultsLatch.countDown();
        checkForResponseError(result);
    }

    private void setOriginalTestDetailsOn(ParticipantResult result)
    {
        // Client knows neither the configured client name nor test name
        String registeredClientName = result.getRegisteredClientName();
        String configuredClient = _participatingClients.getConfiguredNameFromRegisteredName(registeredClientName);

        result.setConfiguredClientName(configuredClient);
        result.setTestName(_testInstance.getName());
        result.setIterationNumber(_testInstance.getIterationNumber());
    }

    private void sendCommandInternal(String registeredClientName, Command command)
    {
        _setOfResponsesToExpect.add(command.getType());
        _jmsDelegate.sendCommandToClient(registeredClientName, command);
    }

    private void awaitLatch(CountDownLatch latch, long timeout, String message)
    {
        try
        {
            final boolean countedDownOK = latch.await(timeout, TimeUnit.MILLISECONDS);
            if (!countedDownOK)
            {
                final long latchCount = latch.getCount();
                String formattedMessage = "After " + timeout + "ms ... " + message + " ... Expecting " + latchCount + " more response(s).";
                LOGGER.info(formattedMessage); // info rather than error because we time out periodically so we can log progress
                throw new DistributedTestException(formattedMessage);
            }
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private void checkForResponseError(final Response response)
    {
        if (response.hasError())
        {
            LOGGER.error("Client {} reported error {}", response.getRegisteredClientName()  ,response);
        }
    }


    protected TestResult doIt()
    {
        TestResult testResult = new TestResult(_testInstance.getName());
        _testResultsLatch = new CountDownLatch(_testInstance.getTotalNumberOfParticipants());
        _errorResponseCount.set(0);

        ParticipantResultListener participantResultListener = new ParticipantResultListener(testResult);
        TestCommandResponseListener testCommandResponseListener = new TestCommandResponseListener();

        try
        {
            _jmsDelegate.addCommandListener(testCommandResponseListener);
            _jmsDelegate.addCommandListener(participantResultListener);

            runParts();

            return testResult;
        }
        catch(RuntimeException e)
        {
            LOGGER.error("Couldn't run test", e);
            throw e;
        }
        finally
        {
            _jmsDelegate.removeCommandListener(participantResultListener);
            _jmsDelegate.removeCommandListener(testCommandResponseListener);
        }
    }

    private void runParts()
    {
        boolean queuesCreated = false;

        try
        {
            _testCleanupComplete = SettableFuture.create();
            createQueues();
            queuesCreated = true;

            Runtime.getRuntime().addShutdownHook(_removeQueuesShutdownHook);

            sendTestSetupCommands();
            awaitCommandResponses();
            sendCommandToParticipatingClients(new StartTestCommand());
            awaitCommandResponses();
            sendCommandToParticipatingClients(new StartDataCollectionCommand());
            awaitCommandResponses();

            awaitTestResults();

            sendCommandToParticipatingClients(new TearDownTestCommand());
            awaitCommandResponses();
        }
        finally
        {

            try
            {
                if (queuesCreated)
                {
                    deleteQueues();
                }
            }
            finally
            {
                _testCleanupComplete.set(null);
            }

            Runtime.getRuntime().removeShutdownHook(_removeQueuesShutdownHook);
        }
    }

    private final class ParticipantResultListener implements CommandListener
    {
        private final TestResult _testResult;

        public ParticipantResultListener(TestResult testResult)
        {
             _testResult = testResult;
        }

        @Override
        public boolean supports(Command command)
        {
            return command instanceof ParticipantResult;
        }

        @Override
        public void processCommand(Command command)
        {
            processParticipantResult((ParticipantResult) command, _testResult);

        }
    }

    private final class TestCommandResponseListener implements CommandListener
    {
        @Override
        public void processCommand(Command command)
        {
            processCommandResponse((Response)command);
        }

        @Override
        public boolean supports(Command command)
        {
            CommandType type = command.getType();
            if (type == CommandType.RESPONSE)
            {
                Response response = (Response)command;
                return _setOfResponsesToExpect.contains(response.getInReplyToCommandType());
            }
            return false;
        }
    }
}
