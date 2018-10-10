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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.TestInstance;
import org.apache.qpid.disttest.jms.ControllerJmsDelegate;
import org.apache.qpid.disttest.message.Command;
import org.apache.qpid.disttest.message.CommandType;
import org.apache.qpid.disttest.message.CreateConnectionCommand;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.message.Response;
import org.apache.qpid.disttest.message.StartDataCollectionCommand;
import org.apache.qpid.disttest.message.StartTestCommand;
import org.apache.qpid.disttest.message.TearDownTestCommand;

import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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

public class OrdinaryTestRunnerTest extends UnitTestBase
{
    private static final String PARTICIPANT_NAME = "TEST_PARTICIPANT_NAME";

    private static final String CLIENT1_REGISTERED_NAME = "client-uid1";
    private static final String CLIENT1_CONFIGURED_NAME = "client1";

    private static final long COMMAND_RESPONSE_TIMEOUT = 1000;
    private static final long TEST_RESULT_TIMEOUT = 2000;

    private OrdinaryTestRunner _testRunner;
    private TestInstance _testInstance;
    private ControllerJmsDelegate _respondingJmsDelegate;
    private ParticipatingClients _participatingClients;
    private Set<CommandListener> _spiedCommandListeners = new HashSet<>();

    @Before
    public void setUp() throws Exception
    {
        _respondingJmsDelegate = mock(ControllerJmsDelegate.class);

        // Spy on the command listeners, so we can pass responses back to the test runner
        doAnswer(new Answer()
        {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
            {
                CommandListener listener = (CommandListener) invocation.getArguments()[0];
                _spiedCommandListeners.add(listener);
                return null;
            }
        }).when(_respondingJmsDelegate).addCommandListener(any(CommandListener.class));

        doAnswer(new Answer()
        {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable
            {
                CommandListener listener = (CommandListener) invocation.getArguments()[0];
                _spiedCommandListeners.remove(listener);
                return null;
            }
        }).when(_respondingJmsDelegate).removeCommandListener(any(CommandListener.class));


        _participatingClients = mock(ParticipatingClients.class);
        when(_participatingClients.getRegisteredNameFromConfiguredName(CLIENT1_CONFIGURED_NAME)).thenReturn(CLIENT1_REGISTERED_NAME);
        when(_participatingClients.getConfiguredNameFromRegisteredName(CLIENT1_REGISTERED_NAME)).thenReturn(
                CLIENT1_CONFIGURED_NAME);
        when(_participatingClients.getRegisteredNames()).thenReturn(Collections.singleton(CLIENT1_REGISTERED_NAME));
    }

    @Test
    public void testSuccessfulTestRunReturnsTestResult()
    {
        ParticipantResult participantResult = new ParticipantResult(PARTICIPANT_NAME);

        configureMockToReturnResponseTo(CreateConnectionCommand.class, new Response(CLIENT1_REGISTERED_NAME, CommandType.CREATE_CONNECTION), null);
        configureMockToReturnResponseTo(StartTestCommand.class, new Response(CLIENT1_REGISTERED_NAME, CommandType.START_TEST), null);
        configureMockToReturnResponseTo(StartDataCollectionCommand.class, new Response(CLIENT1_REGISTERED_NAME, CommandType.START_DATA_COLLECTION),
                                        participantResult);
        configureMockToReturnResponseTo(TearDownTestCommand.class, new Response(CLIENT1_REGISTERED_NAME, CommandType.TEAR_DOWN_TEST), null);

        _testInstance =  createTestInstanceWithConnection();

        _testRunner = new OrdinaryTestRunner(_participatingClients,
                                             _testInstance,
                                             _respondingJmsDelegate,
                                             COMMAND_RESPONSE_TIMEOUT,
                                             TEST_RESULT_TIMEOUT);
        TestResult testResult = _testRunner.run();
        assertNotNull(testResult);
        assertEquals("Unexpected number of participant results",
                            (long) 1,
                            (long) testResult.getParticipantResults().size());

    }

    @Test
    public void testClientRespondingWithErrorResponseStopsTest()
    {
        Response errorResponse = new Response(CLIENT1_REGISTERED_NAME, CommandType.CREATE_CONNECTION, "error occurred");

        configureMockToReturnResponseTo(CreateConnectionCommand.class,
                                        errorResponse, null);

        _testInstance =  createTestInstanceWithConnection();

        _testRunner = new OrdinaryTestRunner(_participatingClients,
                                             _testInstance,
                                             _respondingJmsDelegate,
                                             COMMAND_RESPONSE_TIMEOUT,
                                             TEST_RESULT_TIMEOUT);
        try
        {
            _testRunner.run();
            fail("Exception not thrown");
        }
        catch (DistributedTestException e)
        {
            assertEquals("One or more clients were unable to successfully process commands. "
                                + "1 command(s) generated an error response.", e.getMessage());
        }
    }

    @Test
    public void testClientFailsToSendCommandResponseWithinTimeout()
    {
        configureMockToReturnResponseTo(CreateConnectionCommand.class,
                                        null, null);

        _testInstance =  createTestInstanceWithConnection();

        _testRunner = new OrdinaryTestRunner(_participatingClients,
                                             _testInstance,
                                             _respondingJmsDelegate,
                                             COMMAND_RESPONSE_TIMEOUT,
                                             TEST_RESULT_TIMEOUT);
        try
        {
            _testRunner.run();
            fail("Exception not thrown");
        }
        catch (DistributedTestException e)
        {
            assertEquals(
                    "After 1000ms ... Timed out waiting for command responses ... Expecting 1 more response(s).",
                    e.getMessage());
        }
    }

    private void configureMockToReturnResponseTo(final Class<? extends Command> inReplyTo,
                                                 final Response response,
                                                 final ParticipantResult participantResult)
    {
        // When the JMS gets the command, respond with the response
        doAnswer(new Answer<Void>()
        {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable
            {
                for(CommandListener listener : _spiedCommandListeners)
                {
                    if (response != null && listener.supports(response))
                    {
                        listener.processCommand(response);
                    }
                    if (participantResult != null && listener.supports(participantResult))
                    {
                        listener.processCommand(participantResult);
                    }
                }

                return null;
            }
        }).when(_respondingJmsDelegate).sendCommandToClient(anyString(), isA(inReplyTo));
    }

    private TestInstance createTestInstanceWithConnection()
    {
        TestInstance testInstance = mock(TestInstance.class);

        List<CommandForClient> commands = Arrays.asList(
                new CommandForClient(CLIENT1_CONFIGURED_NAME, new CreateConnectionCommand("conn1", "factory")));

        when(testInstance.createCommands()).thenReturn(commands);

        return testInstance;
    }
}
