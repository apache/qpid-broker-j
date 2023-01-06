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

import static org.apache.qpid.disttest.PerfTestConstants.COMMAND_RESPONSE_TIMEOUT;
import static org.apache.qpid.disttest.PerfTestConstants.REGISTRATION_TIMEOUT;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.disttest.ControllerRunner;
import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.Config;
import org.apache.qpid.disttest.jms.ControllerJmsDelegate;
import org.apache.qpid.disttest.message.Command;
import org.apache.qpid.disttest.message.RegisterClientCommand;
import org.apache.qpid.disttest.message.Response;
import org.apache.qpid.disttest.message.StopClientCommand;

import org.mockito.stubbing.Answer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ControllerTest extends UnitTestBase
{
    private static final String CLIENT1_REGISTERED_NAME = "client-uid1";

    private Controller _controller;
    private ControllerJmsDelegate _respondingJmsDelegate;
    private ClientRegistry _clientRegistry;

    @BeforeEach
    public void setUp() throws Exception
    {
        _respondingJmsDelegate = mock(ControllerJmsDelegate.class);
        final Map<String, String> controllerOptions = new HashMap<>();
        controllerOptions.put(ControllerRunner.REGISTRATION_TIMEOUT, String.valueOf(REGISTRATION_TIMEOUT));
        controllerOptions.put(ControllerRunner.COMMAND_RESPONSE_TIMEOUT, String.valueOf(COMMAND_RESPONSE_TIMEOUT));
        _controller = new Controller(_respondingJmsDelegate,
                                     controllerOptions);
        _clientRegistry = mock(ClientRegistry.class);

        Config configWithOneClient = createMockConfig(1);
        _controller.setConfig(configWithOneClient);
        _controller.setClientRegistry(_clientRegistry);

        doAnswer((Answer<Void>) invocation -> 
        {
            final String clientName = (String)invocation.getArguments()[0];
            final Command command = (Command)invocation.getArguments()[1];
            _controller.processStopClientResponse(new Response(clientName, command.getType()));
            return null;
        }).when(_respondingJmsDelegate).sendCommandToClient(anyString(), isA(Command.class));
    }


    @Test
    public void testControllerRejectsEmptyConfiguration()
    {
        Config configWithZeroClients = createMockConfig(0);

        try
        {
            _controller.setConfig(configWithZeroClients);
            fail("Exception not thrown");
        }
        catch (DistributedTestException e)
        {
            // PASS
        }
    }

    @Test
    public void testControllerReceivesTwoExpectedClientRegistrations()
    {
        Config configWithTwoClients = createMockConfig(2);
        _controller.setConfig(configWithTwoClients);
        when(_clientRegistry.awaitClients(2, REGISTRATION_TIMEOUT)).thenReturn(0);

        _controller.awaitClientRegistrations();
    }

    @Test
    public void testControllerDoesntReceiveAnyRegistrations()
    {
        when(_clientRegistry.awaitClients(1, REGISTRATION_TIMEOUT)).thenReturn(1);

        try
        {
            _controller.awaitClientRegistrations();
            fail("Exception not thrown");
        }
        catch (DistributedTestException e)
        {
            // PASS
        }
    }

    @Test
    public void testRegisterClient()
    {
        RegisterClientCommand command = new RegisterClientCommand(CLIENT1_REGISTERED_NAME, "dummy");
        _controller.registerClient(command);

        verify(_clientRegistry).registerClient(CLIENT1_REGISTERED_NAME);
        verify(_respondingJmsDelegate).registerClient(command);

    }

    @Test
    public void testControllerSendsClientStopCommandToClient()
    {
        when(_clientRegistry.getClients()).thenReturn(Collections.singleton(CLIENT1_REGISTERED_NAME));

        _controller.stopAllRegisteredClients();

        verify(_respondingJmsDelegate).sendCommandToClient(eq(CLIENT1_REGISTERED_NAME), isA(StopClientCommand.class));
    }

    private Config createMockConfig(int numberOfClients)
    {
        Config config = mock(Config.class);
        when(config.getTotalNumberOfClients()).thenReturn(numberOfClients);
        return config;
    }

}
