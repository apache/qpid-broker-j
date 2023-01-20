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
package org.apache.qpid.disttest.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.qpid.disttest.jms.ClientJmsDelegate;
import org.apache.qpid.disttest.message.Command;
import org.apache.qpid.disttest.message.ParticipantResult;
import org.apache.qpid.disttest.message.Response;
import org.apache.qpid.disttest.message.StopClientCommand;

import org.mockito.InOrder;
import org.mockito.Mockito;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ClientTest extends UnitTestBase
{
    private Client _client;
    private ClientJmsDelegate _delegate;
    private ClientCommandVisitor _visitor;
    private ParticipantExecutor _participantExecutor;
    private ParticipantExecutorRegistry _participantRegistry;
    private Participant _participant;

    @BeforeEach
    public void setUp() throws Exception
    {
        _delegate = mock(ClientJmsDelegate.class);
        _visitor = mock(ClientCommandVisitor.class);
        _client = new Client(_delegate);
        _client.setClientCommandVisitor(_visitor);
        _participantExecutor = mock(ParticipantExecutor.class);
        _participant = mock(Participant.class);
        when(_participantExecutor.getParticipantName()).thenReturn("testParticipantMock");

        _participantRegistry = mock(ParticipantExecutorRegistry.class);
        when(_participantRegistry.executors()).thenReturn(Collections.singletonList(_participantExecutor));
        _client.setParticipantRegistry(_participantRegistry);
    }

    @Test
    public void testInitialState()
    {
        assertEquals(ClientState.CREATED, _client.getState(), "Expected client to be in CREATED state");
    }

    @Test
    public void testStart()
    {
        _client.start();
        final InOrder inOrder = inOrder(_delegate);
        inOrder.verify(_delegate).setInstructionListener(_client);
        inOrder.verify(_delegate).sendRegistrationMessage();
        assertEquals(ClientState.READY, _client.getState(), "Expected client to be in STARTED state");
    }

    @Test
    public void testStopClient()
    {
        _client.stop();

        assertEquals(ClientState.STOPPED, _client.getState(), "Expected client to be in STOPPED state");
    }

    @Test
    public void testProcessInstructionVisitsCommandAndResponds()
    {
        // has to be declared to be of supertype Command otherwise Mockito verify()
        // refers to wrong method
        final Command command = new StopClientCommand();
        _client.processInstruction(command);

        verify(_visitor).visit(command);
        verify(_delegate).sendResponseMessage(isA(Response.class));
    }

    @Test
    public void testWaitUntilStopped()
    {
        stopClientLater(500);
        _client.waitUntilStopped(1000);
        verify(_delegate).destroy();
    }

    @Test
    public void testStartTest()
    {
        _client.start();
        _client.addParticipant(_participant);

        verify(_participantRegistry).add(any(ParticipantExecutor.class));

        _client.startTest();

        InOrder inOrder = Mockito.inOrder(_delegate, _participantExecutor);
        inOrder.verify(_delegate).startConnections();
        inOrder.verify(_participantExecutor).start(eq(_client.getClientName()), any(ResultReporter.class));
    }

    @Test
    public void testTearDownTest()
    {
        // before we can tear down the test the client needs to be in the "running test" state, which requires a participant
        _client.start();
        _client.addParticipant(_participant);
        _client.startTest();

        _client.tearDownTest();

        verify(_delegate).tearDownTest();

        verify(_participantRegistry).clear();
    }

    @Test
    public void testResults()
    {
        ParticipantResult testResult = mock(ParticipantResult.class);
        _client.reportResult(testResult);
        verify(_delegate).sendResponseMessage(testResult);
    }

    private void stopClientLater(long delay)
    {
        doLater(new TimerTask()
        {
            @Override
            public void run()
            {
                _client.stop();
            }

        }, delay);
    }

    private void doLater(TimerTask task, long delayInMillis)
    {
        Timer timer = new Timer();
        timer.schedule(task, delayInMillis);
    }

}
