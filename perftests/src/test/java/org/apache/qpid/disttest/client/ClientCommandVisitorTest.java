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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.qpid.disttest.jms.ClientJmsDelegate;
import org.apache.qpid.disttest.message.CreateConnectionCommand;
import org.apache.qpid.disttest.message.CreateConsumerCommand;
import org.apache.qpid.disttest.message.CreateMessageProviderCommand;
import org.apache.qpid.disttest.message.CreateProducerCommand;
import org.apache.qpid.disttest.message.CreateSessionCommand;
import org.apache.qpid.disttest.message.StartTestCommand;
import org.apache.qpid.disttest.message.StopClientCommand;
import org.apache.qpid.disttest.message.TearDownTestCommand;

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

public class ClientCommandVisitorTest extends UnitTestBase
{
    private Client _client;
    private ClientCommandVisitor _visitor;
    private ClientJmsDelegate _delegate;


    @Before
    public void setUp() throws Exception
    {
        _client = mock(Client.class);
        _delegate = mock(ClientJmsDelegate.class);
        _visitor = new ClientCommandVisitor(_client, _delegate);
    }

    @Test
    public void testStopClient()
    {
        StopClientCommand command = new StopClientCommand();
        _visitor.visit(command);
        verify(_client).stop();
    }

    @Test
    public void testCreateConnection() throws Exception
    {
        final CreateConnectionCommand command = new CreateConnectionCommand();
        _visitor.visit(command);
        verify(_delegate).createConnection(command);
    }

    @Test
    public void testCreateSession() throws Exception
    {
        final CreateSessionCommand command = new CreateSessionCommand();
        _visitor.visit(command);
        verify(_delegate).createSession(command);
    }

    @Test
    public void testCreateProducer() throws Exception
    {
        final CreateProducerCommand command = new CreateProducerCommand();
        _visitor.visit(command);
        verify(_delegate).createProducer(command);
    }

    @Test
    public void testCreateConsumer() throws Exception
    {
        final CreateConsumerCommand command = new CreateConsumerCommand();
        _visitor.visit(command);
        verify(_delegate).createConsumer(command);
    }

    @Test
    public void testStartTest() throws Exception
    {
        final StartTestCommand command = new StartTestCommand();
        _visitor.visit(command);
        verify(_client).startTest();
    }

    @Test
    public void testStopTest() throws Exception
    {
        final TearDownTestCommand stopCommand = new TearDownTestCommand();
        _visitor.visit(stopCommand);
        verify(_client).tearDownTest();
    }

    @Test
    public void testCreateMessageProvider() throws Exception
    {
        final CreateMessageProviderCommand command = new CreateMessageProviderCommand();
        command.setProviderName("test");
        _visitor.visit(command);
        verify(_delegate).createMessageProvider(command);
    }
}
