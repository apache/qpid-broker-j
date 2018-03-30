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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;

import org.apache.qpid.disttest.controller.CommandForClient;
import org.apache.qpid.disttest.message.CreateConsumerCommand;
import org.apache.qpid.disttest.message.CreateProducerCommand;
import org.apache.qpid.disttest.message.NoOpCommand;

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

public class TestInstanceTest extends UnitTestBase
{
    private static final String CLIENT_NAME = "CLIENT_NAME";
    private static final int ITERATION_NUMBER = 0;

    private NoOpCommand _noOpCommand;
    private CreateProducerCommand _createProducerCommand;
    private CreateConsumerCommand _createConsumerCommand;

    @Before
    public void setUp() throws Exception
    {
        _noOpCommand = mock(NoOpCommand.class);
        _createProducerCommand = mock(CreateProducerCommand.class);
        when(_createProducerCommand.getMaximumDuration()).thenReturn(1l);

        _createConsumerCommand = mock(CreateConsumerCommand.class);
        when(_createConsumerCommand.getMaximumDuration()).thenReturn(1l);
    }

    @Test
    public void testCreateCommandsWithIterationValues()
    {
        IterationValue iterationValue = mock(IterationValue.class);

        TestConfig config = createTestConfig();

        TestInstance testInstance = new TestInstance(config, ITERATION_NUMBER, iterationValue);

        List<CommandForClient> commandsForClients = testInstance.createCommands();
        assertEquals("Unexpected number of commands for client", (long) 3, (long) commandsForClients.size());

        verify(iterationValue).applyToCommand(_noOpCommand);
        verify(iterationValue).applyToCommand(_createProducerCommand);
        verify(iterationValue).applyToCommand(_createConsumerCommand);
    }

    @Test
    public void testCreateCommandsWithoutIterationValues()
    {
        TestConfig config = createTestConfig();
        TestInstance testInstance = new TestInstance(config);

        List<CommandForClient> commandsForClients = testInstance.createCommands();
        assertEquals("Unexpected number of commands for client", (long) 3, (long) commandsForClients.size());
    }

    @Test
    public void testGetConfiguredClientNames()
    {
        TestConfig testConfig = mock(TestConfig.class);
        when(testConfig.getClientNames()).thenReturn(Collections.singletonList(CLIENT_NAME));
        TestInstance testInstance = new TestInstance(testConfig);

        List<String> clientNames = testInstance.getClientNames();
        assertEquals((long) 1, (long) clientNames.size());
        assertEquals(CLIENT_NAME, clientNames.get(0));
    }

    private TestConfig createTestConfig()
    {
        CommandForClient commandForClient1 = new CommandForClient(CLIENT_NAME, _noOpCommand);
        CommandForClient commandForClient2 = new CommandForClient(CLIENT_NAME, _createProducerCommand);
        CommandForClient commandForClient3 = new CommandForClient(CLIENT_NAME, _createConsumerCommand);

        TestConfig config = mock(TestConfig.class);
        when(config.createCommands()).thenReturn(Arrays.asList(commandForClient1, commandForClient2, commandForClient3));

        return config;
    }

}
