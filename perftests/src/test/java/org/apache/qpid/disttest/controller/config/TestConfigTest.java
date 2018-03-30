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
package org.apache.qpid.disttest.controller.config;

import static org.apache.qpid.disttest.controller.config.ConfigTestUtils.assertCommandForClient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.apache.qpid.disttest.controller.CommandForClient;
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

public class TestConfigTest extends UnitTestBase
{
    private static final QueueConfig[] EMPTY_QUEUES_ARRAY = new QueueConfig[0];
    private static final String CLIENT1 = "client1";
    private static final String CLIENT2 = "client2";
    private static final String TEST1 = "test1";

    @Test
    public void testConfigHasZeroArgConstructorForGson()
    {
        TestConfig c = new TestConfig();
        assertNotNull(c);
    }

    @Test
    public void testCreateCommandsForClient()
    {
        TestConfig config = createTestConfigWithClientConfigReturningChildCommands();

        List<CommandForClient> commandsForClients = config.createCommands();
        assertEquals("Unexpected number of commands for client", (long) 3, (long) commandsForClients.size());

        assertCommandForClient(commandsForClients, 0, CLIENT1, NoOpCommand.class);
        assertCommandForClient(commandsForClients, 1, CLIENT1, NoOpCommand.class);
        assertCommandForClient(commandsForClients, 2, CLIENT2, NoOpCommand.class);
    }

    @Test
    public void testGetClientNames()
    {
        TestConfig config = createTestConfigWithTwoClients();

        assertEquals((long) 2, (long) config.getClientNames().size());
    }

    @Test
    public void testGetTotalNumberOfClients()
    {
        TestConfig config = createTestConfigWithTwoClients();
        assertEquals((long) 2, (long) config.getTotalNumberOfClients());
    }

    @Test
    public void testGetTotalNumberOfParticipants()
    {
        TestConfig config = createTestConfigWithTwoClients();
        assertEquals((long) 2, (long) config.getTotalNumberOfParticipants());
    }

    private TestConfig createTestConfigWithClientConfigReturningChildCommands()
    {
        ClientConfig clientConfig1 = createClientConfigReturningCommands(CLIENT1, 2);
        ClientConfig clientConfig2 = createClientConfigReturningCommands(CLIENT2, 1);

        TestConfig config = new TestConfig(TEST1, new ClientConfig[] { clientConfig1, clientConfig2 }, EMPTY_QUEUES_ARRAY);
        return config;
    }

    private ClientConfig createClientConfigReturningCommands(final String clientName, int numberOfCommands)
    {
        ClientConfig clientConfig = mock(ClientConfig.class);

        List<CommandForClient> commandList = new ArrayList<CommandForClient>();

        for (int i = 1 ; i <= numberOfCommands; i++)
        {
            commandList.add(new CommandForClient(clientName, new NoOpCommand()));
        }

        when(clientConfig.createCommands()).thenReturn(commandList);
        return clientConfig;
    }

    private TestConfig createTestConfigWithTwoClients()
    {
        ClientConfig clientConfig1 = mock(ClientConfig.class);
        ClientConfig clientConfig2 = mock(ClientConfig.class);

        when(clientConfig1.getTotalNumberOfParticipants()).thenReturn(1);
        when(clientConfig2.getTotalNumberOfParticipants()).thenReturn(1);

        TestConfig config = new TestConfig(TEST1, new ClientConfig[] { clientConfig1, clientConfig2 }, EMPTY_QUEUES_ARRAY);
        return config;
    }
}
