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
package org.apache.qpid.server.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class UUIDGeneratorTest extends UnitTestBase
{
    private static final String VIRTUAL_HOST_NAME_1 = "virtualHost1";
    private static final String VIRTUAL_HOST_NAME_2 = "virtualHost2";
    private static final String VHOST_ALIAS_1 = "alias1";
    private static final String VHOST_ALIAS_2 = "alias2";
    private static final String QUEUE_NAME_1 = "queue1";
    private static final String QUEUE_NAME_2 = "queue2";
    private static final String EXCHANGE_NAME_1 = "exchange1";
    private static final String EXCHANGE_NAME_2 = "exchange2";
    private static final String BINDING_KEY_1 = "bindingKey1";
    private static final String BINDING_KEY_2 = "bindingKey2";
    private static final String PORT_1 = "port1";
    private static final String PORT_2 = "port2";
    private static final String CONN_REMOTE_ADDR_1 = "localhost:1234";
    private static final String CONN_REMOTE_ADDR_2 = "localhost:5678";
    private static final String CHANNEL_NUMBER_1 = "1";
    private static final String CHANNEL_NUMBER_2 = "2";
    private static final String CONSUMER_NAME_1 = "consumer1";
    private static final String CONSUMER_NAME_2 = "consumer2";
    private static final String PROVIDER_1 = "provider1";
    private static final String PROVIDER_2 = "provider2";
    private static final String USER_1 = "user1";
    private static final String USER_2 = "user2";

    @Test
    public void testDifferentObjectTypeReturnDifferentIdFromSameValues()
    {
        final String value = "name";
        final Set<UUID> idSet = new HashSet<>();

        final UUID id1 = UUIDGenerator.generateQueueUUID(value, value);
        idSet.add(id1);
        final UUID id2 = UUIDGenerator.generateExchangeUUID(value, value);
        idSet.add(id2);
        final UUID id3 = UUIDGenerator.generateBindingUUID(value, value, value, value);
        idSet.add(id3);
        final UUID id4 = UUIDGenerator.generateConsumerUUID(value, value, value, value, value);
        idSet.add(id4);
        final UUID id5 = UUIDGenerator.generateUserUUID(value, value);
        idSet.add(id5);
        final UUID id6 = UUIDGenerator.generateVhostUUID(value);
        idSet.add(id6);
        final UUID id7 = UUIDGenerator.generateVhostAliasUUID(value, value);
        idSet.add(id7);
        final UUID id8 = UUIDGenerator.generateGroupUUID(value, value);
        idSet.add(id8);
        final UUID id9 = UUIDGenerator.generateGroupMemberUUID(value, value, value);
        idSet.add(id9);

        assertEquals(9, (long) idSet.size(), "The produced UUIDs were not all unique");
    }

    @Test
    public void testQueueIdGeneration()
    {
        //check repeated generation is deterministic
        UUID queue1 = UUIDGenerator.generateQueueUUID(QUEUE_NAME_1, VIRTUAL_HOST_NAME_1);
        UUID queue2 = UUIDGenerator.generateQueueUUID(QUEUE_NAME_1, VIRTUAL_HOST_NAME_1);
        assertEquals(queue1, queue2, "Queue IDs should be equal");

        //check different name gives different ID
        queue1 = UUIDGenerator.generateQueueUUID(QUEUE_NAME_1, VIRTUAL_HOST_NAME_1);
        queue2 = UUIDGenerator.generateQueueUUID(QUEUE_NAME_2, VIRTUAL_HOST_NAME_1);
        assertNotEquals(queue1, queue2, "Queue IDs should not be equal");

        //check different vhost name gives different ID
        queue1 = UUIDGenerator.generateQueueUUID(QUEUE_NAME_1, VIRTUAL_HOST_NAME_1);
        queue2 = UUIDGenerator.generateQueueUUID(QUEUE_NAME_1, VIRTUAL_HOST_NAME_2);
        assertNotEquals(queue1, queue2, "Queue IDs should not be equal");
    }

    @Test
    public void testExchangeIdGeneration()
    {
        //check repeated generation is deterministic
        UUID exchange1 = UUIDGenerator.generateExchangeUUID(EXCHANGE_NAME_1, VIRTUAL_HOST_NAME_1);
        UUID exchange2 = UUIDGenerator.generateExchangeUUID(EXCHANGE_NAME_1, VIRTUAL_HOST_NAME_1);
        assertEquals(exchange1, exchange2, "Exchange IDs should be equal");

        //check different name gives different ID
        exchange1 = UUIDGenerator.generateExchangeUUID(EXCHANGE_NAME_1, VIRTUAL_HOST_NAME_1);
        exchange2 = UUIDGenerator.generateExchangeUUID(EXCHANGE_NAME_2, VIRTUAL_HOST_NAME_1);
        assertNotEquals(exchange1, exchange2, "Exchange IDs should not be equal");

        //check different vhost name gives different ID
        exchange1 = UUIDGenerator.generateExchangeUUID(EXCHANGE_NAME_1, VIRTUAL_HOST_NAME_1);
        exchange2 = UUIDGenerator.generateExchangeUUID(EXCHANGE_NAME_1, VIRTUAL_HOST_NAME_2);
        assertNotEquals(exchange1, exchange2, "Exchange IDs should not be equal");
    }

    @Test
    public void testBindingIdGeneration()
    {
        //check repeated generation is deterministic
        UUID binding1 = UUIDGenerator.generateBindingUUID(EXCHANGE_NAME_1, QUEUE_NAME_1, BINDING_KEY_1, VIRTUAL_HOST_NAME_1);
        UUID binding2 = UUIDGenerator.generateBindingUUID(EXCHANGE_NAME_1, QUEUE_NAME_1, BINDING_KEY_1, VIRTUAL_HOST_NAME_1);

        assertEquals(binding1, binding2, "Binding IDs should be equal");

        //check different name gives different ID
        binding1 = UUIDGenerator.generateBindingUUID(EXCHANGE_NAME_1, QUEUE_NAME_1, BINDING_KEY_1, VIRTUAL_HOST_NAME_1);
        binding2 = UUIDGenerator.generateBindingUUID(EXCHANGE_NAME_1, QUEUE_NAME_1, BINDING_KEY_2, VIRTUAL_HOST_NAME_1);
        assertNotEquals(binding1, binding2, "Binding IDs should not be equal");

        //check different vhost name gives different ID
        binding1 = UUIDGenerator.generateBindingUUID(EXCHANGE_NAME_1, QUEUE_NAME_1, BINDING_KEY_1, VIRTUAL_HOST_NAME_1);
        binding2 = UUIDGenerator.generateBindingUUID(EXCHANGE_NAME_1, QUEUE_NAME_1, BINDING_KEY_1, VIRTUAL_HOST_NAME_2);
        assertNotEquals(binding1, binding2, "Binding IDs should not be equal");
    }

    @Test
    public void testVhostIdGeneration()
    {
        //check repeated generation is deterministic
        UUID vhost1 = UUIDGenerator.generateVhostUUID(VIRTUAL_HOST_NAME_1);
        UUID vhost2 = UUIDGenerator.generateVhostUUID(VIRTUAL_HOST_NAME_1);
        assertEquals(vhost1, vhost2, "Virtualhost IDs should be equal");

        //check different vhost name gives different ID
        vhost1 = UUIDGenerator.generateVhostUUID(VIRTUAL_HOST_NAME_1);
        vhost2 = UUIDGenerator.generateVhostUUID(VIRTUAL_HOST_NAME_2);
        assertNotEquals(vhost1, vhost2, "Virtualhost IDs should not be equal");
    }

    @Test
    public void testVhostAliasIdGeneration()
    {
        //check repeated generation is deterministic
        UUID alias1 = UUIDGenerator.generateVhostAliasUUID(VHOST_ALIAS_1, PORT_1);
        UUID alias2 = UUIDGenerator.generateVhostAliasUUID(VHOST_ALIAS_1, PORT_1);
        assertEquals(alias1, alias2, "Virtualhost Alias IDs should be equal");

        //check different port name gives different ID
        alias1 = UUIDGenerator.generateVhostAliasUUID(VHOST_ALIAS_1, PORT_1);
        alias2 = UUIDGenerator.generateVhostAliasUUID(VHOST_ALIAS_2, PORT_1);
        assertNotEquals(alias1, alias2, "Virtualhost Alias IDs should not be equal");

        //check different alias name gives different ID
        alias1 = UUIDGenerator.generateVhostAliasUUID(VHOST_ALIAS_1, PORT_1);
        alias2 = UUIDGenerator.generateVhostAliasUUID(VHOST_ALIAS_1, PORT_2);
        assertNotEquals(alias1, alias2, "Virtualhost Alias IDs should not be equal");
    }

    @Test
    public void testConsumerIdGeneration()
    {
        //check repeated generation is deterministic
        UUID consumer1 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        UUID consumer2 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        assertEquals(consumer1, consumer2, "Consumer IDs should be equal");

        //check different name gives different ID
        consumer1 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        consumer2 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_2);

        assertNotEquals(consumer1, consumer2, "Consumer IDs should not be equal");

        //check different vhost name gives different ID
        consumer1 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        consumer2 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_2, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        assertNotEquals(consumer1, consumer2, "Consumer IDs should not be equal");

        //check different consumer name gives different ID
        consumer1 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        consumer2 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_2, CONSUMER_NAME_1);
        assertNotEquals(consumer1, consumer2, "Consumer IDs should not be equal");

        //check different address name gives different ID
        consumer1 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        consumer2 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_2, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        assertNotEquals(consumer1, consumer2, "Consumer IDs should not be equal");

        //check different queue name gives different ID
        consumer1 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_1, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        consumer2 = UUIDGenerator.generateConsumerUUID(VIRTUAL_HOST_NAME_1, QUEUE_NAME_2, CONN_REMOTE_ADDR_1, CHANNEL_NUMBER_1, CONSUMER_NAME_1);
        assertNotEquals(consumer1, consumer2, "Consumer IDs should not be equal");
    }

    @Test
    public void testUserIdGeneration()
    {
        //check repeated generation is deterministic
        UUID user1 = UUIDGenerator.generateUserUUID(PROVIDER_1, USER_1);
        UUID user2 = UUIDGenerator.generateUserUUID(PROVIDER_1, USER_1);
        assertEquals(user1, user2, "User IDs should be equal");

        //check different name gives different ID
        user1 = UUIDGenerator.generateUserUUID(PROVIDER_1, USER_1);
        user2 = UUIDGenerator.generateUserUUID(PROVIDER_1, USER_2);
        assertNotEquals(user1, user2, "User IDs should not be equal");

        //check different provider gives different ID
        user1 = UUIDGenerator.generateUserUUID(PROVIDER_1, USER_1);
        user2 = UUIDGenerator.generateUserUUID(PROVIDER_2, USER_1);
        assertNotEquals(user1, user2, "User IDs should not be equal");
    }
}
