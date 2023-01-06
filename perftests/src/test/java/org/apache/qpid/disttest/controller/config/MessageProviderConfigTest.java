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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.disttest.client.property.PropertyValue;
import org.apache.qpid.disttest.client.property.SimplePropertyValue;
import org.apache.qpid.disttest.message.CreateMessageProviderCommand;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class MessageProviderConfigTest extends UnitTestBase
{
    @Test
    public void testCreateCommandsForMessageProvider()
    {
        Map<String, PropertyValue> messageProperties = new HashMap<>();
        messageProperties.put("test", new SimplePropertyValue("testValue"));
        MessageProviderConfig config = new MessageProviderConfig("test", messageProperties);
        CreateMessageProviderCommand command = config.createCommand();
        assertNotNull(command, "Command should not be null");
        assertNotNull(command.getProviderName(), "Unexpected name");
        assertEquals(messageProperties, command.getMessageProperties(), "Unexpected properties");
    }

    @Test
    public void testMessageProviderConfig()
    {
        Map<String, PropertyValue> messageProperties = new HashMap<>();
        messageProperties.put("test", new SimplePropertyValue("testValue"));
        MessageProviderConfig config = new MessageProviderConfig("test", messageProperties);
        assertEquals("test", config.getName(), "Unexpected name");
        assertEquals(messageProperties, config.getMessageProperties(), "Unexpected properties");
    }
}
