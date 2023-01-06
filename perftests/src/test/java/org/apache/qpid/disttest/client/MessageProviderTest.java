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
package org.apache.qpid.disttest.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.disttest.client.property.ListPropertyValue;
import org.apache.qpid.disttest.client.property.PropertyValue;
import org.apache.qpid.disttest.client.property.SimplePropertyValue;
import org.apache.qpid.disttest.message.CreateProducerCommand;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class MessageProviderTest extends UnitTestBase
{
    private Session _session;
    private TextMessage _message;

    @BeforeEach
    public void setUp() throws Exception
    {
        _session = mock(Session.class);
        _message = mock(TextMessage.class);
        when(_session.createTextMessage(isA(String.class))).thenReturn(_message);
        when(_session.createTextMessage()).thenReturn(_message);
    }

    @Test
    public void testGetMessagePayload()
    {
        MessageProvider messageProvider = new MessageProvider(null)
        {
            @Override
            public String getMessagePayload(CreateProducerCommand command)
            {
                return super.getMessagePayload(command);
            }
        };
        CreateProducerCommand command = new CreateProducerCommand();
        command.setMessageSize(100);
        String payloadValue = messageProvider.getMessagePayload(command);
        assertNotNull(payloadValue, "Mesage payload should not be null");
        assertEquals(100, (long) payloadValue.length(), "Unexpected payload size");
    }

    @Test
    public void testNextMessage() throws Exception
    {
        MessageProvider messageProvider = new MessageProvider(null);
        CreateProducerCommand command = new CreateProducerCommand();
        command.setMessageSize(100);
        Message message = messageProvider.nextMessage(_session, command);
        assertNotNull(message, "Mesage should be returned");
        verify(_message, atLeastOnce()).setText(isA(String.class));
    }

    @Test
    public void testNextMessageWithProperties() throws Exception
    {
        Map<String, PropertyValue> properties = new HashMap<>();
        properties.put("test1", new SimplePropertyValue("testValue1"));
        properties.put("test2", new SimplePropertyValue(1));
        properties.put("priority", new SimplePropertyValue(2));
        List<PropertyValue> listItems = new ArrayList<>();
        listItems.add(new SimplePropertyValue(2.0));
        ListPropertyValue list = new ListPropertyValue();
        list.setItems(listItems);
        properties.put("test3", list);

        MessageProvider messageProvider = new MessageProvider(properties);
        CreateProducerCommand command = new CreateProducerCommand();
        command.setMessageSize(100);
        Message message = messageProvider.nextMessage(_session, command);
        assertNotNull(message, "Mesage should be returned");
        verify(_message, atLeastOnce()).setText(isA(String.class));
        verify(_message, atLeastOnce()).setJMSPriority(2);
        verify(_message, atLeastOnce()).setStringProperty("test1", "testValue1");
        verify(_message, atLeastOnce()).setIntProperty("test2", 1);
        verify(_message, atLeastOnce()).setDoubleProperty("test3", 2.0);
    }
}
