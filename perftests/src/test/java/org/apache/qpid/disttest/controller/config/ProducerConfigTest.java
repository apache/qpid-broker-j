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

import javax.jms.DeliveryMode;
import javax.jms.Message;

import org.junit.Assert;

import org.apache.qpid.disttest.message.CreateProducerCommand;

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

public class ProducerConfigTest extends UnitTestBase
{
    @Test
    public void testProducerHasZeroArgConstructorForGson()
    {
        ProducerConfig p = new ProducerConfig();
        assertNotNull(p);
    }

    @Test
    public void testConfigProvidesJmsDefaults()
    {
        CreateProducerCommand p = new ProducerConfig().createCommand("session1");
        assertEquals((long) Message.DEFAULT_DELIVERY_MODE, (long) p.getDeliveryMode());
        assertEquals((long) Message.DEFAULT_PRIORITY, (long) p.getPriority());
        assertEquals(Message.DEFAULT_TIME_TO_LIVE, p.getTimeToLive());
    }

    @Test
    public void testCreateProducerCommandAppliesDurationOverride()
    {
        long overriddenDuration = 123;
        setTestSystemProperty(ParticipantConfig.DURATION_OVERRIDE_SYSTEM_PROPERTY, String.valueOf(overriddenDuration));
        ProducerConfig producerConfig = new ProducerConfig("", "", 0, 0, 1, 0, 0, 0, 0, 0, "");

        CreateProducerCommand command = producerConfig.createCommand("name");

        assertEquals((long) 123, command.getMaximumDuration());
    }

    @Test
    public void testMessageSizeDefault()
    {
        CreateProducerCommand producer = new ProducerConfig().createCommand("session1");
        assertEquals("Unexpected default message size", (long) 1024, (long) producer.getMessageSize());
    }

    @Test
    public void testMessageSizeDefaultOverride()
    {
        final long overriddenMessageSize = 4096;
        setTestSystemProperty(ProducerConfig.MESSAGE_SIZE_OVERRIDE_SYSTEM_PROPERTY, String.valueOf(overriddenMessageSize));

        CreateProducerCommand producer2 = new ProducerConfig().createCommand("session1");
        assertEquals("Unexpected message size", overriddenMessageSize, (long) producer2.getMessageSize());
    }

    @Test
    public void testCreateProducerCommand()
    {
        String destination = "url:/destination";
        int messageSize = 1000;
        int numberOfMessages = 10;
        int priority = 4;
        long timeToLive = 10000;
        int batchSize = 5;
        long interval = 60;
        long maximumDuration = 70;
        long startDelay = 80;
        String providerName = "testProvider1";

        ProducerConfig producerConfig = new ProducerConfig(
                "producer1",
                destination,
                numberOfMessages,
                batchSize,
                maximumDuration,
                DeliveryMode.NON_PERSISTENT,
                messageSize,
                priority,
                timeToLive,
                interval,
                providerName);

        CreateProducerCommand command = producerConfig.createCommand("session1");

        assertEquals("session1", command.getSessionName());
        assertEquals("producer1", command.getParticipantName());
        assertEquals(destination, command.getDestinationName());
        assertEquals((long) numberOfMessages, command.getNumberOfMessages());
        assertEquals((long) batchSize, (long) command.getBatchSize());
        assertEquals(maximumDuration, command.getMaximumDuration());

        assertEquals((long) DeliveryMode.NON_PERSISTENT, (long) command.getDeliveryMode());
        assertEquals((long) messageSize, (long) command.getMessageSize());
        assertEquals((long) priority, (long) command.getPriority());
        assertEquals(timeToLive, command.getTimeToLive());
        assertEquals(interval, command.getInterval());
        assertEquals(providerName, command.getMessageProviderName());
    }
}
