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

import javax.jms.Message;

import org.apache.qpid.disttest.message.CreateProducerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerConfig extends ParticipantConfig
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConfig.class);

    public static final String MESSAGE_SIZE_OVERRIDE_SYSTEM_PROPERTY = "qpid.disttest.messageSize";

    private final int _deliveryMode;
    private final int _messageSize;
    private final int _priority;
    private final long _timeToLive;
    private final long _interval;
    private final String _messageProviderName;

    public ProducerConfig()
    {
        _deliveryMode = Message.DEFAULT_DELIVERY_MODE;
        _messageSize = 1024;
        _priority = Message.DEFAULT_PRIORITY;
        _timeToLive = Message.DEFAULT_TIME_TO_LIVE;
        _interval = 0;
        _messageProviderName = null;
    }

    public ProducerConfig(
            String producerName,
            String destinationName,
            long numberOfMessages,
            int batchSize,
            long maximumDuration,
            int deliveryMode,
            int messageSize,
            int priority,
            long timeToLive,
            long interval,
            String messageProviderName)
    {
        super(producerName, destinationName, false, numberOfMessages, batchSize, maximumDuration);

        _deliveryMode = deliveryMode;
        _messageSize = messageSize;
        _priority = priority;
        _timeToLive = timeToLive;
        _interval = interval;
        _messageProviderName = messageProviderName;
    }

    public CreateProducerCommand createCommand(String sessionName)
    {
        CreateProducerCommand command = new CreateProducerCommand();

        setParticipantProperties(command);

        command.setSessionName(sessionName);
        command.setDeliveryMode(_deliveryMode);

        final Integer overriddenMessageSize = getOverriddenMessageSize();
        Integer messageSize = overriddenMessageSize == null ? _messageSize : overriddenMessageSize ;

        command.setMessageSize(messageSize);
        command.setPriority(_priority);
        command.setTimeToLive(_timeToLive);
        command.setInterval(_interval);
        command.setMessageProviderName(_messageProviderName);

        return command;
    }

    private Integer getOverriddenMessageSize()
    {
        String overriddenMessageSizeString = System.getProperty(MESSAGE_SIZE_OVERRIDE_SYSTEM_PROPERTY);
        Integer overriddenMessageSize = Integer.getInteger(MESSAGE_SIZE_OVERRIDE_SYSTEM_PROPERTY);
        if(overriddenMessageSize != null)
        {
            return overriddenMessageSize;
        }
        else if(overriddenMessageSizeString != null)
        {
            LOGGER.error("Couldn't parse overridden message size as integer :" + overriddenMessageSizeString);
        }
        return null;
    }


}
