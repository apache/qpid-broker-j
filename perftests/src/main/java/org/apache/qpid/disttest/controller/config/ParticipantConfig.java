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

import org.apache.qpid.disttest.message.CreateParticipantCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ParticipantConfig
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParticipantConfig.class);

    public static final String DURATION_OVERRIDE_SYSTEM_PROPERTY = "qpid.disttest.duration";

    private final String _destinationName;
    private final String _name;
    private final int _batchSize;

    private boolean _isTopic;
    private long _numberOfMessages;
    private long _maximumDuration;

    public ParticipantConfig()
    {
        _name = null;
        _destinationName = null;
        _batchSize = 0;
    }

    public ParticipantConfig(
            String name,
            String destinationName,
            boolean isTopic,
            long numberOfMessages,
            int batchSize,
            long maximumDuration)
    {
        _name = name;
        _destinationName = destinationName;
        _isTopic = isTopic;
        _numberOfMessages = numberOfMessages;
        _batchSize = batchSize;
        _maximumDuration = maximumDuration;
    }

    protected void setParticipantProperties(CreateParticipantCommand createParticipantCommand)
    {
        final Long overriddenMaximumDuration = getOverriddenMaximumDuration();
        Long maximumDuration = overriddenMaximumDuration == null ? _maximumDuration : overriddenMaximumDuration;

        createParticipantCommand.setParticipantName(_name);
        createParticipantCommand.setDestinationName(_destinationName);
        createParticipantCommand.setTopic(_isTopic);
        createParticipantCommand.setNumberOfMessages(_numberOfMessages);
        createParticipantCommand.setBatchSize(_batchSize);
        // only override if the test has a _maximumDuration and the override value is valid
        if (_maximumDuration > 0 && maximumDuration >= 0)
        {
            createParticipantCommand.setMaximumDuration(maximumDuration);
        }
    }


    protected Long getOverriddenMaximumDuration()
    {
        String overriddenDurationStr = System.getProperty(DURATION_OVERRIDE_SYSTEM_PROPERTY);
        Long overriddenDuration = Long.getLong(DURATION_OVERRIDE_SYSTEM_PROPERTY);
        if(overriddenDuration != null)
        {
            return overriddenDuration;
        }
        else if(overriddenDurationStr != null)
        {
            LOGGER.error("Couldn't parse overridden duration as long :" + overriddenDurationStr);
        }
        return null;
    }
}
