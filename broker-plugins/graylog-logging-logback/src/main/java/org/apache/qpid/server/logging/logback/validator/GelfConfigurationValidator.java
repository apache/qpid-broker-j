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

package org.apache.qpid.server.logging.logback.validator;

import org.apache.qpid.server.logging.logback.GelfAppenderConfiguration;
import org.apache.qpid.server.model.ConfiguredObject;

import java.util.Arrays;
import java.util.Set;

public enum GelfConfigurationValidator
{
    PORT("port")
            {
                @Override
                public void validate(GelfAppenderConfiguration configuration, ConfiguredObject<?> object)
                {
                    Port.validatePort(configuration.getPort(), object, attributeName());
                }
            },
    RECONNECTION_INTERVAL("reconnectionInterval")
            {
                @Override
                public void validate(GelfAppenderConfiguration configuration, ConfiguredObject<?> object)
                {
                    AtLeastZero.validateValue(configuration.getReconnectionInterval(), object, attributeName());
                }
            },
    CONNECTION_TIMEOUT("connectionTimeout")
            {
                @Override
                public void validate(GelfAppenderConfiguration configuration, ConfiguredObject<?> object)
                {
                    AtLeastZero.validateValue(configuration.getConnectionTimeout(), object, attributeName());
                }
            },
    MAXIMUM_RECONNECTION_ATTEMPTS("maximumReconnectionAttempts")
            {
                @Override
                public void validate(GelfAppenderConfiguration configuration, ConfiguredObject<?> object)
                {
                    AtLeastZero.validateValue(configuration.getMaximumReconnectionAttempts(), object, attributeName());
                }
            },
    RETRY_DELAY("retryDelay")
            {
                @Override
                public void validate(GelfAppenderConfiguration configuration, ConfiguredObject<?> object)
                {
                    AtLeastZero.validateValue(configuration.getRetryDelay(), object, attributeName());
                }
            },
    BUFFER_CAPACITY("messageBufferCapacity")
            {
                @Override
                public void validate(GelfAppenderConfiguration configuration, ConfiguredObject<?> object)
                {
                    AtLeastOne.validateValue(configuration.getMessageBufferCapacity(), object, attributeName());
                }
            },
    FLUSH_TIME_OUT("messagesFlushTimeOut")
            {
                @Override
                public void validate(GelfAppenderConfiguration configuration, ConfiguredObject<?> object)
                {
                    AtLeastZero.validateValue(configuration.getMessagesFlushTimeOut(), object, attributeName());
                }
            },
    STATIC_FIELDS("staticFields")
            {
                @Override
                public void validate(GelfAppenderConfiguration configuration, ConfiguredObject<?> object)
                {
                    GelfMessageStaticFields.validateStaticFields(configuration.getStaticFields(), object, attributeName());
                }
            };

    private final String _attributeName;

    public abstract void validate(GelfAppenderConfiguration logger, ConfiguredObject<?> object);

    public static void validateConfiguration(final GelfAppenderConfiguration logger, final ConfiguredObject<?> object)
    {
        Arrays.asList(values()).forEach(validator -> validator.validate(logger, object));
    }

    public static void validateConfiguration(final GelfAppenderConfiguration logger, final ConfiguredObject<?> object, Set<String> changedAttributes)
    {
        Arrays.stream(values()).filter(validator -> changedAttributes.contains(validator.attributeName())).forEach(validator -> validator.validate(logger, object));
    }

    GelfConfigurationValidator(String name)
    {
        _attributeName = name;
    }

    public String attributeName()
    {
        return _attributeName;
    }
}
