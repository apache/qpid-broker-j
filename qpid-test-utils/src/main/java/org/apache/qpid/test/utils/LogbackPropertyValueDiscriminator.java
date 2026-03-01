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
package org.apache.qpid.test.utils;

import java.util.Optional;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.core.sift.AbstractDiscriminator;

/**
 * Logback discriminator that reads a property from the {@link LoggerContextVO} property map.
 * <p>
 * Used with the {@link ch.qos.logback.classic.sift.SiftingAppender SiftingAppender} to route log
 * output into separate files based on the property value set by {@link QpidUnitTestExtension}.
 * <p>
 * This approach is chosen over MDC because MDC values are thread-local and would be
 * lost in background threads, producing incorrect log routing.
 */
public class LogbackPropertyValueDiscriminator extends AbstractDiscriminator<ILoggingEvent>
{
    public static final String CLASS_QUALIFIED_TEST_NAME = "classQualifiedTestName";

    private String _key;
    private String _defaultValue;

    @Override
    public String getDiscriminatingValue(final ILoggingEvent event)
    {
        return Optional.ofNullable(event.getLoggerContextVO())
                .map(LoggerContextVO::getPropertyMap)
                .map(properties -> properties.get(_key))
                .orElse(_defaultValue);
    }

    @Override
    public String getKey()
    {
        return _key;
    }

    public void setKey(String key)
    {
        _key = key;
    }

    public String getDefaultValue()
    {
        return _defaultValue;
    }

    public void setDefaultValue(String defaultValue)
    {
        _defaultValue = defaultValue;
    }
}
