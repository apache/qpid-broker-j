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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.core.sift.AbstractDiscriminator;

public class LogbackPropertyValueDiscriminator extends AbstractDiscriminator<ILoggingEvent>
{
    public static final String CLASS_QUALIFIED_TEST_NAME = "classQualifiedTestName";

    private String _key;
    private String _defaultValue;

    @Override
    public String getDiscriminatingValue(final ILoggingEvent event)
    {
        final LoggerContextVO context = event.getLoggerContextVO();
        if (context != null && context.getPropertyMap() != null && context.getPropertyMap().get(_key) != null)
        {
            return context.getPropertyMap().get(_key);
        }
        return _defaultValue;
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
