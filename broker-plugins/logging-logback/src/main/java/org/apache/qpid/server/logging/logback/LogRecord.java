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
package org.apache.qpid.server.logging.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;

import org.apache.qpid.server.model.ManagedAttributeValue;
import org.apache.qpid.server.model.ManagedAttributeValueType;

@ManagedAttributeValueType(isAbstract = true)
public class LogRecord implements ManagedAttributeValue
{
    private final ILoggingEvent _event;
    private final long _id;

    public LogRecord(long id, ILoggingEvent event)
    {
        _id = id;
        _event = event;
    }

    public long getId()
    {
        return _id;
    }

    public long getTimestamp()
    {
        return _event.getTimeStamp();
    }

    public String getThreadName()
    {
        return _event.getThreadName();
    }

    public String getLevel()
    {
        return _event.getLevel().toString();
    }

    public String getMessage()
    {
        return _event.getFormattedMessage();
    }

    public String getLogger()
    {
        return _event.getLoggerName();
    }
}
