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

import java.util.concurrent.atomic.AtomicLong;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.helpers.CyclicBuffer;

public class RecordEventAppender extends AppenderBase<ILoggingEvent>
{

    private CyclicBuffer<LogRecord> _buffer;
    private final int _size;
    private AtomicLong _recordId;

    RecordEventAppender(final int size)
    {
        _size = size;
        _recordId = new AtomicLong();
    }

    @Override
    public void start()
    {
        _buffer = new CyclicBuffer<>(_size);
        super.start();
    }

    @Override
    public void stop()
    {
        _buffer = null;
        super.stop();
    }

    @Override
    protected void append(ILoggingEvent eventObject)
    {
        if (isStarted())
        {
            _buffer.add(new LogRecord(_recordId.incrementAndGet(), eventObject));
        }
    }

    public CyclicBuffer<LogRecord> getBuffer()
    {
        return _buffer;
    }
}
