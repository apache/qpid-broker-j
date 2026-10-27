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

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import ch.qos.logback.classic.net.LoggingEventPreSerializationTransformer;
import ch.qos.logback.classic.net.SocketAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEventVO;
import ch.qos.logback.core.spi.PreSerializationTransformer;

class MdcEnrichingSocketAppender extends SocketAppender
{
    private static final PreSerializationTransformer<ILoggingEvent> EVENT_TRANSFORMER =
            new LoggingEventPreSerializationTransformer()
            {
                @Override
                public Serializable transform(final ILoggingEvent event)
                {
                    return event instanceof MdcLoggingEvent ? LoggingEventVO.build(event) : super.transform(event);
                }
            };

    private final Supplier<Map<String, String>> _mdcSupplier;

    MdcEnrichingSocketAppender(final Supplier<Map<String, String>> mdcSupplier)
    {
        _mdcSupplier = Objects.requireNonNull(mdcSupplier, "MDC supplier must not be null");
    }

    @Override
    protected void append(final ILoggingEvent event)
    {
        if (event == null)
        {
            return;
        }

        super.postProcessEvent(event);
        final Map<String, String> additions = _mdcSupplier.get();
        super.append(additions == null || additions.isEmpty() ? event : new MdcLoggingEvent(event, additions));
    }

    @Override
    public PreSerializationTransformer<ILoggingEvent> getPST()
    {
        return EVENT_TRANSFORMER;
    }
}
