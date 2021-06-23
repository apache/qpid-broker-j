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
package org.apache.qpid.server.user.connection.limits.logging;

import java.util.Objects;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.messages.ResourceLimitMessages;

public class ConnectionLimitEventLogger
{
    static final String OPENING = "Opening";
    static final String CONNECTION = "connection";
    static final String MESSAGE = "Limiter '%s': %s";

    final EventLoggerProvider _logger;

    final String _sourceName;

    public ConnectionLimitEventLogger(String name, EventLoggerProvider logger)
    {
        super();
        _sourceName = Objects.requireNonNull(name);
        _logger = Objects.requireNonNull(logger);
    }

    public void logRejectConnection(String userId, String reason)
    {
        _logger.getEventLogger().message(ResourceLimitMessages.REJECTED(
                OPENING, CONNECTION, userId, String.format(MESSAGE, _sourceName, reason)));
    }

    public void logAcceptConnection(String userId, String message)
    {
        // Do nothing
    }
}
