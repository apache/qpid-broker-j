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
package org.apache.qpid.server.user.connection.limits.outcome;

import java.time.Duration;

import org.apache.qpid.server.security.limit.ConnectionLimitException;
import org.apache.qpid.server.user.connection.limits.logging.ConnectionLimitEventLogger;

public final class RejectRegistration extends ConnectionLimitException
{
    private static final String USER_COUNT_BREAKS_LIMIT_ON_PORT =
            "User %s breaks connection count limit %d on port %s";
    private static final String USER_FREQUENCY_BREAKS_LIMIT_ON_PORT =
            "User %s breaks connection frequency limit %d per %d s on port %s";
    private static final String USER_BLOCKED_ON_PORT = "User %s is blocked on port %s";

    private final String _userId;

    public static RejectRegistration breakingConnectionCount(String userId, int limit, String port)
    {
        return new RejectRegistration(userId,
                String.format(USER_COUNT_BREAKS_LIMIT_ON_PORT, userId, limit, port));
    }

    public static RejectRegistration breakingConnectionFrequency(
            String userId, int limit, Duration frequencyPeriod, String port)
    {
        return new RejectRegistration(userId, String.format(
                USER_FREQUENCY_BREAKS_LIMIT_ON_PORT, userId, limit, frequencyPeriod.getSeconds(), port));
    }

    public static RejectRegistration blockedUser(String userId, String port)
    {
        return new RejectRegistration(userId,
                String.format(USER_BLOCKED_ON_PORT, userId, port));
    }

    public String logMessage(ConnectionLimitEventLogger logger)
    {
        final String message = getMessage();
        logger.logRejectConnection(_userId, message);
        return message;
    }

    private RejectRegistration(String userId, String message)
    {
        super(message);
        this._userId = userId;
    }
}
