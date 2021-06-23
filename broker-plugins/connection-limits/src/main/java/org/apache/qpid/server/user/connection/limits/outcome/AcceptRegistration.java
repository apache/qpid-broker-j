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

import java.util.Objects;

import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.user.connection.limits.logging.ConnectionLimitEventLogger;

public final class AcceptRegistration implements ConnectionSlot
{
    private static final String USER_WITH_COUNT_ON_PORT = "User %s with connection count %d on %s port";

    private final String _message;

    private final String _userId;

    private final ConnectionSlot _slot;

    public static AcceptRegistration newInstance(ConnectionSlot slot, String userId, long currentCount, String port)
    {
        return new AcceptRegistration(slot, userId, String.format(USER_WITH_COUNT_ON_PORT, userId, currentCount, port));
    }

    private AcceptRegistration(ConnectionSlot slot, String userId, String message)
    {
        super();
        this._slot = Objects.requireNonNull(slot);
        this._userId = Objects.requireNonNull(userId);
        this._message = Objects.requireNonNull(message);
    }

    public ConnectionSlot logMessage(ConnectionLimitEventLogger logger)
    {
        logger.logAcceptConnection(_userId, getMessage());
        return _slot;
    }


    @Override
    public void free()
    {
        _slot.free();
    }

    protected String getMessage()
    {
        return _message;
    }
}
