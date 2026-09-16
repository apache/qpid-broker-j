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
package org.apache.qpid.server.transport.websocket.connection;

import org.eclipse.jetty.websocket.api.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class WriteCallback implements Callback
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteCallback.class);

    private final WebSocketConnection _connection;
    private final WriteJob _writeJob;
    private final WebSocketConnectionScheduler _scheduler;

    WriteCallback(final WebSocketConnection connection,
                  final WriteJob writeJob,
                  final WebSocketConnectionScheduler scheduler)
    {
        _connection = connection;
        _writeJob = writeJob;
        _scheduler = scheduler;
    }

    @Override
    public void succeed()
    {
        if (_writeJob.completeWrite())
        {
            _connection.doWrite();
        }
    }

    @Override
    public void fail(final Throwable failure)
    {
        reportWriteFailure(failure, _writeJob.failWrite(false));
    }

    void submissionFailed(final Throwable failure)
    {
        reportWriteFailure(failure, _writeJob.failWrite(true));
    }

    private void reportWriteFailure(final Throwable failure, final boolean forceClose)
    {
        if (forceClose)
        {
            LOGGER.debug("WebSocket write failed for connection {}; forcing transport closure",
                    _connection.getRemoteAddress(), failure);
            _scheduler.wakeup();
        }
    }
}
