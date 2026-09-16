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

import org.eclipse.jetty.util.thread.ThreadPool;

import org.apache.qpid.server.transport.MultiVersionProtocolEngine;

final class ProtocolWorkJob implements Runnable
{
    private final WebSocketConnection _connection;
    private final MultiVersionProtocolEngine _protocolEngine;
    private final ThreadPool _threadPool;
    private final WebSocketConnectionScheduler _scheduler;

    ProtocolWorkJob(final WebSocketConnection connection,
                    final MultiVersionProtocolEngine protocolEngine,
                    final ThreadPool threadPool,
                    final WebSocketConnectionScheduler scheduler)
    {
        _connection = connection;
        _protocolEngine = protocolEngine;
        _threadPool = threadPool;
        _scheduler = scheduler;
    }

    void schedule()
    {
        _threadPool.execute(this);
    }

    @Override
    public void run()
    {
        doWork();
    }

    void doWork()
    {
        _connection.lockProtocol();
        try
        {
            _protocolEngine.clearWork();
            try
            {
                _protocolEngine.setIOThread(Thread.currentThread());
                _connection.processPendingWork();
            }
            finally
            {
                _protocolEngine.setIOThread(null);
            }
        }
        finally
        {
            _connection.unlockProtocol();
            _connection.doWrite();
            _scheduler.wakeup();
        }
    }
}
