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

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;

import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.websocket.core.Behavior;
import org.eclipse.jetty.websocket.core.FrameHandler;
import org.eclipse.jetty.websocket.core.Negotiated;
import org.eclipse.jetty.websocket.core.OpCode;
import org.eclipse.jetty.websocket.core.OutgoingEntry;
import org.eclipse.jetty.websocket.core.WebSocketComponents;
import org.eclipse.jetty.websocket.core.WebSocketCoreSession;

class DeadlineWebSocketCoreSession extends WebSocketCoreSession
{
    private final Executor _contextExecutor;
    private volatile WebSocketConnection _brokerConnection;

    DeadlineWebSocketCoreSession(final FrameHandler handler,
                                 final Negotiated negotiated,
                                 final WebSocketComponents components,
                                 final Executor contextExecutor)
    {
        super(handler, Behavior.SERVER, negotiated, components);
        _contextExecutor = contextExecutor;
    }

    void setBrokerConnection(final WebSocketConnection connection)
    {
        _brokerConnection = connection;
    }

    @Override
    protected void handle(final Runnable task)
    {
        _contextExecutor.execute(task);
    }

    @Override
    public void sendFrame(final OutgoingEntry entry)
    {
        final WebSocketConnection connection = _brokerConnection;
        if (connection == null || entry.getFrame().getOpCode() != OpCode.CLOSE)
        {
            super.sendFrame(entry);
            return;
        }

        final boolean jettyInitiated = connection.beginJettyClose();
        final OutgoingEntry close = jettyInitiated
                ? new OutgoingEntry.Builder(entry)
                        .callback(Callback.from(entry.getCallback(), connection::completeCloseWrite)).build()
                : entry;
        if (!connection.canSubmitWebSocketClose())
        {
            entry.getCallback().failed(new ClosedChannelException());
            return;
        }

        super.sendFrame(close);
    }
}
