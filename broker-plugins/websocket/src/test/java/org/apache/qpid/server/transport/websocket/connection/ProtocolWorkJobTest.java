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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.transport.MultiVersionProtocolEngine;

class ProtocolWorkJobTest extends WebSocketTestBase
{
    @Test
    void protocolWorkRunsAfterWebSocketCloseRequested()
    {
        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        final Runnable work = mock(Runnable.class);
        when(protocolEngine.processPendingIterator()).thenReturn(List.of(work).iterator());
        final WebSocketConnection connection = createConnection(protocolEngine);
        connection.close();

        connection.doWork();

        verify(work).run();
    }

    @Test
    void protocolWorkContinuesWhenWebSocketCloseIsRequestedDuringIteration()
    {
        final MultiVersionProtocolEngine protocolEngine = mock(MultiVersionProtocolEngine.class);
        final Runnable remainingWork = mock(Runnable.class);
        final WebSocketConnection connection = createConnection(protocolEngine);
        when(protocolEngine.processPendingIterator()).thenReturn(List.of(connection::close, remainingWork).iterator());

        connection.doWork();

        verify(remainingWork).run();
    }
}
