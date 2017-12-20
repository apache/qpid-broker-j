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
package org.apache.qpid.tests.protocol.v0_10;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.qpid.server.protocol.v0_10.FrameSizeObserver;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionTune;
import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolError;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolEvent;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolHeader;
import org.apache.qpid.tests.protocol.HeaderResponse;
import org.apache.qpid.tests.protocol.Response;

public class ProtocolEventReceiver
{
    private final Queue<Response<?>> _events = new ConcurrentLinkedQueue<>();
    private final byte[] _headerBytes;
    private FrameSizeObserver _frameSizeObserver;

    public ProtocolEventReceiver(final byte[] headerBytes,
                                 final FrameSizeObserver frameSizeObserver)
    {
        _headerBytes = headerBytes;
        _frameSizeObserver = frameSizeObserver;
    }

    void received(ProtocolEvent msg)
    {
        if (msg instanceof ProtocolHeader)
        {
            _events.add(new HeaderResponse(_headerBytes));
        }
        else if (msg instanceof Method)
        {
            if (msg instanceof ConnectionTune)
            {
                int maxFrameSize = ((ConnectionTune) msg).getMaxFrameSize();
                _frameSizeObserver.setMaxFrameSize(maxFrameSize);
            }
            _events.add(new PerformativeResponse((Method) msg));
        }
        else if (msg instanceof ProtocolError)
        {
            _events.add(new ErrorResponse((ProtocolError) msg));
        }
    }

    public Collection<Response<?>> getReceivedEvents()
    {
        Collection<Response<?>> results = new ArrayList<>(_events);
        _events.removeAll(results);
        return results;
    }
}
