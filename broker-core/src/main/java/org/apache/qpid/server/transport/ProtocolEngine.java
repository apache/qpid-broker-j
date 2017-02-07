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
package org.apache.qpid.server.transport;

import java.util.Iterator;

import javax.security.auth.Subject;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.transport.network.TransportActivity;

/**
 * A ProtocolEngine is a Receiver for java.nio.ByteBuffers. It takes the data passed to it in the received
 * decodes it and then process the result.
 */
public interface ProtocolEngine extends TransportActivity
{

    // Called by the NetworkDriver when the socket has been closed for reading
    void closed();

    // Called when the NetworkEngine has not written data for the specified period of time (will trigger a
    // heartbeat)
    @Override
    void writerIdle();

    // Called when the NetworkEngine has not read data for the specified period of time (will close the connection)
    @Override
    void readerIdle();

    Subject getSubject();

    boolean isTransportBlockedForWriting();

    void setTransportBlockedForWriting(boolean blocked);

    Iterator<Runnable> processPendingIterator();

    boolean hasWork();

    void clearWork();

    void notifyWork();

    void setWorkListener(Action<ProtocolEngine> listener);

    AggregateTicker getAggregateTicker();

    void encryptedTransport();

    void received(QpidByteBuffer msg);

    void setIOThread(Thread ioThread);
}
