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
package org.apache.qpid.server.transport;

import static org.apache.qpid.server.transport.NonBlockingConnectionTestHelper.createPort;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionCloseSchedulingTest extends UnitTestBase
{
    private static final int TIMEOUT_MILLIS = 10000;
    private final List<Thread> _threads = new CopyOnWriteArrayList<>();
    private NetworkConnectionScheduler _scheduler;

    @BeforeEach
    void startScheduler()
    {
        _threads.clear();
        _scheduler = new NetworkConnectionScheduler(getTestName(), 1, 2, 1L, task ->
        {
            final Thread thread = new Thread(task, getTestName() + "-" + _threads.size());
            _threads.add(thread);
            return thread;
        });
        _scheduler.start();
    }

    @AfterEach
    void stopScheduler() throws InterruptedException
    {
        _scheduler.close();
        try
        {
            for (final Thread thread : _threads)
            {
                thread.join(TIMEOUT_MILLIS);
                assertFalse(thread.isAlive(), "Scheduler thread did not terminate: " + thread.getName());
            }
        }
        finally
        {
            for (final Thread thread : _threads)
            {
                thread.interrupt();
            }
            for (final Thread thread : _threads)
            {
                thread.join(TIMEOUT_MILLIS);
            }
        }
    }

    @Test
    void deadlineClosesBlockedSocketWithoutPeerActivity() throws Exception
    {
        final AggregateTicker ticker = new AggregateTicker();
        final ProtocolEngine engine = createProtocolEngine(ticker);
        final CompletableFuture<Void> blocked = new CompletableFuture<>();
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        try (final ServerSocketChannel listener = ServerSocketChannel.open();
             final Socket client = new Socket();
             final SocketChannel channel = acceptConnection(listener, client))
        {
            channel.configureBlocking(false);
            channel.socket().setSendBufferSize(4096);
            final NonBlockingConnection connection = createConnection(channel, engine, blocked, closed);
            final AtomicBoolean pendingTaskRan = new AtomicBoolean();
            final List<Runnable> work = List.of(() ->
            {
                try (final QpidByteBuffer output = QpidByteBuffer.wrap(new byte[2 * 1024 * 1024]))
                {
                    connection.send(output);
                }
            }, () -> pendingTaskRan.set(true));
            when(engine.processPendingIterator()).thenReturn(work.iterator());
            _scheduler.addConnection(connection);
            try
            {
                engine.notifyWork();
                blocked.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                ticker.addTicker(new ConnectionClosingTicker(100, connection));
                engine.notifyWork();
                closed.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

                assertFalse(pendingTaskRan.get(), "The socket must close while protocol work is still blocked");
                assertFalse(channel.isOpen());
            }
            finally
            {
                connection.abort();
                closed.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            }
        }
    }

    private SocketChannel acceptConnection(final ServerSocketChannel listener, final Socket client) throws IOException
    {
        listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        client.setReceiveBufferSize(4096);
        client.connect(listener.getLocalAddress(), TIMEOUT_MILLIS);
        return listener.accept();
    }

    private ProtocolEngine createProtocolEngine(final AggregateTicker ticker)
    {
        final ProtocolEngine engine = mock(ProtocolEngine.class);
        final AtomicBoolean hasWork = new AtomicBoolean();
        final AtomicReference<Action<ProtocolEngine>> workListener = new AtomicReference<>();
        when(engine.getAggregateTicker()).thenReturn(ticker);
        when(engine.hasWork()).thenAnswer(invocation -> hasWork.get());
        doAnswer(invocation ->
        {
            hasWork.set(false);
            return null;
        }).when(engine).clearWork();
        doAnswer(invocation ->
        {
            workListener.set(invocation.getArgument(0));
            return null;
        }).when(engine).setWorkListener(any());
        doAnswer(invocation ->
        {
            hasWork.set(true);
            workListener.get().performAction(engine);
            return null;
        }).when(engine).notifyWork();
        return engine;
    }

    private NonBlockingConnection createConnection(final SocketChannel channel,
                                                   final ProtocolEngine engine,
                                                   final CompletableFuture<Void> blocked,
                                                   final CompletableFuture<Void> closed)
    {
        return new NonBlockingConnection(channel, engine, Set.of(TransportEncryption.NONE), () -> { },
                _scheduler, createPort(1024, 1000L))
        {
            @Override
            public boolean doWork()
            {
                final boolean result = super.doWork();
                if (result)
                {
                    closed.complete(null);
                }
                else if (wantsWrite())
                {
                    blocked.complete(null);
                }
                return result;
            }
        };
    }
}
