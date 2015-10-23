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
package org.apache.qpid.server.virtualhost;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.QpidTestCase;

public class AsynchronousMessageStoreRecovererTest extends QpidTestCase
{
    private VirtualHostImpl _virtualHost;
    private MessageStore _store;
    private MessageStore.MessageStoreReader _storeReader;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();

        _virtualHost = mock(VirtualHostImpl.class);
        _store = mock(MessageStore.class);
        _storeReader = mock(MessageStore.MessageStoreReader.class);

        when(_virtualHost.getEventLogger()).thenReturn(new EventLogger());
        when(_virtualHost.getMessageStore()).thenReturn(_store);
        when(_store.newMessageStoreReader()).thenReturn(_storeReader);
    }

    public void testExceptionDuringRecoveryShutsDownBroker() throws Exception
    {
        final CountDownLatch uncaughtExceptionHandlerCalledLatch = new CountDownLatch(1);
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException(final Thread t, final Throwable e)
            {
                uncaughtExceptionHandlerCalledLatch.countDown();
            }
        });
        doThrow(ServerScopedRuntimeException.class).when(_storeReader).visitMessageInstances(any(TransactionLogResource.class),
                                                                                             any(MessageInstanceHandler.class));
        AMQQueue queue = mock(AMQQueue.class);
        when(_virtualHost.getQueues()).thenReturn(Collections.singleton(queue));

        AsynchronousMessageStoreRecoverer recoverer = new AsynchronousMessageStoreRecoverer();
        recoverer.recover(_virtualHost);
        assertTrue("UncaughtExceptionHandler was not called", uncaughtExceptionHandlerCalledLatch.await(1000, TimeUnit.MILLISECONDS));
    }
}
