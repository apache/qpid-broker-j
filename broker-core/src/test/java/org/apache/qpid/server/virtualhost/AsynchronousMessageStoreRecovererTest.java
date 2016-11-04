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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.QpidTestCase;

public class AsynchronousMessageStoreRecovererTest extends QpidTestCase
{
    private QueueManagingVirtualHost _virtualHost;
    private MessageStore _store;
    private MessageStore.MessageStoreReader _storeReader;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();

        _virtualHost = mock(QueueManagingVirtualHost.class);
        _store = mock(MessageStore.class);
        _storeReader = mock(MessageStore.MessageStoreReader.class);

        when(_virtualHost.getEventLogger()).thenReturn(new EventLogger());
        when(_virtualHost.getMessageStore()).thenReturn(_store);
        when(_store.newMessageStoreReader()).thenReturn(_storeReader);
    }

    public void testExceptionOnRecovery() throws Exception
    {
        ServerScopedRuntimeException exception = new ServerScopedRuntimeException("test");
        doThrow(exception).when(_storeReader).visitMessageInstances(any(TransactionLogResource.class),
                                                                                             any(MessageInstanceHandler.class));
        Queue<?> queue = mock(Queue.class);
        when(_virtualHost.getChildren(eq(Queue.class))).thenReturn(Collections.singleton(queue));

        AsynchronousMessageStoreRecoverer recoverer = new AsynchronousMessageStoreRecoverer();
        ListenableFuture<Void> result = recoverer.recover(_virtualHost);
        try
        {
            result.get();
            fail("ServerScopedRuntimeException should be rethrown");
        }
        catch(ExecutionException e)
        {
            assertEquals("Unexpected cause", exception, e.getCause());
        }
    }

    public void testRecoveryEmptyQueue() throws Exception
    {
        Queue<?> queue = mock(Queue.class);
        when(_virtualHost.getChildren(eq(Queue.class))).thenReturn(Collections.singleton(queue));

        AsynchronousMessageStoreRecoverer recoverer = new AsynchronousMessageStoreRecoverer();
        ListenableFuture<Void> result = recoverer.recover(_virtualHost);
        assertNull(result.get());
    }
}
