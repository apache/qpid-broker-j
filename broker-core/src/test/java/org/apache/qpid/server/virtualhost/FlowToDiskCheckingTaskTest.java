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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.test.utils.UnitTestBase;

public class FlowToDiskCheckingTaskTest extends UnitTestBase
{
    private static final int FLOW_TO_DISK_CHECK_PERIOD = 0;
    private AbstractVirtualHost<?> _virtualHost;
    private Queue _queue;
    private AbstractVirtualHost.FlowToDiskCheckingTask _task;

    @Before
    public void setUp() throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHost.NAME, getTestName());
        attributes.put(VirtualHost.TYPE,  TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        attributes.put(VirtualHost.CONTEXT, Collections.singletonMap(QueueManagingVirtualHost.FLOW_TO_DISK_CHECK_PERIOD,
                                                                     FLOW_TO_DISK_CHECK_PERIOD));
        _virtualHost = (AbstractVirtualHost)BrokerTestHelper.createVirtualHost(attributes, this);
        _task = _virtualHost. new FlowToDiskCheckingTask();
        _queue = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName()));
        _queue.enqueue(InternalMessage.createMessage(_virtualHost.getMessageStore(),
                                                     mock(AMQMessageHeader.class),
                                                     "test",
                                                     true,
                                                     _queue.getName()), null, null);
    }

    @After
    public void tearDown() throws Exception
    {
        if (_queue != null)
        {
            _queue.close();
        }
        if (_virtualHost !=  null)
        {
            _virtualHost.close();
        }
    }

    @Test
    public void testFlowToDiskInMemoryMessage()
    {
        final ServerMessage message = createMessage(10, 20);
        _queue.enqueue(message, null, null);

        makeVirtualHostTargetSizeExceeded();

        _task.execute();
        verify(message.getStoredMessage()).flowToDisk();
    }

    private void makeVirtualHostTargetSizeExceeded()
    {
        if (_virtualHost.getInMemoryMessageSize() == 0)
        {
            _queue.enqueue(InternalMessage.createMessage(_virtualHost.getMessageStore(),
                                                         mock(AMQMessageHeader.class),
                                                         "test",
                                                         true,
                                                         _queue.getName()), null, null);
        }
        _virtualHost.setTargetSize(1L);
        assertTrue(_virtualHost.isOverTargetSize());
    }

    private ServerMessage createMessage(final int headerSize, final int payloadSize)
    {
        long totalSize = headerSize + payloadSize;
        final long id = System.currentTimeMillis();
        final ServerMessage message = mock(ServerMessage.class);
        when(message.getMessageNumber()).thenReturn(id);
        when(message.getMessageHeader()).thenReturn(mock(AMQMessageHeader.class));
        when(message.checkValid()).thenReturn(true);
        when(message.getSizeIncludingHeader()).thenReturn(totalSize);
        when(message.getValidationStatus()).thenReturn(ServerMessage.ValidationStatus.UNKNOWN);

        final StoredMessage storedMessage = mock(StoredMessage.class);
        when(storedMessage.getContentSize()).thenReturn(payloadSize);
        when(storedMessage.getMetadataSize()).thenReturn(headerSize);
        when(storedMessage.getInMemorySize()).thenReturn(totalSize);
        when(message.getStoredMessage()).thenReturn(storedMessage);

        final MessageReference ref = mock(MessageReference.class);
        when(ref.getMessage()).thenReturn(message);

        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);

        return message;
    }
}
