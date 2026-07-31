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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionPlainDelegateTest extends UnitTestBase
{
    private NonBlockingConnection _connection;
    private NonBlockingConnectionPlainDelegate _delegate;

    @BeforeEach
    void beforeEach()
    {
        _connection = mock(NonBlockingConnection.class);
        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getNetworkBufferSize()).thenReturn(1024);
        _delegate = new NonBlockingConnectionPlainDelegate(_connection, port);
    }

    @AfterEach
    void afterEach()
    {
        _delegate.shutdownInput();
    }

    @Test
    void emptyCollectionIsComplete() throws Exception
    {
        final NonBlockingConnectionDelegate.WriteResult result = _delegate.doWrite(List.of());

        assertTrue(result.isComplete(), "Empty collection should be complete");
        assertEquals(0, result.getBytesConsumed(), "Empty collection should not consume bytes");
        verify(_connection, never()).writeToTransport(any());
    }

    @Test
    void partialWriteIsIncomplete() throws Exception
    {
        try (final QpidByteBuffer first = QpidByteBuffer.wrap(new byte[] { 1, 2 });
             final QpidByteBuffer second = QpidByteBuffer.wrap(new byte[] { 3 }))
        {
            final List<QpidByteBuffer> buffers = List.of(first, second);
            when(_connection.writeToTransport(buffers)).thenReturn(2L);

            final NonBlockingConnectionDelegate.WriteResult result = _delegate.doWrite(buffers);

            assertFalse(result.isComplete(), "Buffers with remaining data should be incomplete");
            assertEquals(2, result.getBytesConsumed(), "Unexpected consumed byte count");
        }
    }

    @Test
    void fullyConsumedWriteIsComplete() throws Exception
    {
        try (final QpidByteBuffer first = QpidByteBuffer.wrap(new byte[] { 1, 2 });
             final QpidByteBuffer second = QpidByteBuffer.wrap(new byte[] { 3 }))
        {
            final List<QpidByteBuffer> buffers = List.of(first, second);
            doAnswer(invocation ->
            {
                final Collection<QpidByteBuffer> writtenBuffers = invocation.getArgument(0);
                for (final QpidByteBuffer buffer : writtenBuffers)
                {
                    buffer.position(buffer.limit());
                }
                return 3L;
            }).when(_connection).writeToTransport(buffers);

            final NonBlockingConnectionDelegate.WriteResult result = _delegate.doWrite(buffers);

            assertTrue(result.isComplete(), "Fully consumed buffers should be complete");
            assertEquals(3, result.getBytesConsumed(), "Unexpected consumed byte count");
        }
    }
}
