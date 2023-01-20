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
package org.apache.qpid.server.message;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.test.utils.UnitTestBase;

public class AbstractServerMessageTest extends UnitTestBase
{
    private static class TestMessage<T extends StorableMessageMetaData> extends AbstractServerMessageImpl<TestMessage<T>,T>
    {

        public TestMessage(final StoredMessage<T> handle,
                           final Object connectionReference)
        {
            super(handle, connectionReference);
        }

        @Override
        public String getInitialRoutingAddress()
        {
            return "";
        }

        @Override
        public String getTo()
        {
            return null;
        }

        @Override
        public AMQMessageHeader getMessageHeader()
        {
            return null;
        }

        @Override
        public long getExpiration()
        {
            return 0;
        }

        @Override
        public String getMessageType()
        {
            return "test";
        }

        @Override
        public long getArrivalTime()
        {
            return 0;
        }

        @Override
        public boolean isResourceAcceptable(final TransactionLogResource resource)
        {
            return true;
        }
    }

    private TransactionLogResource createQueue(final String name)
    {
        final TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(randomUUID());
        when(queue.getName()).thenReturn(name);
        return queue;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReferences()
    {
        final TransactionLogResource q1 = createQueue("1");
        final TransactionLogResource q2 = createQueue("2");

        final TestMessage<StorableMessageMetaData> msg = new TestMessage<StorableMessageMetaData>(mock(StoredMessage.class),this);

        assertFalse(msg.isReferenced());
        assertFalse(msg.isReferenced(q1));

        final MessageReference<TestMessage<StorableMessageMetaData>> nonQueueRef = msg.newReference();
        assertFalse(msg.isReferenced());
        assertFalse(msg.isReferenced(q1));

        MessageReference<TestMessage<StorableMessageMetaData>> q1ref = msg.newReference(q1);
        assertTrue(msg.isReferenced());
        assertTrue(msg.isReferenced(q1));
        assertFalse(msg.isReferenced(q2));

        q1ref.release();
        assertFalse(msg.isReferenced());
        assertFalse(msg.isReferenced(q1));

        q1ref = msg.newReference(q1);
        assertTrue(msg.isReferenced());
        assertTrue(msg.isReferenced(q1));
        assertFalse(msg.isReferenced(q2));

        MessageReference<TestMessage<StorableMessageMetaData>> q2ref = msg.newReference(q2);
        assertTrue(msg.isReferenced());
        assertTrue(msg.isReferenced(q1));
        assertTrue(msg.isReferenced(q2));
        assertThrows(MessageAlreadyReferencedException.class, () -> msg.newReference(q1),
                "Should not be able to create a second reference to the same queue");
        q2ref.release();
        assertTrue(msg.isReferenced());
        assertTrue(msg.isReferenced(q1));
        assertFalse(msg.isReferenced(q2));

        q1ref.release();
        assertFalse(msg.isReferenced());
        assertFalse(msg.isReferenced(q1));

        nonQueueRef.release();

        assertThrows(MessageDeletedException.class, () -> msg.newReference(q1),
                "Message should not allow new references as all references had been removed");
    }
}
