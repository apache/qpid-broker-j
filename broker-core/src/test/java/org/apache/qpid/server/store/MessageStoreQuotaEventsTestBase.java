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
package org.apache.qpid.server.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public abstract class MessageStoreQuotaEventsTestBase extends UnitTestBase implements EventListener, TransactionLogResource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageStoreQuotaEventsTestBase.class);
    protected static final byte[] MESSAGE_DATA = new byte[32 * 1024];

    private MessageStore _store;
    private File _storeLocation;

    private List<Event> _events;
    private UUID _transactionResource;

    protected abstract MessageStore createStore() throws Exception;
    protected abstract int getNumberOfMessagesToFillStore();

    @BeforeEach
    public void setUp() throws Exception
    {
        _storeLocation = new File(new File(TMP_FOLDER), getTestName());
        FileUtils.delete(_storeLocation, true);

        _store = createStore();

        final ConfiguredObject<?> parent = createVirtualHost(_storeLocation.getAbsolutePath());

        _store.openMessageStore(parent);

        _transactionResource = randomUUID();
        _events = new ArrayList<>();
        _store.addEventListener(this, Event.PERSISTENT_MESSAGE_SIZE_OVERFULL, Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);
    }

    protected abstract VirtualHost<?> createVirtualHost(final String storeLocation);

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_store != null)
        {
            _store.closeMessageStore();
        }
        if (_storeLocation != null)
        {
            FileUtils.delete(_storeLocation, true);
        }
    }

    @Test
    public void testOverflow()
    {
        final Transaction transaction = _store.newTransaction();
        final List<EnqueueableMessage<?>> messages = new ArrayList<>();
        for (int i = 0; i < getNumberOfMessagesToFillStore(); i++)
        {
            final EnqueueableMessage<?> m = addMessage(i);
            messages.add(m);
            transaction.enqueueMessage(this, m);
        }
        transaction.commitTran();

        assertEvent(1, Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);

        for (final EnqueueableMessage<?> m : messages)
        {
            m.getStoredMessage().remove();
        }

        assertEvent(2, Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);
    }

    protected EnqueueableMessage<?> addMessage(final long id)
    {
        final StorableMessageMetaData metaData = createMetaData(id, MESSAGE_DATA.length);
        final MessageHandle<?> handle = _store.addMessage(metaData);
        handle.addContent(QpidByteBuffer.wrap(MESSAGE_DATA));
        final StoredMessage<? extends StorableMessageMetaData> storedMessage = handle.allContentAdded();
        return new TestMessage<>(id, storedMessage);
    }

    private StorableMessageMetaData createMetaData(final long id, final int length)
    {
        return new TestMessageMetaData(id, length);
    }

    @Override
    public void event(final Event event)
    {
        LOGGER.debug("Test event listener received event " + event);
        _events.add(event);
    }

    private void assertEvent(final int expectedNumberOfEvents, final Event... expectedEvents)
    {
        assertEquals(expectedNumberOfEvents, _events.size(), "Unexpected number of events received ");
        for (final Event event : expectedEvents)
        {
            assertTrue(_events.contains(event), "Expected event is not found:" + event);
        }
    }

    @Override
    public UUID getId()
    {
        return _transactionResource;
    }

    private static class TestMessage<T extends StorableMessageMetaData> implements EnqueueableMessage<T>
    {
        private final StoredMessage<T> _handle;
        private final long _messageId;

        public TestMessage(final long messageId, final StoredMessage<T> handle)
        {
            _messageId = messageId;
            _handle = handle;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageId;
        }

        @Override
        public boolean isPersistent()
        {
            return true;
        }

        @Override
        public StoredMessage<T> getStoredMessage()
        {
            return _handle;
        }
    }

    @Override
    public MessageDurability getMessageDurability()
    {
        return MessageDurability.DEFAULT;
    }

    @Override
    public String getName()
    {
        return getTestName();
    }
}
