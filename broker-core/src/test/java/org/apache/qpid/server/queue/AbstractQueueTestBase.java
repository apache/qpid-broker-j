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

package org.apache.qpid.server.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.TestConsumerTarget;
import org.apache.qpid.server.exchange.DirectExchange;
import org.apache.qpid.server.exchange.DirectExchangeImpl;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.QueueNotificationListener;
import org.apache.qpid.server.queue.AbstractQueue.QueueEntryFilter;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.MessageDestinationIsAlternateException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.UnknownAlternateBindingException;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
abstract class AbstractQueueTestBase extends UnitTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQueueTestBase.class);

    private final String _qname = "qname";
    private final String _owner = "owner";
    private final String _routingKey = "routing key";

    private Queue<?> _queue;
    private QueueManagingVirtualHost<?> _virtualHost;
    private DirectExchangeImpl _exchange;
    private TestConsumerTarget _consumerTarget;  // TODO replace with minimally configured mockito mock
    private QueueConsumer<?,?> _consumer;
    private Map<String,Object> _arguments = Map.of();

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _virtualHost = BrokerTestHelper.createVirtualHost(getTestClassName(), this);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);
        _queue = (AbstractQueue<?>) _virtualHost.createChild(Queue.class, attributes);
        _exchange = (DirectExchangeImpl) _virtualHost.getChildByName(Exchange.class, ExchangeDefaults.DIRECT_EXCHANGE_NAME);
        _consumerTarget = new TestConsumerTarget();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_queue != null)
        {
            _queue.close();
        }
    }

    @Test
    public void testCreateQueue()
    {
        _queue.close();

        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> _queue =  _virtualHost.createChild(Queue.class, Map.copyOf(_arguments)),
                "Expected exception not thrown");
        assertTrue(thrown.getMessage().contains("name"), "Exception was not about missing name");

        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, "differentName");
        _queue = _virtualHost.createChild(Queue.class, attributes);
        assertNotNull(_queue, "Queue was not created");
    }

    @Test
    public void testGetVirtualHost()
    {
        assertEquals(_virtualHost, _queue.getVirtualHost(), "Virtual host was wrong");
    }

    @Test
    public void testBinding() throws Exception
    {
        _exchange.addBinding(_routingKey, _queue, Map.of());

        assertTrue(_exchange.isBound(_routingKey), "Routing key was not bound");
        assertTrue(_exchange.isBound(_routingKey, _queue), "Queue was not bound to key");
        assertEquals(1, (long) _queue.getPublishingLinks().size(), "Exchange binding count");
        final Binding firstBinding = (Binding) _queue.getPublishingLinks().iterator().next();
        assertEquals(_routingKey, firstBinding.getBindingKey(), "Wrong binding key");

        _exchange.deleteBinding(_routingKey, _queue);
        assertFalse(_exchange.isBound(_routingKey), "Routing key was still bound");
    }

    @Test
    public void testRegisterConsumerThenEnqueueMessage() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);

        // Check adding a consumer adds it to the queue
        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);
        assertEquals(1, (long) _queue.getConsumerCount(), "Queue does not have consumer");
        assertEquals(1, (long) _queue.getConsumerCountWithCredit(), "Queue does not have active consumer");

        // Check sending a message ends up with the subscriber
        _queue.enqueue(messageA, null, null);
        while(_consumerTarget.processPending());

        assertEquals(messageA, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull(_consumer.getQueueContext().getReleasedEntry());

        // Check removing the consumer removes it's information from the queue
        _consumer.close();
        assertTrue(_consumerTarget.isClosed(), "Consumer still had queue");
        assertNotEquals(1, _queue.getConsumerCount(), "Queue still has consumer");
        assertNotEquals(1, _queue.getConsumerCountWithCredit(), "Queue still has active consumer");

        final ServerMessage<?> messageB = createMessage(25L);
        _queue.enqueue(messageB, null, null);
        assertNull(_consumer.getQueueContext());
    }

    @Test
    public void testEnqueueMessageThenRegisterConsumer() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);
        _queue.enqueue(messageA, null, null);
        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());
        assertEquals(messageA, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull(_consumer.getQueueContext().getReleasedEntry(), "There should be no releasedEntry after an enqueue");
    }

    /**
     * Tests enqueuing two messages.
     */
    @Test
    public void testEnqueueTwoMessagesThenRegisterConsumer() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);
        final ServerMessage<?> messageB = createMessage(25L);
        _queue.enqueue(messageA, null, null);
        _queue.enqueue(messageB, null, null);
        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());
        assertEquals(messageB, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull(_consumer.getQueueContext().getReleasedEntry(),
                "There should be no releasedEntry after enqueues");
    }

    @Test
    public void testMessageHeldIfNotYetValidWhenConsumerAdded() throws Exception
    {
        _queue.close();
        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);
        attributes.put(Queue.HOLD_ON_PUBLISH_ENABLED, Boolean.TRUE);

        _queue = _virtualHost.createChild(Queue.class, attributes);

        final ServerMessage<?> messageA = createMessage(24L);
        final AMQMessageHeader messageHeader = messageA.getMessageHeader();
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()+20000L);
        _queue.enqueue(messageA, null, null);
        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());

        assertEquals(0, (long) _consumerTarget.getMessages().size(),
                "Message which was not yet valid was received");
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()-100L);
        _queue.checkMessageStatus();
        while(_consumerTarget.processPending());
        assertEquals(1, (long) _consumerTarget.getMessages().size(),
                "Message which was valid was not received");
    }

    @Test
    public void testMessageHoldingDependentOnQueueProperty() throws Exception
    {
        _queue.close();
        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);
        attributes.put(Queue.HOLD_ON_PUBLISH_ENABLED, Boolean.FALSE);

        _queue = _virtualHost.createChild(Queue.class, attributes);

        final ServerMessage<?> messageA = createMessage(24L);
        final AMQMessageHeader messageHeader = messageA.getMessageHeader();
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()+20000L);
        _queue.enqueue(messageA, null, null);
        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());

        assertEquals(1, (long) _consumerTarget.getMessages().size(),
                "Message was held despite queue not having holding enabled");
    }

    @Test
    public void testUnheldMessageOvertakesHeld() throws Exception
    {
        _queue.close();
        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);
        attributes.put(Queue.HOLD_ON_PUBLISH_ENABLED, Boolean.TRUE);

        _queue = _virtualHost.createChild(Queue.class, attributes);

        final ServerMessage<?> messageA = createMessage(24L);
        final AMQMessageHeader messageHeader = messageA.getMessageHeader();
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()+20000L);
        _queue.enqueue(messageA, null, null);
        final ServerMessage<?> messageB = createMessage(25L);
        _queue.enqueue(messageB, null, null);

        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());

        assertEquals(1, (long) _consumerTarget.getMessages().size(),
                "Expect one message (message B)");
        assertEquals(messageB.getMessageHeader().getMessageId(), _consumerTarget.getMessages().get(0).getMessage()
                .getMessageHeader().getMessageId(), "Wrong message received");

        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()-100L);
        _queue.checkMessageStatus();
        while(_consumerTarget.processPending());
        assertEquals(2, (long) _consumerTarget.getMessages().size(),
                "Message which was valid was not received");
        assertEquals(messageA.getMessageHeader().getMessageId(), _consumerTarget.getMessages().get(1).getMessage()
                .getMessageHeader().getMessageId(), "Wrong message received");
    }

    /**
     * Tests that a released queue entry is resent to the subscriber.  Verifies also that the
     * QueueContext._releasedEntry is reset to null after the entry has been reset.
     */
    @Test
    public void testReleasedMessageIsResentToSubscriber() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);
        final ServerMessage<?> messageB = createMessage(25L);
        final ServerMessage<?> messageC = createMessage(26L);

        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<>();
        final EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue three messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);
        _queue.enqueue(messageC, postEnqueueAction, null);

        while(_consumerTarget.processPending());

        assertEquals(3, (long) _consumerTarget.getMessages().size(),
                "Unexpected total number of messages sent to consumer");
        assertFalse(queueEntries.get(0).isRedelivered(), "Redelivery flag should not be set");
        assertFalse(queueEntries.get(1).isRedelivered(), "Redelivery flag should not be set");
        assertFalse(queueEntries.get(2).isRedelivered(), "Redelivery flag should not be set");

        /* Now release the first message only, causing it to be requeued */

        queueEntries.get(0).release();

        while(_consumerTarget.processPending());

        assertEquals(4, (long) _consumerTarget.getMessages().size(),
                "Unexpected total number of messages sent to consumer");
        assertTrue(queueEntries.get(0).isRedelivered(), "Redelivery flag should now be set");
        assertFalse(queueEntries.get(1).isRedelivered(), "Redelivery flag should remain be unset");
        assertFalse(queueEntries.get(2).isRedelivered(), "Redelivery flag should remain be unset");
        assertNull(_consumer.getQueueContext().getReleasedEntry(),
                "releasedEntry should be cleared after requeue processed");
    }

    /**
     * Tests that a released message that becomes expired is not resent to the subscriber.
     * This tests ensures that SimpleAMQQueue<?>Entry.getNextAvailableEntry avoids expired entries.
     * Verifies also that the QueueContext._releasedEntry is reset to null after the entry has been reset.
     */
    @Test
    public void testReleaseMessageThatBecomesExpiredIsNotRedelivered() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);
        final CountDownLatch sendIndicator = new CountDownLatch(1);
        _consumerTarget = new TestConsumerTarget()
        {

            @Override
            public void notifyWork()
            {
                while(processPending());
            }

            @Override
            public void send(final MessageInstanceConsumer consumer, final MessageInstance entry, final boolean batch)
            {
                try
                {
                    super.send(consumer, entry, batch);
                }
                finally
                {
                    sendIndicator.countDown();
                }
            }
        };

        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.SEES_REQUEUES, ConsumerOption.ACQUIRES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<>();
        final EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue one message with expiration set for a short time in the future */

        final long expiration = System.currentTimeMillis() + 100L;
        when(messageA.getExpiration()).thenReturn(expiration);

        _queue.enqueue(messageA, postEnqueueAction, null);

        assertTrue(sendIndicator.await(5000, TimeUnit.MILLISECONDS),
                "Message was not sent during expected time interval");

        assertEquals(1, (long) _consumerTarget.getMessages().size(),
                "Unexpected total number of messages sent to consumer");
        final QueueEntry queueEntry = queueEntries.get(0);

        final CountDownLatch dequeueIndicator = new CountDownLatch(1);
        queueEntry.addStateChangeListener((object, oldState, newState) ->
        {
            if (newState.equals(MessageInstance.DEQUEUED_STATE))
            {
                dequeueIndicator.countDown();
            }
        });
        assertFalse(queueEntry.isRedelivered(), "Redelivery flag should not be set");

        /* Wait a little more to be sure that message will have expired, then release the first message only, causing it to be requeued */
        while(!queueEntry.expired() && System.currentTimeMillis() <= expiration )
        {
            Thread.sleep(10);
        }

        assertTrue(queueEntry.expired(), "Expecting the queue entry to be now expired");
        queueEntry.release();

        assertTrue(dequeueIndicator.await(5000, TimeUnit.MILLISECONDS),
                "Message was not de-queued due to expiration");

        assertEquals(1, (long) _consumerTarget.getMessages().size(),
                "Total number of messages sent should not have changed");
        assertFalse(queueEntry.isRedelivered(), "Redelivery flag should not be set");

        // QueueContext#_releasedEntry is updated after notification, thus, we need to make sure that it is updated
        long waitLoopLimit = 10;
        while (_consumer.getQueueContext().getReleasedEntry() != null && waitLoopLimit-- > 0 )
        {
            Thread.sleep(10);
        }
        assertNull(_consumer.getQueueContext().getReleasedEntry(),
                "releasedEntry should be cleared after requeue processed:" +
                _consumer.getQueueContext().getReleasedEntry());
    }

    /**
     * Tests that if a client releases entries 'out of order' (the order
     * used by QueueEntryImpl.compareTo) that messages are still resent
     * successfully.  Specifically this test ensures the {@see AbstractQueue#requeue()}
     * can correctly move the _releasedEntry to an earlier position in the QueueEntry list.
     */
    @Test
    public void testReleasedOutOfComparableOrderAreRedelivered() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);
        final ServerMessage<?> messageB = createMessage(25L);
        final ServerMessage<?> messageC = createMessage(26L);

        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<>();
        final EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue three messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);
        _queue.enqueue(messageC, postEnqueueAction, null);

        while(_consumerTarget.processPending());

        assertEquals(3, (long) _consumerTarget.getMessages().size(),
                "Unexpected total number of messages sent to consumer");
        assertFalse(queueEntries.get(0).isRedelivered(), "Redelivery flag should not be set");
        assertFalse(queueEntries.get(1).isRedelivered(), "Redelivery flag should not be set");
        assertFalse(queueEntries.get(2).isRedelivered(), "Redelivery flag should not be set");

        /* Now release the third and first message only, causing it to be requeued */

        queueEntries.get(2).release();
        queueEntries.get(0).release();

        while(_consumerTarget.processPending());

        assertEquals(5, (long) _consumerTarget.getMessages().size(),
                "Unexpected total number of messages sent to consumer");
        assertTrue(queueEntries.get(0).isRedelivered(), "Redelivery flag should now be set");
        assertFalse(queueEntries.get(1).isRedelivered(), "Redelivery flag should remain be unset");
        assertTrue(queueEntries.get(2).isRedelivered(), "Redelivery flag should now be set");
        assertNull(_consumer.getQueueContext().getReleasedEntry(),
                "releasedEntry should be cleared after requeue processed");
    }

    /**
     * Tests that a release requeues an entry for a queue with multiple consumers.  Verifies that a
     * requeue resends a message to a <i>single</i> subscriber.
     */
    @Test
    public void testReleaseForQueueWithMultipleConsumers() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);
        final ServerMessage<?> messageB = createMessage(25L);

        final TestConsumerTarget target1 = new TestConsumerTarget();
        final TestConsumerTarget target2 = new TestConsumerTarget();

        final QueueConsumer<?, ?> consumer1 = (QueueConsumer<?, ?>) _queue
                .addConsumer(target1, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);

        final QueueConsumer<?, ?> consumer2 = (QueueConsumer<?, ?>) _queue
                .addConsumer(target2, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);
        final ArrayList<QueueEntry> queueEntries = new ArrayList<>();
        final EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue two messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);

        while(target1.processPending());
        while(target2.processPending());

        assertEquals(2, (long) (target1.getMessages().size() + target2.getMessages().size()),
                "Unexpected total number of messages sent to both after enqueue");

        /* Now release the first message only, causing it to be requeued */
        queueEntries.get(0).release();

        while(target1.processPending());
        while(target2.processPending());

        assertEquals(3, (long) (target1.getMessages().size() + target2.getMessages().size()),
                "Unexpected total number of messages sent to both consumers after release");
        assertNull(consumer1.getQueueContext().getReleasedEntry(),
                "releasedEntry should be cleared after requeue processed");
        assertNull(consumer2.getQueueContext().getReleasedEntry(),
                "releasedEntry should be cleared after requeue processed");
    }

    @Test
    public void testExclusivePolicy() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);
        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.EXCLUSIVE, ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);

        _queue.setAttributes(Map.of(Queue.EXCLUSIVE, ExclusivityPolicy.CONNECTION));
        assertEquals("mock connection", _queue.getOwner());

        _queue.setAttributes(Map.of(Queue.EXCLUSIVE, ExclusivityPolicy.SESSION));
        assertEquals("mock session", _queue.getOwner());

        _queue.setAttributes(Map.of(Queue.EXCLUSIVE, ExclusivityPolicy.PRINCIPAL));
        assertEquals("mock principal", _queue.getOwner());

        _queue.setAttributes(Map.of(Queue.EXCLUSIVE, ExclusivityPolicy.CONTAINER));
        assertEquals("mock container", _queue.getOwner());

        _queue.setAttributes(Map.of(Queue.EXCLUSIVE, ExclusivityPolicy.LINK));
        assertNull(_queue.getOwner());

        _queue.setAttributes(Map.of(Queue.EXCLUSIVE, ExclusivityPolicy.SHARED_SUBSCRIPTION));
        assertNull(_queue.getOwner());

        _queue.setAttributes(Map.of(Queue.EXCLUSIVE, ExclusivityPolicy.NONE));
        assertNull(_queue.getOwner());

        _consumer.close();
    }

    @Test
    public void testExclusiveConsumer() throws Exception
    {
        final ServerMessage<?> messageA = createMessage(24L);
        // Check adding an exclusive consumer adds it to the queue

        _consumer = (QueueConsumer<?,?>) _queue
                .addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.EXCLUSIVE, ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);

        assertEquals(1, (long) _queue.getConsumerCount(), "Queue does not have consumer");
        assertEquals(1, (long) _queue.getConsumerCountWithCredit(), "Queue does not have active consumer");

        // Check sending a message ends up with the subscriber
        _queue.enqueue(messageA, null, null);

        while(_consumerTarget.processPending());

        assertEquals(messageA, _consumer.getQueueContext().getLastSeenEntry().getMessage(),
                "Queue context did not see expected message");

        // Check we cannot add a second subscriber to the queue
        final TestConsumerTarget subB = new TestConsumerTarget();
        assertThrows(MessageSource.ExistingExclusiveConsumer.class,
                () -> _queue.addConsumer(subB, null, messageA.getClass(), "test",
                        EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0),
                "Exception not thrown");

        // Check we cannot add an exclusive subscriber to a queue with an
        // existing consumer
        _consumer.close();
        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);

        assertThrows(MessageSource.ExistingConsumerPreventsExclusive.class,
                () -> _consumer = (QueueConsumer<?,?>) _queue.addConsumer(subB, null, messageA.getClass(), "test",
                        EnumSet.of(ConsumerOption.EXCLUSIVE), 0),
                "Exception not thrown");
    }

    /**
     * Tests that dequeued message is not present in the list returned form
     * {@link AbstractQueue#getMessagesOnTheQueue()}
     */
    @Test
    public void testGetMessagesOnTheQueueWithDequeuedEntry()
    {
        final int messageNumber = 4;
        final int dequeueMessageIndex = 1;

        // send test messages into a test queue
        enqueueGivenNumberOfMessages(_queue, messageNumber);

        // dequeue message
        dequeueMessage(_queue, dequeueMessageIndex);

        // get messages on the queue
        final List<? extends QueueEntry> entries = _queue.getMessagesOnTheQueue();

        // assert queue entries
        assertEquals(messageNumber - 1, (long) entries.size());
        int expectedId = 0;
        for (int i = 0; i < messageNumber - 1; i++)
        {
            final Long id = (entries.get(i).getMessage()).getMessageNumber();
            if (i == dequeueMessageIndex)
            {
                assertNotEquals(Long.valueOf(expectedId), id, "Message with id " + dequeueMessageIndex +
                        " was dequeued and should not be returned by method getMessagesOnTheQueue!");
                expectedId++;
            }
            assertEquals(Long.valueOf(expectedId), id, "Expected message with id " + expectedId +
                    " but got message with id " + id);
            expectedId++;
        }
    }

    /**
     * Tests that dequeued message is not present in the list returned form
     * {@link AbstractQueue#getMessagesOnTheQueue(QueueEntryFilter)}
     */
    @Test
    public void testGetMessagesOnTheQueueByQueueEntryFilterWithDequeuedEntry()
    {
        final int messageNumber = 4;
        final int dequeueMessageIndex = 1;

        // send test messages into a test queue
        enqueueGivenNumberOfMessages(_queue, messageNumber);

        // dequeue message
        dequeueMessage(_queue, dequeueMessageIndex);

        // get messages on the queue with filter accepting all available messages
        final List<? extends QueueEntry> entries = ((AbstractQueue<?>)_queue).getMessagesOnTheQueue(new QueueEntryFilter()
        {
            @Override
            public boolean accept(QueueEntry entry)
            {
                return true;
            }

            @Override
            public boolean filterComplete()
            {
                return false;
            }
        });

        // assert entries on the queue
        assertEquals(messageNumber - 1, (long) entries.size());
        int expectedId = 0;
        for (int i = 0; i < messageNumber - 1; i++)
        {
            final Long id = (entries.get(i).getMessage()).getMessageNumber();
            if (i == dequeueMessageIndex)
            {
                assertNotEquals(Long.valueOf(expectedId), id, "Message with id " + dequeueMessageIndex +
                        " was dequeued and should not be returned by method getMessagesOnTheQueue!");
                expectedId++;
            }
            assertEquals(Long.valueOf(expectedId), id, "Expected message with id " + expectedId +
                    " but got message with id " + id);
            expectedId++;
        }
    }

    /**
     * Tests that all messages including dequeued one are deleted from the queue
     * on invocation of {@link AbstractQueue#clearQueue()}
     */
    @Test
    public void testClearQueueWithDequeuedEntry()
    {
        final int messageNumber = 4;
        final int dequeueMessageIndex = 1;

        // put messages into a test queue
        enqueueGivenNumberOfMessages(_queue, messageNumber);

        // dequeue message on a test queue
        dequeueMessage(_queue, dequeueMessageIndex);

        // clean queue
        _queue.clearQueue();

        // get queue entries
        final List<? extends QueueEntry> entries = _queue.getMessagesOnTheQueue();

        // assert queue entries
        assertNotNull(entries);
        assertEquals(0, (long) entries.size());
    }

    @Test
    public void testNotificationFiredOnEnqueue()
    {
        final QueueNotificationListener listener = mock(QueueNotificationListener .class);

        _queue.setNotificationListener(listener);
        _queue.setAttributes(Map.of(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES,2));

        _queue.enqueue(createMessage(24L), null, null);
        verifyNoInteractions(listener);

        _queue.enqueue(createMessage(25L), null, null);

        verify(listener, atLeastOnce()).notifyClients(eq(NotificationCheck.MESSAGE_COUNT_ALERT), eq(_queue), contains("Maximum count on queue threshold"));
    }

    @Test
    public void testNotificationFiredAsync()
    {
        final QueueNotificationListener  listener = mock(QueueNotificationListener .class);

        _queue.enqueue(createMessage(24L), null, null);
        _queue.enqueue(createMessage(25L), null, null);
        _queue.enqueue(createMessage(26L), null, null);

        _queue.setNotificationListener(listener);
        _queue.setAttributes(Map.of(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 2));

        verifyNoInteractions(listener);

        _queue.checkMessageStatus();

        verify(listener, atLeastOnce()).notifyClients(eq(NotificationCheck.MESSAGE_COUNT_ALERT), eq(_queue), contains("Maximum count on queue threshold"));
    }

    @Test
    public void testMaximumMessageTtl()
    {
        // Test scenarios where only the maximum TTL has been set
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME,"testTtlOverrideMaximumTTl");
        attributes.put(Queue.MAXIMUM_MESSAGE_TTL, 10000L);

        Queue<?> queue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals(60000L, getExpirationOnQueue(queue, 50000L, 0L),
                "TTL has not been overridden");

        assertEquals(60000L, getExpirationOnQueue(queue, 50000L, 65000L),
                "TTL has not been overridden");

        assertEquals(55000L, getExpirationOnQueue(queue, 50000L, 55000L),
                "TTL has been incorrectly overridden");

        final long tooLateExpiration = System.currentTimeMillis() + 20000L;

        assertTrue(tooLateExpiration != getExpirationOnQueue(queue, 0L, tooLateExpiration),
                "TTL has not been overridden");

        long acceptableExpiration = System.currentTimeMillis() + 5000L;

        assertEquals(acceptableExpiration, getExpirationOnQueue(queue, 0L, acceptableExpiration),
                "TTL has been incorrectly overriden");

        // Test the scenarios where only the minimum TTL has been set

        attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME,"testTtlOverrideMinimumTTl");
        attributes.put(Queue.MINIMUM_MESSAGE_TTL, 10000L);

        queue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals(0L, getExpirationOnQueue(queue, 50000L, 0L),
                "TTL has been overridden incorrectly");

        assertEquals(65000L, getExpirationOnQueue(queue, 50000L, 65000L),
                "TTL has been overridden incorrectly");

        assertEquals(60000L, getExpirationOnQueue(queue, 50000L, 55000L),
                "TTL has not been overriden");

        final long unacceptableExpiration = System.currentTimeMillis() + 5000L;

        assertTrue(unacceptableExpiration != getExpirationOnQueue(queue, 0L, tooLateExpiration),
                "TTL has not been overridden");

        acceptableExpiration = System.currentTimeMillis() + 20000L;

        assertEquals(acceptableExpiration, getExpirationOnQueue(queue, 0L, acceptableExpiration),
                "TTL has been incorrectly overridden");

        // Test the scenarios where both the minimum and maximum TTL have been set

        attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME,"testTtlOverrideBothTTl");
        attributes.put(Queue.MINIMUM_MESSAGE_TTL, 10000L);
        attributes.put(Queue.MAXIMUM_MESSAGE_TTL, 20000L);

        queue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals(70000L, getExpirationOnQueue(queue, 50000L, 0L),
                "TTL has not been overridden");

        assertEquals(65000L, getExpirationOnQueue(queue, 50000L, 65000L),
                "TTL has been overridden incorrectly");

        assertEquals(60000L, getExpirationOnQueue(queue, 50000L, 55000L),
                "TTL has not been overridden");
    }

    @Test
    public void testOldestMessage()
    {
        final Queue<?> queue = getQueue();
        queue.enqueue(createMessage(1L, (byte)1, Map.of("sortKey", "Z"), 10L), null, null);
        queue.enqueue(createMessage(2L, (byte)4, Map.of("sortKey", "M"), 100L), null, null);
        queue.enqueue(createMessage(3L, (byte)9, Map.of("sortKey", "A"), 1000L), null, null);

        assertEquals(10L, queue.getOldestMessageArrivalTime());
    }

    @Test
    public void testNoneOverflowPolicy()
    {
        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 2);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 100);

        final Queue<?> queue = getQueue();
        queue.setAttributes(attributes);

        ServerMessage<?> message = createMessage(24L, 50, 50);
        when(message.getArrivalTime()).thenReturn(10L);
        queue.enqueue(message, null, null);
        message = createMessage(25L, 50, 50);
        when(message.getArrivalTime()).thenReturn(50L);
        queue.enqueue(message, null, null);
        message = createMessage(26L, 50, 50);
        when(message.getArrivalTime()).thenReturn(200L);
        queue.enqueue(message, null, null);

        assertEquals(3, (long) queue.getQueueDepthMessages(), "Wrong number of messages in queue");
        assertEquals(300, queue.getQueueDepthBytes(), "Wrong size of messages in queue");
        assertEquals(10L, ((AbstractQueue<?>) queue).getEntries().getOldestEntry().getMessage().getArrivalTime(),
                "Wrong oldest message");
    }

    @Test
    public void testRingOverflowPolicyMaxCount()
    {
        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 4);

        final Queue<?> queue = getQueue();
        queue.setAttributes(attributes);

        ServerMessage<?> message = createMessage(24L, 10, 10);
        when(message.getArrivalTime()).thenReturn(10L);
        queue.enqueue(message, null, null);
        message = createMessage(25L, 10, 10);
        when(message.getArrivalTime()).thenReturn(50L);
        queue.enqueue(message, null, null);
        message = createMessage(26L, 10, 10);
        when(message.getArrivalTime()).thenReturn(200L);
        queue.enqueue(message, null, null);
        message = createMessage(27L, 10, 10);
        when(message.getArrivalTime()).thenReturn(500L);
        queue.enqueue(message, null, null);
        message = createMessage(28L, 10, 10);
        when(message.getArrivalTime()).thenReturn(1000L);
        queue.enqueue(message, null, null);

        assertEquals(4, (long) queue.getQueueDepthMessages(), "Wrong number of messages in queue");
        assertEquals(80, queue.getQueueDepthBytes(), "Wrong size of messages in queue");
        assertEquals(50L, ((AbstractQueue<?>) queue).getEntries().getOldestEntry().getMessage().getArrivalTime(),
                "Wrong oldest message");
    }

    @Test
    public void testRingOverflowPolicyMaxSize()
    {
        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 4);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 100);

        final Queue<?> queue = getQueue();
        queue.setAttributes(attributes);

        ServerMessage<?> message = createMessage(24L, 10, 10);
        when(message.getArrivalTime()).thenReturn(10L);
        queue.enqueue(message, null, null);
        message = createMessage(25L, 10, 10);
        when(message.getArrivalTime()).thenReturn(50L);
        queue.enqueue(message, null, null);
        message = createMessage(26L, 20, 10);
        when(message.getArrivalTime()).thenReturn(200L);
        queue.enqueue(message, null, null);
        message = createMessage(27L, 20, 10);
        when(message.getArrivalTime()).thenReturn(200L);
        queue.enqueue(message, null, null);

        assertEquals(4, (long) queue.getQueueDepthMessages(), "Wrong number of messages in queue");
        assertEquals(100, queue.getQueueDepthBytes(), "Wrong size of messages in queue");

        message = createMessage(27L, 20, 10);
        when(message.getArrivalTime()).thenReturn(500L);
        queue.enqueue(message, null, null);

        assertEquals(3, (long) queue.getQueueDepthMessages(), "Wrong number of messages in queue");
        assertEquals(90, queue.getQueueDepthBytes(), "Wrong size of messages in queue");
        assertEquals(200L, ((AbstractQueue<?>) queue).getEntries().getOldestEntry().getMessage().getArrivalTime(),
                "Wrong oldest message");
    }

    @Test
    public void testRingOverflowPolicyMessagesRejected()
    {
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 0);

        final Queue<?> queue = getQueue();
        queue.setAttributes(attributes);

        ServerMessage<?> message;
        RoutingResult<?> result;

        message = createMessage(27L, 20, 10);
        result = queue.route(message, message.getInitialRoutingAddress(), null);
        assertTrue(result.isRejected(), "Result should include not accepting route");

        final int headerSize = 20;
        final int payloadSize = 10;
        final int id = 28;

        attributes = new HashMap<>(_arguments);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 10);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 10);
        queue.setAttributes(attributes);

        message = createMessage((long) id, headerSize, payloadSize);
        result = queue.route(message, message.getInitialRoutingAddress(), null);
        assertTrue(result.isRejected(), "Result should include not accepting route");
    }

    @Test
    public void testAlternateBindingValidationRejectsNonExistingDestination()
    {
        final String alternateBinding = "nonExisting";
        final Map<String, Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.ALTERNATE_BINDING, Map.of(AlternateBinding.DESTINATION, alternateBinding));
        final UnknownAlternateBindingException thrown = assertThrows(UnknownAlternateBindingException.class,
                () -> _virtualHost.createChild(Queue.class, attributes),
                "Expected exception is not thrown");
        assertEquals(alternateBinding, thrown.getAlternateBindingName(),
                "Unexpected exception alternate binding");
    }

    @Test
    public void testAlternateBindingValidationRejectsSelf()
    {
        final Map<String, String> alternateBinding = Map.of(AlternateBinding.DESTINATION, _qname);
        final Map<String, Object> newAttributes = Map.of(Queue.ALTERNATE_BINDING, alternateBinding);
        assertThrows(IllegalConfigurationException.class,
                () -> _queue.setAttributes(newAttributes),
                "Expected exception is not thrown");
    }

    @Test
    public void testDurableQueueRejectsNonDurableAlternateBinding()
    {
        final String dlqName = getTestName() + "_DLQ";
        final Map<String, Object> dlqAttributes = new HashMap<>(_arguments);
        dlqAttributes.put(Queue.NAME, dlqName);
        dlqAttributes.put(Queue.DURABLE, false);

        _virtualHost.createChild(Queue.class, dlqAttributes);

        final Map<String, Object> queueAttributes = new HashMap<>(_arguments);
        queueAttributes.put(Queue.NAME, getTestName());
        queueAttributes.put(Queue.ALTERNATE_BINDING, Map.of(AlternateBinding.DESTINATION, dlqName));
        queueAttributes.put(Queue.DURABLE, true);

        assertThrows(IllegalConfigurationException.class,
                () -> _virtualHost.createChild(Queue.class, queueAttributes),
                "Expected exception is not thrown");
    }

    @Test
    public void testAlternateBinding()
    {
        final Map<String, Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.ALTERNATE_BINDING, Map.of(AlternateBinding.DESTINATION, _qname));
        final Queue<?> newQueue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals(_qname, newQueue.getAlternateBinding().getDestination(), "Unexpected alternate binding");
    }

    @Test
    public void testDeleteOfQueueSetAsAlternate()
    {
        final Map<String, Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.ALTERNATE_BINDING, Map.of(AlternateBinding.DESTINATION, _qname));
        final Queue<?> newQueue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals(_qname, newQueue.getAlternateBinding().getDestination(), "Unexpected alternate binding");
        assertThrows(MessageDestinationIsAlternateException.class, () -> _queue.delete(),
                "Expected exception is not thrown");
        assertFalse(_queue.isDeleted());
    }

    @Test
    public void testMoveMessages()
    {
        doMoveOrCopyMessageTest(true);
    }

    @Test
    public void testCopyMessages()
    {
        doMoveOrCopyMessageTest(false);
    }

    @Test
    public void testExpiryPolicyRouteToAlternate()
    {
        final Map<String, Object> dlqAttributes = new HashMap<>(_arguments);
        dlqAttributes.put(Queue.NAME, getTestName() + "_dlq");
        dlqAttributes.put(Queue.MINIMUM_MESSAGE_TTL, Long.MAX_VALUE);
        final Queue<?> dlq = _virtualHost.createChild(Queue.class, dlqAttributes);

        final Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.ALTERNATE_BINDING, Map.of("destination", dlq.getName()));
        attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);

        final Queue<?> queue = _virtualHost.createChild(Queue.class, attributes);

        final ServerMessage<?> message = createMessage(1L);
        final long arrivalTime = 50000L;
        when(message.getArrivalTime()).thenReturn(arrivalTime);
        when(message.getExpiration()).thenReturn(arrivalTime + 5000L);
        when(message.isResourceAcceptable(any())).thenReturn(true);
        queue.enqueue(message,null, null);

        assertEquals(1, queue.getQueueDepthMessages(), "Unexpected queue depth");

        queue.checkMessageStatus();

        assertEquals(0, queue.getQueueDepthMessages(),
                "Unexpected queue depth after checking message status");
        assertEquals(1, dlq.getQueueDepthMessages(), "Unexpected DLQ depth");
    }

    private void doMoveOrCopyMessageTest(final boolean move)
    {
        final Queue<?> target = _virtualHost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_target"));

        _queue.enqueue(createMessage(1L), null, null);
        _queue.enqueue(createMessage(2L), null, null);
        _queue.enqueue(createMessage(3L), null, null);

        assertEquals(3, (long) _queue.getQueueDepthMessages(),
                "Unexpected number of messages on source queue");
        assertEquals(0, (long) target.getQueueDepthMessages(),
                "Unexpected number of messages on target queue before test");

        if (move)
        {
            _queue.moveMessages(target, null, "true = true", -1);
        }
        else
        {
            _queue.copyMessages(target, null, "true = true", -1);

        }

        final long expected = move ? 0 : 3;
        assertEquals(expected, _queue.getQueueDepthMessages(),
                "Unexpected number of messages on source queue after test");
        assertEquals(3, (long) target.getQueueDepthMessages(),
                "Unexpected number of messages on target queue after test");
    }

    @Test
    public void testCopyMessageRespectsQueueSizeLimits()
    {
        final Map<String, Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName() + "_target");
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 2);

        final Queue<?> target = _virtualHost.createChild(Queue.class, attributes);

        _queue.enqueue(createMessage(1L), null, null);
        _queue.enqueue(createMessage(2L), null, null);
        _queue.enqueue(createMessage(3L), null, null);

        assertEquals(3, (long) _queue.getQueueDepthMessages(),
                "Unexpected number of messages on source queue");
        assertEquals(0, (long) target.getQueueDepthMessages(),
                "Unexpected number of messages on target queue before test");

        _queue.copyMessages(target, null, "true = true", -1);

        assertEquals(3, (long) _queue.getQueueDepthMessages(),
                "Unexpected number of messages on source queue after test");
        assertEquals(2, (long) target.getQueueDepthMessages(),
                "Unexpected number of messages on target queue after test");
    }

    @Test
    public void testEnqueuedMessageFlowedToDisk()
    {
        makeVirtualHostTargetSizeExceeded();

        final ServerMessage<?> message2 = createMessage(1L, 2, 3);
        final long sizeIncludingHeader = message2.getSizeIncludingHeader();
        final StoredMessage<?> storedMessage = message2.getStoredMessage();
        when(storedMessage.getInMemorySize()).thenReturn(sizeIncludingHeader);

        _queue.enqueue(message2, null, null);

        verify(storedMessage).getInMemorySize();
        verify(storedMessage).flowToDisk();

        assertEquals(2, _queue.getQueueDepthMessages(), "Unexpected number of messages on the queue");
    }

    @Test
    public void testEnqueuedMalformedMessageDeleted()
    {
        makeVirtualHostTargetSizeExceeded();

        final ServerMessage<?> message2 = createMessage(1L, 2, 3);
        final long sizeIncludingHeader = message2.getSizeIncludingHeader();
        final StoredMessage<?> storedMessage = message2.getStoredMessage();
        when(storedMessage.getInMemorySize()).thenReturn(sizeIncludingHeader);
        when(message2.checkValid()).thenReturn(false);

        _queue.enqueue(message2, null, null);

        verify(storedMessage).getInMemorySize();
        verify(storedMessage, never()).flowToDisk();

        assertEquals(1, _queue.getQueueDepthMessages(), "Unexpected number of messages on the queue");
    }

    @Test
    public void testVisit()
    {
        final ServerMessage<?> message = createMessage(1L, 2, 3);
        _queue.enqueue(message, null, null);

        final QueueEntryVisitor visitor = mock(QueueEntryVisitor.class);
        _queue.visit(visitor);

        final ArgumentCaptor<QueueEntry> argument = ArgumentCaptor.forClass(QueueEntry.class);
        verify(visitor).visit(argument.capture());

        final QueueEntry queueEntry = argument.getValue();
        assertEquals(message, queueEntry.getMessage());
        verify(message.newReference()).release();
    }

    @Test
    public void testVisitWhenNodeDeletedAfterAdvance()
    {
        final QueueEntryList list = mock(QueueEntryList.class);

        final Map<String,Object> attributes = Map.of(
                Queue.NAME, _qname,
                Queue.OWNER, _owner);

        final Queue<?> queue = new AbstractQueue(attributes, _virtualHost)
        {
            @Override
            QueueEntryList getEntries()
            {
                return list;
            }
        };

        final MessageReference<?> reference = mock(MessageReference.class);
        final QueueEntry entry = mock(QueueEntry.class);
        when(entry.isDeleted()).thenReturn(true);
        when(entry.newMessageReference()).thenReturn(reference);
        final QueueEntryIterator iterator = mock(QueueEntryIterator.class);
        when(iterator.advance()).thenReturn(true, false);
        when(iterator.getNode()).thenReturn(entry);
        when(list.iterator()).thenReturn(iterator);

        final QueueEntryVisitor visitor = mock(QueueEntryVisitor.class);
        queue.visit(visitor);
        verifyNoMoreInteractions(visitor);
        verify(reference).release();
    }
    @Test
    public void testDeleteEntryNotPersistent() throws Exception
    {
        deleteEntry(1L, null);
    }

    @Test
    public void testDeleteEntryPersistent() throws Exception
    {
        long messageNumber = 1L;
        final MessageEnqueueRecord record = mock(MessageEnqueueRecord.class);
        when(record.getMessageNumber()).thenReturn(messageNumber);
        when(record.getQueueId()).thenReturn(_queue.getId());

        deleteEntry(messageNumber, record);
    }

    private void deleteEntry(final long messageNumber, final MessageEnqueueRecord record) throws InterruptedException
    {
        final CountDownLatch messageDeleteDetector = new CountDownLatch(1);
        final ServerMessage<?> message = createMessage(messageNumber, 2, 3);
        final MessageReference<?> reference = message.newReference();
        doAnswer((Answer<Void>) invocationOnMock ->
        {
            messageDeleteDetector.countDown();
            return null;
        }).when(reference).release();

        _queue.enqueue(message, null, record);

        _queue.visit(entry ->
        {
            _queue.deleteEntry(entry);
            return false;
        });

        assertTrue(messageDeleteDetector.await(2000L, TimeUnit.MILLISECONDS),
                "Message reference is not released withing given timeout interval");
    }


    private void makeVirtualHostTargetSizeExceeded()
    {
        final InternalMessage message = InternalMessage.createMessage(_virtualHost.getMessageStore(),
                                                                       mock(AMQMessageHeader.class),
                                                                       "test",
                                                                       true,
                                                                       _qname);
        _queue.enqueue(message, null, null);
        assertEquals(1, _queue.getQueueDepthMessages(), "Unexpected number of messages on the queue");
        _virtualHost.setTargetSize(1L);
        assertTrue(_virtualHost.isOverTargetSize());
    }

    private long getExpirationOnQueue(final Queue<?> queue, long arrivalTime, long expiration)
    {
        final List<QueueEntry> entries = new ArrayList<>();

        final ServerMessage<?> message = createMessage(1L);
        when(message.getArrivalTime()).thenReturn(arrivalTime);
        when(message.getExpiration()).thenReturn(expiration);
        queue.enqueue(message,null, null);
        queue.visit(entry ->
        {
            entries.add(entry);
            return true;
        });
        assertEquals(1, (long) entries.size(), "Expected only one entry in the queue");

        final Long entryExpiration =
                (Long) entries.get(0).getInstanceProperties().getProperty(InstanceProperties.Property.EXPIRATION);

        queue.clearQueue();
        entries.clear();
        return entryExpiration;
    }

    /**
     * A helper method to put given number of messages into queue
     * <p>
     * All messages are asserted that they are present on queue
     *
     * @param queue
     *            queue to put messages into
     * @param messageNumber
     *            number of messages to put into queue
     */
    protected List<? extends QueueEntry> enqueueGivenNumberOfMessages(Queue<?> queue, int messageNumber)
    {
        putGivenNumberOfMessages(queue, messageNumber);

        // make sure that all enqueued messages are on the queue
        List<? extends QueueEntry> entries = queue.getMessagesOnTheQueue();
        assertEquals(messageNumber, (long) entries.size());
        for (int i = 0; i < messageNumber; i++)
        {
            assertEquals(i, (entries.get(i).getMessage()).getMessageNumber());
        }
        return entries;
    }

    /**
     * A helper method to put given number of messages into queue
     * <p>
     * Queue is not checked if messages are added into queue
     *
     * @param queue
     *            queue to put messages into
     * @param messageNumber
     *            number of messages to put into queue
     */
    protected void putGivenNumberOfMessages(final Queue<?> queue, final int messageNumber)
    {
        for (int i = 0; i < messageNumber; i++)
        {
            // Create message
            final ServerMessage<?> message = createMessage((long)i);

            // Put message on queue
            queue.enqueue(message,null, null);
        }
    }

    /**
     * A helper method to dequeue an entry on queue with given index
     *
     * @param queue
     *            queue to dequeue message on
     * @param dequeueMessageIndex
     *            entry index to dequeue.
     */
    protected QueueEntry dequeueMessage(final Queue<?> queue, final int dequeueMessageIndex)
    {
        final List<? extends QueueEntry> entries = queue.getMessagesOnTheQueue();
        final QueueEntry entry = entries.get(dequeueMessageIndex);
        entry.acquire();
        entry.delete();
        assertTrue(entry.isDeleted());
        return entry;
    }

    protected void verifyReceivedMessages(final List<MessageInstance> expected,
                                          final List<MessageInstance> delivered)
    {
        assertEquals(expected.size(), (long) delivered.size(),
                "Consumer did not receive the expected number of messages");

        for (final MessageInstance msg : expected)
        {
            assertTrue(delivered.contains(msg), "Consumer did not receive msg: " +
                    msg.getMessage().getMessageNumber());
        }
    }

    public Queue<?> getQueue()
    {
        return _queue;
    }

    protected void setQueue(final Queue<?> queue)
    {
        _queue = queue;
    }

    public TestConsumerTarget getConsumer()
    {
        return _consumerTarget;
    }

    public Map<String,Object> getArguments()
    {
        return _arguments;
    }

    public void setArguments(final Map<String,Object> arguments)
    {
        _arguments = arguments;
    }

    protected ServerMessage<?> createMessage(final Long id,
                                             final byte priority,
                                             final Map<String,Object> arguments,
                                             final long arrivalTime)
    {
        final ServerMessage<?> message = createMessage(id);
        final AMQMessageHeader hdr = message.getMessageHeader();
        when(hdr.getPriority()).thenReturn(priority);
        when(message.getArrivalTime()).thenReturn(arrivalTime);
        when(hdr.getHeaderNames()).thenReturn(arguments.keySet());
        final ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
        when(hdr.containsHeader(nameCaptor.capture())).thenAnswer((Answer<Boolean>) invocationOnMock ->
                arguments.containsKey(nameCaptor.getValue()));

        final ArgumentCaptor<Set<String>> namesCaptor = ArgumentCaptor.forClass(Set.class);
        when(hdr.containsHeaders(namesCaptor.capture())).thenAnswer((Answer<Boolean>) invocationOnMock ->
                arguments.keySet().containsAll(namesCaptor.getValue()));

        final ArgumentCaptor<String> nameCaptor2 = ArgumentCaptor.forClass(String.class);
        when(hdr.getHeader(nameCaptor2.capture())).thenAnswer(invocationOnMock ->
                arguments.get(nameCaptor2.getValue()));

        return message;
    }

    protected ServerMessage<?> createMessage(final Long id, final int headerSize, final int payloadSize)
    {
        final ServerMessage<?> message = createMessage(id);
        when(message.getSizeIncludingHeader()).thenReturn(Long.valueOf(headerSize + payloadSize));
        return message;
    }

    protected ServerMessage<?> createMessage(final Long id)
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(String.valueOf(id));
        final ServerMessage<?> message = mock(ServerMessage.class);
        when(message.getMessageNumber()).thenReturn(id);
        when(message.getMessageHeader()).thenReturn(header);
        when(message.checkValid()).thenReturn(true);

        final StoredMessage storedMessage = mock(StoredMessage.class);
        when(message.getStoredMessage()).thenReturn(storedMessage);

        final MessageReference ref = mock(MessageReference.class);
        when(ref.getMessage()).thenReturn(message);

        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);

        return message;
    }

    private static class EntryListAddingAction implements Action<MessageInstance>
    {
        private final ArrayList<QueueEntry> _queueEntries;

        public EntryListAddingAction(final ArrayList<QueueEntry> queueEntries)
        {
            _queueEntries = queueEntries;
        }

        @Override
        public void performAction(final MessageInstance entry)
        {
            _queueEntries.add((QueueEntry) entry);
        }
    }

    public QueueManagingVirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }

    public String getQname()
    {
        return _qname;
    }

    public String getOwner()
    {
        return _owner;
    }

    public String getRoutingKey()
    {
        return _routingKey;
    }

    public DirectExchange<?> getExchange()
    {
        return _exchange;
    }

    public TestConsumerTarget getConsumerTarget()
    {
        return _consumerTarget;
    }
}
