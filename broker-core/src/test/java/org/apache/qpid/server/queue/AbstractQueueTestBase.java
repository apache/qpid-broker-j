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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
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
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.QueueNotificationListener;
import org.apache.qpid.server.queue.AbstractQueue.QueueEntryFilter;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;
import org.apache.qpid.server.virtualhost.MessageDestinationIsAlternateException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.UnknownAlternateBindingException;
import org.apache.qpid.test.utils.UnitTestBase;


abstract class AbstractQueueTestBase extends UnitTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQueueTestBase.class);
    private Queue<?> _queue;
    private QueueManagingVirtualHost<?> _virtualHost;
    private String _qname = "qname";
    private String _owner = "owner";
    private String _routingKey = "routing key";
    private DirectExchangeImpl _exchange;
    private TestConsumerTarget _consumerTarget = new TestConsumerTarget();  // TODO replace with minimally configured mockito mock
    private QueueConsumer<?,?> _consumer;
    private Map<String,Object> _arguments = Collections.emptyMap();

    @Before
    public void setUp() throws Exception
    {
        BrokerTestHelper.setUp();

        _virtualHost = BrokerTestHelper.createVirtualHost(getClass().getName(), this);

        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);

        _queue = (AbstractQueue<?>) _virtualHost.createChild(Queue.class, attributes);

        _exchange = (DirectExchangeImpl) _virtualHost.getChildByName(Exchange.class, ExchangeDefaults.DIRECT_EXCHANGE_NAME);
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            _queue.close();
            _virtualHost.close();
        }
        finally
        {
            BrokerTestHelper.tearDown();
        }
    }

    @Test
    public void testCreateQueue() throws Exception
    {
        _queue.close();
        try
        {
            Map<String,Object> attributes = new HashMap<>(_arguments);

            _queue =  _virtualHost.createChild(Queue.class, attributes);
            assertNull("Queue was created", _queue);
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Exception was not about missing name", e.getMessage().contains("name"));
        }

        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, "differentName");
        _queue =  _virtualHost.createChild(Queue.class, attributes);
        assertNotNull("Queue was not created", _queue);
    }



    @Test
    public void testGetVirtualHost()
    {
        assertEquals("Virtual host was wrong", _virtualHost, _queue.getVirtualHost());
    }

    @Test
    public void testBinding() throws Exception
    {
        _exchange.addBinding(_routingKey, _queue, Collections.EMPTY_MAP);

        assertTrue("Routing key was not bound", _exchange.isBound(_routingKey));
        assertTrue("Queue was not bound to key", _exchange.isBound(_routingKey, _queue));
        assertEquals("Exchange binding count", (long) 1, (long) _queue.getPublishingLinks().size());
        final Binding firstBinding = (Binding) _queue.getPublishingLinks().iterator().next();
        assertEquals("Wrong binding key", _routingKey, firstBinding.getBindingKey());

        _exchange.deleteBinding(_routingKey, _queue);
        assertFalse("Routing key was still bound", _exchange.isBound(_routingKey));
    }

    @Test
    public void testRegisterConsumerThenEnqueueMessage() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));

        // Check adding a consumer adds it to the queue
        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);
        assertEquals("Queue does not have consumer", (long) 1, (long) _queue.getConsumerCount());
        assertEquals("Queue does not have active consumer", (long) 1, (long) _queue.getConsumerCountWithCredit());

        // Check sending a message ends up with the subscriber
        _queue.enqueue(messageA, null, null);
        while(_consumerTarget.processPending());

        assertEquals(messageA, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull(_consumer.getQueueContext().getReleasedEntry());

        // Check removing the consumer removes it's information from the queue
        _consumer.close();
        assertTrue("Consumer still had queue", _consumerTarget.isClosed());
        assertFalse("Queue still has consumer", 1 == _queue.getConsumerCount());
        assertFalse("Queue still has active consumer", 1 == _queue.getConsumerCountWithCredit());

        ServerMessage messageB = createMessage(new Long (25));
        _queue.enqueue(messageB, null, null);
        assertNull(_consumer.getQueueContext());
    }

    @Test
    public void testEnqueueMessageThenRegisterConsumer() throws Exception, InterruptedException
    {
        ServerMessage messageA = createMessage(new Long(24));
        _queue.enqueue(messageA, null, null);
        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());
        assertEquals(messageA, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull("There should be no releasedEntry after an enqueue",
                          _consumer.getQueueContext().getReleasedEntry());
    }

    /**
     * Tests enqueuing two messages.
     */
    @Test
    public void testEnqueueTwoMessagesThenRegisterConsumer() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));
        ServerMessage messageB = createMessage(new Long(25));
        _queue.enqueue(messageA, null, null);
        _queue.enqueue(messageB, null, null);
        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());
        assertEquals(messageB, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull("There should be no releasedEntry after enqueues",
                          _consumer.getQueueContext().getReleasedEntry());
    }

    @Test
    public void testMessageHeldIfNotYetValidWhenConsumerAdded() throws Exception
    {
        _queue.close();
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);
        attributes.put(Queue.HOLD_ON_PUBLISH_ENABLED, Boolean.TRUE);

        _queue = _virtualHost.createChild(Queue.class, attributes);

        ServerMessage messageA = createMessage(new Long(24));
        AMQMessageHeader messageHeader = messageA.getMessageHeader();
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()+20000L);
        _queue.enqueue(messageA, null, null);
        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());

        assertEquals("Message which was not yet valid was received",
                            (long) 0,
                            (long) _consumerTarget.getMessages().size());
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()-100L);
        _queue.checkMessageStatus();
        while(_consumerTarget.processPending());
        assertEquals("Message which was valid was not received",
                            (long) 1,
                            (long) _consumerTarget.getMessages().size());
    }

    @Test
    public void testMessageHoldingDependentOnQueueProperty() throws Exception
    {
        _queue.close();
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);
        attributes.put(Queue.HOLD_ON_PUBLISH_ENABLED, Boolean.FALSE);

        _queue = _virtualHost.createChild(Queue.class, attributes);

        ServerMessage messageA = createMessage(new Long(24));
        AMQMessageHeader messageHeader = messageA.getMessageHeader();
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()+20000L);
        _queue.enqueue(messageA, null, null);
        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());

        assertEquals("Message was held despite queue not having holding enabled",
                            (long) 1,
                            (long) _consumerTarget.getMessages().size());
    }

    @Test
    public void testUnheldMessageOvertakesHeld() throws Exception
    {
        _queue.close();
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);
        attributes.put(Queue.HOLD_ON_PUBLISH_ENABLED, Boolean.TRUE);

        _queue = _virtualHost.createChild(Queue.class, attributes);

        ServerMessage messageA = createMessage(new Long(24));
        AMQMessageHeader messageHeader = messageA.getMessageHeader();
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()+20000L);
        _queue.enqueue(messageA, null, null);
        ServerMessage messageB = createMessage(new Long(25));
        _queue.enqueue(messageB, null, null);

        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);
        while(_consumerTarget.processPending());

        assertEquals("Expect one message (message B)", (long) 1, (long) _consumerTarget.getMessages().size());
        assertEquals("Wrong message received",
                            messageB.getMessageHeader().getMessageId(),
                            _consumerTarget.getMessages().get(0).getMessage().getMessageHeader().getMessageId());


        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()-100L);
        _queue.checkMessageStatus();
        while(_consumerTarget.processPending());
        assertEquals("Message which was valid was not received",
                            (long) 2,
                            (long) _consumerTarget.getMessages().size());
        assertEquals("Wrong message received",
                            messageA.getMessageHeader().getMessageId(),
                            _consumerTarget.getMessages().get(1).getMessage().getMessageHeader().getMessageId());
    }

    /**
     * Tests that a released queue entry is resent to the subscriber.  Verifies also that the
     * QueueContext._releasedEntry is reset to null after the entry has been reset.
     */
    @Test
    public void testReleasedMessageIsResentToSubscriber() throws Exception
    {

        ServerMessage messageA = createMessage(new Long(24));
        ServerMessage messageB = createMessage(new Long(25));
        ServerMessage messageC = createMessage(new Long(26));


        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue three messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);
        _queue.enqueue(messageC, postEnqueueAction, null);

        while(_consumerTarget.processPending());

        assertEquals("Unexpected total number of messages sent to consumer",
                            (long) 3,
                            (long) _consumerTarget.getMessages().size());
        assertFalse("Redelivery flag should not be set", queueEntries.get(0).isRedelivered());
        assertFalse("Redelivery flag should not be set", queueEntries.get(1).isRedelivered());
        assertFalse("Redelivery flag should not be set", queueEntries.get(2).isRedelivered());

        /* Now release the first message only, causing it to be requeued */

        queueEntries.get(0).release();

        while(_consumerTarget.processPending());

        assertEquals("Unexpected total number of messages sent to consumer",
                            (long) 4,
                            (long) _consumerTarget.getMessages().size());
        assertTrue("Redelivery flag should now be set", queueEntries.get(0).isRedelivered());
        assertFalse("Redelivery flag should remain be unset", queueEntries.get(1).isRedelivered());
        assertFalse("Redelivery flag should remain be unset", queueEntries.get(2).isRedelivered());
        assertNull("releasedEntry should be cleared after requeue processed",
                          _consumer.getQueueContext().getReleasedEntry());
    }

    /**
     * Tests that a released message that becomes expired is not resent to the subscriber.
     * This tests ensures that SimpleAMQQueue<?>Entry.getNextAvailableEntry avoids expired entries.
     * Verifies also that the QueueContext._releasedEntry is reset to null after the entry has been reset.
     */
    @Test
    public void testReleaseMessageThatBecomesExpiredIsNotRedelivered() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));
        final CountDownLatch sendIndicator = new CountDownLatch(1);
        _consumerTarget = new TestConsumerTarget()
        {

            @Override
            public void notifyWork()
            {
                while(processPending());
            }

            @Override
            public void send(MessageInstanceConsumer consumer, MessageInstance entry, boolean batch)
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
                                                          EnumSet.of(ConsumerOption.SEES_REQUEUES,
                                                                     ConsumerOption.ACQUIRES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue one message with expiration set for a short time in the future */

        final long expiration = System.currentTimeMillis() + 100L;
        when(messageA.getExpiration()).thenReturn(expiration);

        _queue.enqueue(messageA, postEnqueueAction, null);

        assertTrue("Message was not sent during expected time interval",
                          sendIndicator.await(5000, TimeUnit.MILLISECONDS));

        assertEquals("Unexpected total number of messages sent to consumer",
                            (long) 1,
                            (long) _consumerTarget.getMessages().size());
        QueueEntry queueEntry = queueEntries.get(0);

        final CountDownLatch dequeueIndicator = new CountDownLatch(1);
        queueEntry.addStateChangeListener(new StateChangeListener<MessageInstance, MessageInstance.EntryState>()
        {
            @Override
            public void stateChanged(MessageInstance object, MessageInstance.EntryState oldState, MessageInstance.EntryState newState)
            {
                if (newState.equals(MessageInstance.DEQUEUED_STATE))
                {
                    dequeueIndicator.countDown();
                }
            }
        });
        assertFalse("Redelivery flag should not be set", queueEntry.isRedelivered());

        /* Wait a little more to be sure that message will have expired, then release the first message only, causing it to be requeued */
        while(!queueEntry.expired() && System.currentTimeMillis() <= expiration )
        {
            Thread.sleep(10);
        }

        assertTrue("Expecting the queue entry to be now expired", queueEntry.expired());
        queueEntry.release();

        assertTrue("Message was not de-queued due to expiration",
                          dequeueIndicator.await(5000, TimeUnit.MILLISECONDS));

        assertEquals("Total number of messages sent should not have changed",
                            (long) 1,
                            (long) _consumerTarget.getMessages().size());
        assertFalse("Redelivery flag should not be set", queueEntry.isRedelivered());

        // QueueContext#_releasedEntry is updated after notification, thus, we need to make sure that it is updated
        long waitLoopLimit = 10;
        while(_consumer.getQueueContext().getReleasedEntry() != null && waitLoopLimit-- > 0 )
        {
            Thread.sleep(10);
        }
        assertNull("releasedEntry should be cleared after requeue processed:" + _consumer.getQueueContext().getReleasedEntry(),
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

        ServerMessage messageA = createMessage(new Long(24));
        ServerMessage messageB = createMessage(new Long(25));
        ServerMessage messageC = createMessage(new Long(26));

        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue three messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);
        _queue.enqueue(messageC, postEnqueueAction, null);

        while(_consumerTarget.processPending());

        assertEquals("Unexpected total number of messages sent to consumer",
                            (long) 3,
                            (long) _consumerTarget.getMessages().size());
        assertFalse("Redelivery flag should not be set", queueEntries.get(0).isRedelivered());
        assertFalse("Redelivery flag should not be set", queueEntries.get(1).isRedelivered());
        assertFalse("Redelivery flag should not be set", queueEntries.get(2).isRedelivered());

        /* Now release the third and first message only, causing it to be requeued */

        queueEntries.get(2).release();
        queueEntries.get(0).release();

        while(_consumerTarget.processPending());

        assertEquals("Unexpected total number of messages sent to consumer",
                            (long) 5,
                            (long) _consumerTarget.getMessages().size());
        assertTrue("Redelivery flag should now be set", queueEntries.get(0).isRedelivered());
        assertFalse("Redelivery flag should remain be unset", queueEntries.get(1).isRedelivered());
        assertTrue("Redelivery flag should now be set", queueEntries.get(2).isRedelivered());
        assertNull("releasedEntry should be cleared after requeue processed",
                          _consumer.getQueueContext().getReleasedEntry());
    }


    /**
     * Tests that a release requeues an entry for a queue with multiple consumers.  Verifies that a
     * requeue resends a message to a <i>single</i> subscriber.
     */
    @Test
    public void testReleaseForQueueWithMultipleConsumers() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));
        ServerMessage messageB = createMessage(new Long(25));

        TestConsumerTarget target1 = new TestConsumerTarget();
        TestConsumerTarget target2 = new TestConsumerTarget();


        QueueConsumer consumer1 = (QueueConsumer) _queue.addConsumer(target1, null, messageA.getClass(), "test",
                                                                     EnumSet.of(ConsumerOption.ACQUIRES,
                                                                                ConsumerOption.SEES_REQUEUES), 0);

        QueueConsumer consumer2 = (QueueConsumer) _queue.addConsumer(target2, null, messageA.getClass(), "test",
                                                                     EnumSet.of(ConsumerOption.ACQUIRES,
                                                                                ConsumerOption.SEES_REQUEUES), 0);


        final ArrayList<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue two messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);

        while(target1.processPending());
        while(target2.processPending());

        assertEquals("Unexpected total number of messages sent to both after enqueue",
                            (long) 2,
                            (long) (target1.getMessages().size() + target2.getMessages().size()));

        /* Now release the first message only, causing it to be requeued */
        queueEntries.get(0).release();

        while(target1.processPending());
        while(target2.processPending());

        assertEquals("Unexpected total number of messages sent to both consumers after release",
                            (long) 3,
                            (long) (target1.getMessages().size() + target2.getMessages().size()));
        assertNull("releasedEntry should be cleared after requeue processed",
                          consumer1.getQueueContext().getReleasedEntry());
        assertNull("releasedEntry should be cleared after requeue processed",
                          consumer2.getQueueContext().getReleasedEntry());
    }

    @Test
    public void testExclusiveConsumer() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));
        // Check adding an exclusive consumer adds it to the queue

        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.EXCLUSIVE, ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);

        assertEquals("Queue does not have consumer", (long) 1, (long) _queue.getConsumerCount());
        assertEquals("Queue does not have active consumer", (long) 1, (long) _queue.getConsumerCountWithCredit());

        // Check sending a message ends up with the subscriber
        _queue.enqueue(messageA, null, null);

        while(_consumerTarget.processPending());

        assertEquals("Queue context did not see expected message",
                            messageA,
                            _consumer.getQueueContext().getLastSeenEntry().getMessage());

        // Check we cannot add a second subscriber to the queue
        TestConsumerTarget subB = new TestConsumerTarget();
        Exception ex = null;
        try
        {

            _queue.addConsumer(subB, null, messageA.getClass(), "test",
                               EnumSet.of(ConsumerOption.ACQUIRES,
                                          ConsumerOption.SEES_REQUEUES), 0);

        }
        catch (MessageSource.ExistingExclusiveConsumer e)
        {
           ex = e;
        }
        assertNotNull(ex);

        // Check we cannot add an exclusive subscriber to a queue with an
        // existing consumer
        _consumer.close();
        _consumer = (QueueConsumer<?,?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerOption.ACQUIRES,
                                                                     ConsumerOption.SEES_REQUEUES), 0);

        try
        {

            _consumer = (QueueConsumer<?,?>) _queue.addConsumer(subB, null, messageA.getClass(), "test",
                                                              EnumSet.of(ConsumerOption.EXCLUSIVE), 0);

        }
        catch (MessageSource.ExistingConsumerPreventsExclusive e)
        {
           ex = e;
        }
        assertNotNull(ex);
    }

    /**
     * Tests that dequeued message is not present in the list returned form
     * {@link AbstractQueue#getMessagesOnTheQueue()}
     */
    @Test
    public void testGetMessagesOnTheQueueWithDequeuedEntry()
    {
        int messageNumber = 4;
        int dequeueMessageIndex = 1;

        // send test messages into a test queue
        enqueueGivenNumberOfMessages(_queue, messageNumber);

        // dequeue message
        dequeueMessage(_queue, dequeueMessageIndex);

        // get messages on the queue
        List<? extends QueueEntry> entries = _queue.getMessagesOnTheQueue();

        // assert queue entries
        assertEquals((long) (messageNumber - 1), (long) entries.size());
        int expectedId = 0;
        for (int i = 0; i < messageNumber - 1; i++)
        {
            Long id = ( entries.get(i).getMessage()).getMessageNumber();
            if (i == dequeueMessageIndex)
            {
                assertFalse("Message with id " + dequeueMessageIndex
                                   + " was dequeued and should not be returned by method getMessagesOnTheQueue!",
                                   new Long(expectedId).equals(id));
                expectedId++;
            }
            assertEquals("Expected message with id " + expectedId + " but got message with id " + id,
                                new Long(expectedId),
                                id);
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
        int messageNumber = 4;
        int dequeueMessageIndex = 1;

        // send test messages into a test queue
        enqueueGivenNumberOfMessages(_queue, messageNumber);

        // dequeue message
        dequeueMessage(_queue, dequeueMessageIndex);

        // get messages on the queue with filter accepting all available messages
        List<? extends QueueEntry> entries = ((AbstractQueue)_queue).getMessagesOnTheQueue(new QueueEntryFilter()
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
        assertEquals((long) (messageNumber - 1), (long) entries.size());
        int expectedId = 0;
        for (int i = 0; i < messageNumber - 1; i++)
        {
            Long id = (entries.get(i).getMessage()).getMessageNumber();
            if (i == dequeueMessageIndex)
            {
                assertFalse("Message with id " + dequeueMessageIndex
                                   + " was dequeued and should not be returned by method getMessagesOnTheQueue!",
                                   new Long(expectedId).equals(id));
                expectedId++;
            }
            assertEquals("Expected message with id " + expectedId + " but got message with id " + id,
                                new Long(expectedId),
                                id);
            expectedId++;
        }
    }

    /**
     * Tests that all messages including dequeued one are deleted from the queue
     * on invocation of {@link AbstractQueue#clearQueue()}
     */
    @Test
    public void testClearQueueWithDequeuedEntry() throws Exception
    {
        int messageNumber = 4;
        int dequeueMessageIndex = 1;

        // put messages into a test queue
        enqueueGivenNumberOfMessages(_queue, messageNumber);

        // dequeue message on a test queue
        dequeueMessage(_queue, dequeueMessageIndex);

        // clean queue
        _queue.clearQueue();

        // get queue entries
        List<? extends QueueEntry> entries = _queue.getMessagesOnTheQueue();

        // assert queue entries
        assertNotNull(entries);
        assertEquals((long) 0, (long) entries.size());
    }

    @Test
    public void testNotificationFiredOnEnqueue() throws Exception
    {
        QueueNotificationListener listener = mock(QueueNotificationListener .class);

        _queue.setNotificationListener(listener);
        _queue.setAttributes(Collections.<String, Object>singletonMap(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES,
                                                                      Integer.valueOf(2)));

        _queue.enqueue(createMessage(new Long(24)), null, null);
        verifyNoInteractions(listener);

        _queue.enqueue(createMessage(new Long(25)), null, null);

        verify(listener, atLeastOnce()).notifyClients(eq(NotificationCheck.MESSAGE_COUNT_ALERT), eq(_queue), contains("Maximum count on queue threshold"));
    }

    @Test
    public void testNotificationFiredAsync() throws Exception
    {
        QueueNotificationListener  listener = mock(QueueNotificationListener .class);

        _queue.enqueue(createMessage(new Long(24)), null, null);
        _queue.enqueue(createMessage(new Long(25)), null, null);
        _queue.enqueue(createMessage(new Long(26)), null, null);

        _queue.setNotificationListener(listener);
        _queue.setAttributes(Collections.<String, Object>singletonMap(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES,
                                                                      Integer.valueOf(2)));

        verifyNoInteractions(listener);

        _queue.checkMessageStatus();

        verify(listener, atLeastOnce()).notifyClients(eq(NotificationCheck.MESSAGE_COUNT_ALERT), eq(_queue), contains("Maximum count on queue threshold"));

    }


    @Test
    public void testMaximumMessageTtl() throws Exception
    {

        // Test scenarios where only the maximum TTL has been set

        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME,"testTtlOverrideMaximumTTl");
        attributes.put(Queue.MAXIMUM_MESSAGE_TTL, 10000l);

        Queue<?> queue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals("TTL has not been overridden", 60000l, getExpirationOnQueue(queue, 50000l, 0l));

        assertEquals("TTL has not been overridden", 60000l, getExpirationOnQueue(queue, 50000l, 65000l));

        assertEquals("TTL has been incorrectly overridden", 55000l, getExpirationOnQueue(queue, 50000l, 55000l));

        long tooLateExpiration = System.currentTimeMillis() + 20000l;

        assertTrue("TTL has not been overridden",
                          tooLateExpiration != getExpirationOnQueue(queue, 0l, tooLateExpiration));

        long acceptableExpiration = System.currentTimeMillis() + 5000l;

        assertEquals("TTL has been incorrectly overriden",
                            acceptableExpiration,
                            getExpirationOnQueue(queue, 0l, acceptableExpiration));

        // Test the scenarios where only the minimum TTL has been set

        attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME,"testTtlOverrideMinimumTTl");
        attributes.put(Queue.MINIMUM_MESSAGE_TTL, 10000l);

        queue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals("TTL has been overridden incorrectly", 0l, getExpirationOnQueue(queue, 50000l, 0l));

        assertEquals("TTL has been overridden incorrectly", 65000l, getExpirationOnQueue(queue, 50000l, 65000l));

        assertEquals("TTL has not been overriden", 60000l, getExpirationOnQueue(queue, 50000l, 55000l));

        long unacceptableExpiration = System.currentTimeMillis() + 5000l;

        assertTrue("TTL has not been overridden",
                          unacceptableExpiration != getExpirationOnQueue(queue, 0l, tooLateExpiration));

        acceptableExpiration = System.currentTimeMillis() + 20000l;

        assertEquals("TTL has been incorrectly overridden",
                            acceptableExpiration,
                            getExpirationOnQueue(queue, 0l, acceptableExpiration));

        // Test the scenarios where both the minimum and maximum TTL have been set

        attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME,"testTtlOverrideBothTTl");
        attributes.put(Queue.MINIMUM_MESSAGE_TTL, 10000l);
        attributes.put(Queue.MAXIMUM_MESSAGE_TTL, 20000l);

        queue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals("TTL has not been overridden", 70000l, getExpirationOnQueue(queue, 50000l, 0l));

        assertEquals("TTL has been overridden incorrectly", 65000l, getExpirationOnQueue(queue, 50000l, 65000l));

        assertEquals("TTL has not been overridden", 60000l, getExpirationOnQueue(queue, 50000l, 55000l));
    }

    @Test
    public void testOldestMessage()
    {
        Queue<?> queue = getQueue();
        queue.enqueue(createMessage(1l, (byte)1, Collections.singletonMap("sortKey", (Object) "Z"), 10l), null, null);
        queue.enqueue(createMessage(2l, (byte)4, Collections.singletonMap("sortKey", (Object) "M"), 100l), null, null);
        queue.enqueue(createMessage(3l, (byte)9, Collections.singletonMap("sortKey", (Object) "A"), 1000l), null, null);

        assertEquals(10l, queue.getOldestMessageArrivalTime());
    }

    @Test
    public void testNoneOverflowPolicy()
    {
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 2);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 100);

        Queue<?> queue = getQueue();
        queue.setAttributes(attributes);

        ServerMessage message = createMessage(new Long(24), 50, 50);
        when(message.getArrivalTime()).thenReturn(10l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(25), 50, 50);
        when(message.getArrivalTime()).thenReturn(50l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(26), 50, 50);
        when(message.getArrivalTime()).thenReturn(200l);
        queue.enqueue(message, null, null);

        assertEquals("Wrong number of messages in queue", (long) 3, (long) queue.getQueueDepthMessages());
        assertEquals("Wrong size of messages in queue", (long) 300, queue.getQueueDepthBytes());
        assertEquals("Wrong oldest message",
                            10l,
                            ((AbstractQueue) queue).getEntries().getOldestEntry().getMessage().getArrivalTime());
    }

    @Test
    public void testRingOverflowPolicyMaxCount()
    {
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 4);

        Queue<?> queue = getQueue();
        queue.setAttributes(attributes);

        ServerMessage message = createMessage(new Long(24), 10, 10);
        when(message.getArrivalTime()).thenReturn(10l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(25), 10, 10);
        when(message.getArrivalTime()).thenReturn(50l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(26), 10, 10);
        when(message.getArrivalTime()).thenReturn(200l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(27), 10, 10);
        when(message.getArrivalTime()).thenReturn(500l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(28), 10, 10);
        when(message.getArrivalTime()).thenReturn(1000l);
        queue.enqueue(message, null, null);

        assertEquals("Wrong number of messages in queue", (long) 4, (long) queue.getQueueDepthMessages());
        assertEquals("Wrong size of messages in queue", (long) 80, queue.getQueueDepthBytes());
        assertEquals("Wrong oldest message",
                            50l,
                            ((AbstractQueue) queue).getEntries().getOldestEntry().getMessage().getArrivalTime());
    }

    @Test
    public void testRingOverflowPolicyMaxSize()
    {
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 4);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 100);

        Queue<?> queue = getQueue();
        queue.setAttributes(attributes);

        ServerMessage message = createMessage(new Long(24), 10, 10);
        when(message.getArrivalTime()).thenReturn(10l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(25), 10, 10);
        when(message.getArrivalTime()).thenReturn(50l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(26), 20, 10);
        when(message.getArrivalTime()).thenReturn(200l);
        queue.enqueue(message, null, null);
        message = createMessage(new Long(27), 20, 10);
        when(message.getArrivalTime()).thenReturn(200l);
        queue.enqueue(message, null, null);

        assertEquals("Wrong number of messages in queue", (long) 4, (long) queue.getQueueDepthMessages());
        assertEquals("Wrong size of messages in queue", (long) 100, queue.getQueueDepthBytes());

        message = createMessage(new Long(27), 20, 10);
        when(message.getArrivalTime()).thenReturn(500l);
        queue.enqueue(message, null, null);

        assertEquals("Wrong number of messages in queue", (long) 3, (long) queue.getQueueDepthMessages());
        assertEquals("Wrong size of messages in queue", (long) 90, queue.getQueueDepthBytes());
        assertEquals("Wrong oldest message",
                            200l,
                            ((AbstractQueue) queue).getEntries().getOldestEntry().getMessage().getArrivalTime());
    }

    @Test
    public void testRingOverflowPolicyMessagesRejected()
    {
        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 0);

        Queue<?> queue = getQueue();
        queue.setAttributes(attributes);

        ServerMessage message;
        RoutingResult result;

        message = createMessage(new Long(27), 20, 10);
        result = queue.route(message, message.getInitialRoutingAddress(), null);
        assertTrue("Result should include not accepting route", result.isRejected());

        int headerSize = 20;
        int payloadSize = 10;
        int id = 28;

        attributes = new HashMap<>(_arguments);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 10);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 10);
        queue.setAttributes(attributes);

        message = createMessage(new Long(id), headerSize, payloadSize);
        result = queue.route(message, message.getInitialRoutingAddress(), null);
        assertTrue("Result should include not accepting route", result.isRejected());
    }

    @Test
    public void testAlternateBindingValidationRejectsNonExistingDestination()
    {
        Map<String, Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName());
        String alternateBinding = "nonExisting";
        attributes.put(Queue.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, alternateBinding));

        try
        {
            _virtualHost.createChild(Queue.class, attributes);
            fail("Expected exception is not thrown");
        }
        catch (UnknownAlternateBindingException e)
        {
            assertEquals("Unexpected exception alternate binding", alternateBinding, e.getAlternateBindingName());
        }
    }

    @Test
    public void testAlternateBindingValidationRejectsSelf()
    {
        Map<String, String> alternateBinding = Collections.singletonMap(AlternateBinding.DESTINATION, _qname);
        Map<String, Object> newAttributes = Collections.singletonMap(Queue.ALTERNATE_BINDING, alternateBinding);
        try
        {
            _queue.setAttributes(newAttributes);
            fail("Expected exception is not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testDurableQueueRejectsNonDurableAlternateBinding()
    {
        Map<String, Object> dlqAttributes = new HashMap<>(_arguments);
        String dlqName = getTestName() + "_DLQ";
        dlqAttributes.put(Queue.NAME, dlqName);
        dlqAttributes.put(Queue.DURABLE, false);
        _virtualHost.createChild(Queue.class, dlqAttributes);

        Map<String, Object> queueAttributes = new HashMap<>(_arguments);
        queueAttributes.put(Queue.NAME, getTestName());
        queueAttributes.put(Queue.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, dlqName));
        queueAttributes.put(Queue.DURABLE, true);

        try
        {
            _virtualHost.createChild(Queue.class, queueAttributes);
            fail("Expected exception is not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testAlternateBinding()
    {
        Map<String, Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, _qname));

        Queue newQueue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals("Unexpected alternate binding", _qname, newQueue.getAlternateBinding().getDestination());
    }

    @Test
    public void testDeleteOfQueueSetAsAlternate()
    {
        Map<String, Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, _qname));

        Queue newQueue = _virtualHost.createChild(Queue.class, attributes);
        assertEquals("Unexpected alternate binding", _qname, newQueue.getAlternateBinding().getDestination());
        try
        {
            _queue.delete();
            fail("Expected exception is not thrown");
        }
        catch (MessageDestinationIsAlternateException e)
        {
            //pass
        }
        assertFalse(_queue.isDeleted());
    }

    @Test
    public void testMoveMessages() throws Exception
    {
        doMoveOrCopyMessageTest(true);
    }

    @Test
    public void testCopyMessages() throws Exception
    {
        doMoveOrCopyMessageTest(false);
    }

    @Test
    public void testExpiryPolicyRouteToAlternate()
    {
        Map<String, Object> dlqAttributes = new HashMap<>();
        dlqAttributes.put(Queue.NAME, getTestName() + "_dlq");
        dlqAttributes.put(Queue.MINIMUM_MESSAGE_TTL, Long.MAX_VALUE);
        Queue<?> dlq = _virtualHost.createChild(Queue.class, dlqAttributes);

        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.ALTERNATE_BINDING, Collections.singletonMap("destination", dlq.getName()));
        attributes.put(Queue.EXPIRY_POLICY, Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE);

        Queue<?> queue = _virtualHost.createChild(Queue.class, attributes);

        ServerMessage message = createMessage(1L);
        long arrivalTime = 50000L;
        when(message.getArrivalTime()).thenReturn(arrivalTime);
        when(message.getExpiration()).thenReturn(arrivalTime + 5000L);
        when(message.isResourceAcceptable(any())).thenReturn(true);
        queue.enqueue(message,null, null);

        assertEquals("Unexpected queue depth", 1, queue.getQueueDepthMessages());

        queue.checkMessageStatus();

        assertEquals("Unexpected queue depth after checking message status", 0, queue.getQueueDepthMessages());
        assertEquals("Unexpected DLQ depth", 1, dlq.getQueueDepthMessages());
    }

    private void doMoveOrCopyMessageTest(final boolean move)
    {
        Queue target = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_target"));

        _queue.enqueue(createMessage(1L), null, null);
        _queue.enqueue(createMessage(2L), null, null);
        _queue.enqueue(createMessage(3L), null, null);

        assertEquals("Unexpected number of messages on source queue",
                            (long) 3,
                            (long) _queue.getQueueDepthMessages());
        assertEquals("Unexpected number of messages on target queue before test",
                            (long) 0,
                            (long) target.getQueueDepthMessages());

        if (move)
        {
            _queue.moveMessages(target, null, "true = true", -1);
        }
        else
        {
            _queue.copyMessages(target, null, "true = true", -1);

        }

        final long expected = move ? 0 : 3;
        assertEquals("Unexpected number of messages on source queue after test", expected,
                            (long) _queue.getQueueDepthMessages());
        assertEquals("Unexpected number of messages on target queue after test",
                            (long) 3,
                            (long) target.getQueueDepthMessages());
    }

    @Test
    public void testCopyMessageRespectsQueueSizeLimits() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, getTestName() + "_target");
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.RING);
        attributes.put(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 2);

        Queue target = _virtualHost.createChild(Queue.class, attributes);

        _queue.enqueue(createMessage(1L), null, null);
        _queue.enqueue(createMessage(2L), null, null);
        _queue.enqueue(createMessage(3L), null, null);

        assertEquals("Unexpected number of messages on source queue",
                            (long) 3,
                            (long) _queue.getQueueDepthMessages());
        assertEquals("Unexpected number of messages on target queue before test",
                            (long) 0,
                            (long) target.getQueueDepthMessages());

        _queue.copyMessages(target, null, "true = true", -1);

        assertEquals("Unexpected number of messages on source queue after test",
                            (long) 3,
                            (long) _queue.getQueueDepthMessages());
        assertEquals("Unexpected number of messages on target queue after test",
                            (long) 2,
                            (long) target.getQueueDepthMessages());
    }

    @Test
    public void testEnqueuedMessageFlowedToDisk() throws Exception
    {
        makeVirtualHostTargetSizeExceeded();

        final ServerMessage message2 = createMessage(1L, 2, 3);
        final long sizeIncludingHeader = message2.getSizeIncludingHeader();
        final StoredMessage storedMessage = message2.getStoredMessage();
        when(storedMessage.getInMemorySize()).thenReturn(sizeIncludingHeader);

        _queue.enqueue(message2, null, null);

        verify(storedMessage).getInMemorySize();
        verify(storedMessage).flowToDisk();

        assertEquals("Unexpected number of messages on the queue",
                     2,
                     _queue.getQueueDepthMessages());
    }

    @Test
    public void testEnqueuedMalformedMessageDeleted() throws Exception
    {
        makeVirtualHostTargetSizeExceeded();

        final ServerMessage message2 = createMessage(1L, 2, 3);
        final long sizeIncludingHeader = message2.getSizeIncludingHeader();
        final StoredMessage storedMessage = message2.getStoredMessage();
        when(storedMessage.getInMemorySize()).thenReturn(sizeIncludingHeader);
        when(message2.checkValid()).thenReturn(false);

        _queue.enqueue(message2, null, null);

        verify(storedMessage).getInMemorySize();
        verify(storedMessage, never()).flowToDisk();

        assertEquals("Unexpected number of messages on the queue",
                     1,
                     _queue.getQueueDepthMessages());
    }

    @Test
    public void testVisit()
    {
        final ServerMessage message = createMessage(1L, 2, 3);
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

        final Map<String,Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);

        @SuppressWarnings("unchecked")
        final Queue queue = new AbstractQueue(attributes, _virtualHost)
        {
            @Override
            QueueEntryList getEntries()
            {
                return list;
            }

        };

        final MessageReference reference = mock(MessageReference.class);
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
        final ServerMessage message = createMessage(messageNumber, 2, 3);
        final MessageReference reference = message.newReference();
        doAnswer((Answer<Void>) invocationOnMock -> {
            messageDeleteDetector.countDown();
            return null;
        }).when(reference).release();

        _queue.enqueue(message, null, record);

        _queue.visit(entry -> {
            _queue.deleteEntry(entry);
            return false;
        });

        assertTrue("Message reference is not released withing given timeout interval",
                   messageDeleteDetector.await(2000L, TimeUnit.MILLISECONDS));
    }


    private void makeVirtualHostTargetSizeExceeded()
    {
        final InternalMessage message = InternalMessage.createMessage(_virtualHost.getMessageStore(),
                                                                       mock(AMQMessageHeader.class),
                                                                       "test",
                                                                       true,
                                                                       _qname);
        _queue.enqueue(message, null, null);
        assertEquals("Unexpected number of messages on the queue",
                     1,
                     _queue.getQueueDepthMessages());
        _virtualHost.setTargetSize(1L);
        assertTrue(_virtualHost.isOverTargetSize());
    }

    private long getExpirationOnQueue(final Queue<?> queue, long arrivalTime, long expiration)
    {
        final List<QueueEntry> entries = new ArrayList<>();

        ServerMessage message = createMessage(1l);
        when(message.getArrivalTime()).thenReturn(arrivalTime);
        when(message.getExpiration()).thenReturn(expiration);
        queue.enqueue(message,null, null);
        queue.visit(new QueueEntryVisitor()
        {
            @Override
            public boolean visit(final QueueEntry entry)
            {
                entries.add(entry);
                return true;
            }
        });
        assertEquals("Expected only one entry in the queue", (long) 1, (long) entries.size());

        Long entryExpiration =
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
        assertEquals((long) messageNumber, (long) entries.size());
        for (int i = 0; i < messageNumber; i++)
        {
            assertEquals((long)i, (entries.get(i).getMessage()).getMessageNumber());
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
     * @param queue
     * @param messageNumber
     */
    protected void putGivenNumberOfMessages(Queue<?> queue, int messageNumber)
    {
        for (int i = 0; i < messageNumber; i++)
        {
            // Create message
            ServerMessage message = null;
            message = createMessage((long)i);

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
    protected QueueEntry dequeueMessage(Queue<?> queue, int dequeueMessageIndex)
    {
        List<? extends QueueEntry> entries = queue.getMessagesOnTheQueue();
        QueueEntry entry = entries.get(dequeueMessageIndex);
        entry.acquire();
        entry.delete();
        assertTrue(entry.isDeleted());
        return entry;
    }

    protected void verifyReceivedMessages(List<MessageInstance> expected,
                                        List<MessageInstance> delivered)
    {
        assertEquals("Consumer did not receive the expected number of messages",
                            (long) expected.size(),
                            (long) delivered.size());

        for (MessageInstance msg : expected)
        {
            assertTrue("Consumer did not receive msg: "
                              + msg.getMessage().getMessageNumber(), delivered.contains(msg));
        }
    }

    public Queue<?> getQueue()
    {
        return _queue;
    }

    protected void setQueue(Queue<?> queue)
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

    public void setArguments(Map<String,Object> arguments)
    {
        _arguments = arguments;
    }

    protected ServerMessage createMessage(Long id, byte priority, final Map<String,Object> arguments, long arrivalTime)
    {
        ServerMessage message = createMessage(id);

        AMQMessageHeader hdr = message.getMessageHeader();
        when(hdr.getPriority()).thenReturn(priority);
        when(message.getArrivalTime()).thenReturn(arrivalTime);
        when(hdr.getHeaderNames()).thenReturn(arguments.keySet());
        final ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
        when(hdr.containsHeader(nameCaptor.capture())).thenAnswer(new Answer<Boolean>()
        {
            @Override
            public Boolean answer(final InvocationOnMock invocationOnMock) throws Throwable
            {
                return arguments.containsKey(nameCaptor.getValue());
            }
        });

        final ArgumentCaptor<Set> namesCaptor = ArgumentCaptor.forClass(Set.class);
        when(hdr.containsHeaders(namesCaptor.capture())).thenAnswer(new Answer<Boolean>()
        {
            @Override
            public Boolean answer(final InvocationOnMock invocationOnMock) throws Throwable
            {
                return arguments.keySet().containsAll(namesCaptor.getValue());
            }
        });

        final ArgumentCaptor<String> nameCaptor2 = ArgumentCaptor.forClass(String.class);
        when(hdr.getHeader(nameCaptor2.capture())).thenAnswer(new Answer<Object>()
        {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) throws Throwable
            {
                return arguments.get(nameCaptor2.getValue());
            }
        });


        return message;

    }

    protected ServerMessage createMessage(Long id, final int headerSize, final int payloadSize)
    {
        ServerMessage message = createMessage(id);
        when(message.getSizeIncludingHeader()).thenReturn(new Long(headerSize + payloadSize));
        return message;
    }

    protected ServerMessage createMessage(Long id)
    {
        AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(String.valueOf(id));
        ServerMessage message = mock(ServerMessage.class);
        when(message.getMessageNumber()).thenReturn(id);
        when(message.getMessageHeader()).thenReturn(header);
        when(message.checkValid()).thenReturn(true);

        StoredMessage storedMessage = mock(StoredMessage.class);
        when(message.getStoredMessage()).thenReturn(storedMessage);

        MessageReference ref = mock(MessageReference.class);
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
        public void performAction(MessageInstance entry)
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

    public DirectExchange getExchange()
    {
        return _exchange;
    }

    public TestConsumerTarget getConsumerTarget()
    {
        return _consumerTarget;
    }

}
