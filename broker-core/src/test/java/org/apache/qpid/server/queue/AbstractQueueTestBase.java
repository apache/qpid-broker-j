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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
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

import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.server.consumer.ConsumerImpl;
import org.apache.qpid.server.consumer.MockConsumer;
import org.apache.qpid.server.exchange.DirectExchange;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.QueueNotificationListener;
import org.apache.qpid.server.queue.AbstractQueue.QueueEntryFilter;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.QpidTestCase;

abstract class AbstractQueueTestBase extends QpidTestCase
{
    private static final Logger _logger = LoggerFactory.getLogger(AbstractQueueTestBase.class);
    private long _queueRunnerWaitTime;
    private Queue<?> _queue;
    private QueueManagingVirtualHost<?> _virtualHost;
    private String _qname = "qname";
    private String _owner = "owner";
    private String _routingKey = "routing key";
    private DirectExchange _exchange;
    private MockConsumer _consumerTarget = new MockConsumer();
    private QueueConsumer<?> _consumer;
    private Map<String,Object> _arguments = Collections.emptyMap();

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        BrokerTestHelper.setUp();

        _virtualHost = BrokerTestHelper.createVirtualHost(getClass().getName());

        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, _qname);
        attributes.put(Queue.OWNER, _owner);

        _queue = _virtualHost.createChild(Queue.class, attributes);

        _exchange = (DirectExchange) _virtualHost.getChildByName(Exchange.class, ExchangeDefaults.DIRECT_EXCHANGE_NAME);
        _queueRunnerWaitTime = Long.getLong("AbstractQueueTestBase.queueRunnerWaitTime", 150L);
        _logger.debug("Using AbstractQueueTestBase.queueRunnerWaitTime {}", _queueRunnerWaitTime);
    }

    @Override
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
            super.tearDown();
        }
    }

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
            assertTrue("Exception was not about missing name",
                            e.getMessage().contains("name"));
        }

        Map<String,Object> attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME, "differentName");
        _queue =  _virtualHost.createChild(Queue.class, attributes);
        assertNotNull("Queue was not created", _queue);
    }



    public void testGetVirtualHost()
    {
        assertEquals("Virtual host was wrong", _virtualHost, _queue.getVirtualHost());
    }

    public void testBinding()
    {
        _exchange.addBinding(_routingKey, _queue, Collections.EMPTY_MAP);

        assertTrue("Routing key was not bound",
                        _exchange.isBound(_routingKey));
        assertTrue("Queue was not bound to key",
                    _exchange.isBound(_routingKey,_queue));
        assertEquals("Exchange binding count", 1,
                     _queue.getBindings().size());
        final Binding<?> firstBinding = _queue.getBindings().iterator().next();
        assertEquals("Wrong exchange bound", _routingKey,
                     firstBinding.getBindingKey());
        assertEquals("Wrong exchange bound", _exchange,
                     firstBinding.getExchange());

        _exchange.deleteBinding(_routingKey, _queue);
        assertFalse("Routing key was still bound",
                _exchange.isBound(_routingKey));

    }

    public void testRegisterConsumerThenEnqueueMessage() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));

        // Check adding a consumer adds it to the queue
        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                  ConsumerImpl.Option.SEES_REQUEUES), 0);
        assertEquals("Queue does not have consumer", 1,
                     _queue.getConsumerCount());
        assertEquals("Queue does not have active consumer", 1,
                     _queue.getConsumerCountWithCredit());

        // Check sending a message ends up with the subscriber
        _queue.enqueue(messageA, null, null);
        Thread.sleep(_queueRunnerWaitTime);

        assertEquals(messageA, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull(_consumer.getQueueContext().getReleasedEntry());

        // Check removing the consumer removes it's information from the queue
        _consumer.close();
        assertTrue("Consumer still had queue", _consumerTarget.isClosed());
        assertFalse("Queue still has consumer", 1 == _queue.getConsumerCount());
        assertFalse("Queue still has active consumer",
                    1 == _queue.getConsumerCountWithCredit());

        ServerMessage messageB = createMessage(new Long (25));
        _queue.enqueue(messageB, null, null);
         assertNull(_consumer.getQueueContext());

    }

    public void testEnqueueMessageThenRegisterConsumer() throws Exception, InterruptedException
    {
        ServerMessage messageA = createMessage(new Long(24));
        _queue.enqueue(messageA, null, null);
        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                  ConsumerImpl.Option.SEES_REQUEUES), 0);
        Thread.sleep(_queueRunnerWaitTime);
        assertEquals(messageA, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull("There should be no releasedEntry after an enqueue",
                   _consumer.getQueueContext().getReleasedEntry());
    }

    /**
     * Tests enqueuing two messages.
     */
    public void testEnqueueTwoMessagesThenRegisterConsumer() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));
        ServerMessage messageB = createMessage(new Long(25));
        _queue.enqueue(messageA, null, null);
        _queue.enqueue(messageB, null, null);
        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                  ConsumerImpl.Option.SEES_REQUEUES), 0);
        Thread.sleep(_queueRunnerWaitTime);
        assertEquals(messageB, _consumer.getQueueContext().getLastSeenEntry().getMessage());
        assertNull("There should be no releasedEntry after enqueues",
                   _consumer.getQueueContext().getReleasedEntry());
    }

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
        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                                     ConsumerImpl.Option.SEES_REQUEUES), 0);
        Thread.sleep(_queueRunnerWaitTime);

        assertEquals("Message which was not yet valid was received", 0, _consumerTarget.getMessages().size());
        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()-100L);
        _queue.checkMessageStatus();
        Thread.sleep(_queueRunnerWaitTime);
        assertEquals("Message which was valid was not received", 1, _consumerTarget.getMessages().size());
    }

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
        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                                     ConsumerImpl.Option.SEES_REQUEUES), 0);
        Thread.sleep(_queueRunnerWaitTime);

        assertEquals("Message was held despite queue not having holding enabled", 1, _consumerTarget.getMessages().size());

    }

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

        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                                     ConsumerImpl.Option.SEES_REQUEUES), 0);
        Thread.sleep(_queueRunnerWaitTime);

        assertEquals("Expect one message (message B)", 1, _consumerTarget.getMessages().size());
        assertEquals("Wrong message received", messageB.getMessageHeader().getMessageId(), _consumerTarget.getMessages().get(0).getMessage().getMessageHeader().getMessageId());

        when(messageHeader.getNotValidBefore()).thenReturn(System.currentTimeMillis()-100L);
        _queue.checkMessageStatus();
        Thread.sleep(_queueRunnerWaitTime);
        assertEquals("Message which was valid was not received", 2, _consumerTarget.getMessages().size());
        assertEquals("Wrong message received", messageA.getMessageHeader().getMessageId(), _consumerTarget.getMessages().get(1).getMessage().getMessageHeader().getMessageId());

    }

    /**
     * Tests that a released queue entry is resent to the subscriber.  Verifies also that the
     * QueueContext._releasedEntry is reset to null after the entry has been reset.
     */
    public void testReleasedMessageIsResentToSubscriber() throws Exception
    {

        ServerMessage messageA = createMessage(new Long(24));
        ServerMessage messageB = createMessage(new Long(25));
        ServerMessage messageC = createMessage(new Long(26));


        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                      ConsumerImpl.Option.SEES_REQUEUES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue three messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);
        _queue.enqueue(messageC, postEnqueueAction, null);

        Thread.sleep(_queueRunnerWaitTime);  // Work done by QueueRunner Thread

        assertEquals("Unexpected total number of messages sent to consumer",
                     3,
                     _consumerTarget.getMessages().size());
        assertFalse("Redelivery flag should not be set", queueEntries.get(0).isRedelivered());
        assertFalse("Redelivery flag should not be set", queueEntries.get(1).isRedelivered());
        assertFalse("Redelivery flag should not be set", queueEntries.get(2).isRedelivered());

        /* Now release the first message only, causing it to be requeued */

        queueEntries.get(0).release();

        Thread.sleep(_queueRunnerWaitTime); // Work done by QueueRunner Thread

        assertEquals("Unexpected total number of messages sent to consumer",
                     4,
                     _consumerTarget.getMessages().size());
        assertTrue("Redelivery flag should now be set", queueEntries.get(0).isRedelivered());
        assertFalse("Redelivery flag should remain be unset", queueEntries.get(1).isRedelivered());
        assertFalse("Redelivery flag should remain be unset",queueEntries.get(2).isRedelivered());
        assertNull("releasedEntry should be cleared after requeue processed",
                   _consumer.getQueueContext().getReleasedEntry());
    }

    /**
     * Tests that a released message that becomes expired is not resent to the subscriber.
     * This tests ensures that SimpleAMQQueue<?>Entry.getNextAvailableEntry avoids expired entries.
     * Verifies also that the QueueContext._releasedEntry is reset to null after the entry has been reset.
     */
    public void testReleaseMessageThatBecomesExpiredIsNotRedelivered() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));
        final CountDownLatch sendIndicator = new CountDownLatch(1);
        _consumerTarget = new MockConsumer()
        {
            @Override
            public long send(ConsumerImpl consumer, MessageInstance entry, boolean batch)
            {
                try
                {
                    return super.send(consumer, entry, batch);
                }
                finally
                {
                    sendIndicator.countDown();
                }
            }
        };

        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.SEES_REQUEUES,
                                                      ConsumerImpl.Option.ACQUIRES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue one message with expiration set for a short time in the future */

        final long expiration = System.currentTimeMillis() + _queueRunnerWaitTime;
        when(messageA.getExpiration()).thenReturn(expiration);

        _queue.enqueue(messageA, postEnqueueAction, null);

        assertTrue("Message was not sent during expected time interval", sendIndicator.await(5000, TimeUnit.MILLISECONDS));

        assertEquals("Unexpected total number of messages sent to consumer", 1, _consumerTarget.getMessages().size());
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

        assertTrue("Message was not de-queued due to expiration", dequeueIndicator.await(5000, TimeUnit.MILLISECONDS));

        assertEquals("Total number of messages sent should not have changed", 1, _consumerTarget.getMessages().size());
        assertFalse("Redelivery flag should not be set", queueEntry.isRedelivered());

        // QueueContext#_releasedEntry is updated after notification, thus, we need to make sure that it is updated
        long waitLoopLimit = 10;
        while(_consumer.getQueueContext().getReleasedEntry() != null && waitLoopLimit-- > 0 )
        {
            Thread.sleep(10);
        }
        assertNull("releasedEntry should be cleared after requeue processed:" +  _consumer.getQueueContext().getReleasedEntry(),
                   _consumer.getQueueContext().getReleasedEntry());

    }

    /**
     * Tests that if a client releases entries 'out of order' (the order
     * used by QueueEntryImpl.compareTo) that messages are still resent
     * successfully.  Specifically this test ensures the {@see AbstractQueue#requeue()}
     * can correctly move the _releasedEntry to an earlier position in the QueueEntry list.
     */
    public void testReleasedOutOfComparableOrderAreRedelivered() throws Exception
    {

        ServerMessage messageA = createMessage(new Long(24));
        ServerMessage messageB = createMessage(new Long(25));
        ServerMessage messageC = createMessage(new Long(26));

        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                      ConsumerImpl.Option.SEES_REQUEUES), 0);

        final ArrayList<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue three messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);
        _queue.enqueue(messageC, postEnqueueAction, null);

        Thread.sleep(_queueRunnerWaitTime);  // Work done by QueueRunner Thread

        assertEquals("Unexpected total number of messages sent to consumer",
                     3,
                     _consumerTarget.getMessages().size());
        assertFalse("Redelivery flag should not be set", queueEntries.get(0).isRedelivered());
        assertFalse("Redelivery flag should not be set", queueEntries.get(1).isRedelivered());
        assertFalse("Redelivery flag should not be set", queueEntries.get(2).isRedelivered());

        /* Now release the third and first message only, causing it to be requeued */

        queueEntries.get(2).release();
        queueEntries.get(0).release();

        Thread.sleep(_queueRunnerWaitTime); // Work done by QueueRunner Thread

        assertEquals("Unexpected total number of messages sent to consumer",
                     5,
                     _consumerTarget.getMessages().size());
        assertTrue("Redelivery flag should now be set", queueEntries.get(0).isRedelivered());
        assertFalse("Redelivery flag should remain be unset", queueEntries.get(1).isRedelivered());
        assertTrue("Redelivery flag should now be set",queueEntries.get(2).isRedelivered());
        assertNull("releasedEntry should be cleared after requeue processed",
                   _consumer.getQueueContext().getReleasedEntry());
    }


    /**
     * Tests that a release requeues an entry for a queue with multiple consumers.  Verifies that a
     * requeue resends a message to a <i>single</i> subscriber.
     */
    public void testReleaseForQueueWithMultipleConsumers() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));
        ServerMessage messageB = createMessage(new Long(25));

        MockConsumer target1 = new MockConsumer();
        MockConsumer target2 = new MockConsumer();


        QueueConsumer consumer1 = (QueueConsumer) _queue.addConsumer(target1, null, messageA.getClass(), "test",
                                                                     EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                                    ConsumerImpl.Option.SEES_REQUEUES), 0);

        QueueConsumer consumer2 = (QueueConsumer) _queue.addConsumer(target2, null, messageA.getClass(), "test",
                                                                     EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                                    ConsumerImpl.Option.SEES_REQUEUES), 0);


        final ArrayList<QueueEntry> queueEntries = new ArrayList<QueueEntry>();
        EntryListAddingAction postEnqueueAction = new EntryListAddingAction(queueEntries);

        /* Enqueue two messages */

        _queue.enqueue(messageA, postEnqueueAction, null);
        _queue.enqueue(messageB, postEnqueueAction, null);

        Thread.sleep(_queueRunnerWaitTime);  // Work done by QueueRunner Thread

        assertEquals("Unexpected total number of messages sent to both after enqueue",
                     2,
                     target1.getMessages().size() + target2.getMessages().size());

        /* Now release the first message only, causing it to be requeued */
        queueEntries.get(0).release();

        Thread.sleep(_queueRunnerWaitTime); // Work done by QueueRunner Thread

        assertEquals("Unexpected total number of messages sent to both consumers after release",
                     3,
                     target1.getMessages().size() + target2.getMessages().size());
        assertNull("releasedEntry should be cleared after requeue processed",
                   consumer1.getQueueContext().getReleasedEntry());
        assertNull("releasedEntry should be cleared after requeue processed",
                   consumer2.getQueueContext().getReleasedEntry());
    }

    public void testExclusiveConsumer() throws Exception
    {
        ServerMessage messageA = createMessage(new Long(24));
        // Check adding an exclusive consumer adds it to the queue

        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.EXCLUSIVE, ConsumerImpl.Option.ACQUIRES,
                                                      ConsumerImpl.Option.SEES_REQUEUES), 0);

        assertEquals("Queue does not have consumer", 1,
                     _queue.getConsumerCount());
        assertEquals("Queue does not have active consumer", 1,
                     _queue.getConsumerCountWithCredit());

        // Check sending a message ends up with the subscriber
        _queue.enqueue(messageA, null, null);

        final long timeout = System.currentTimeMillis() + _queueRunnerWaitTime;

        QueueEntry lastSeen = null;
        while (timeout > System.currentTimeMillis() &&
               ((lastSeen = _consumer.getQueueContext().getLastSeenEntry()) == null || lastSeen.getMessage() == null))
        {
            Thread.sleep(10);
        }

        assertEquals("Queue context did not see expected message within timeout",
                     messageA, _consumer.getQueueContext().getLastSeenEntry().getMessage());

        // Check we cannot add a second subscriber to the queue
        MockConsumer subB = new MockConsumer();
        Exception ex = null;
        try
        {

            _queue.addConsumer(subB, null, messageA.getClass(), "test",
                               EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                          ConsumerImpl.Option.SEES_REQUEUES), 0);

        }
        catch (MessageSource.ExistingExclusiveConsumer e)
        {
           ex = e;
        }
        assertNotNull(ex);

        // Check we cannot add an exclusive subscriber to a queue with an
        // existing consumer
        _consumer.close();
        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, messageA.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES,
                                                  ConsumerImpl.Option.SEES_REQUEUES), 0);

        try
        {

            _consumer = (QueueConsumer<?>) _queue.addConsumer(subB, null, messageA.getClass(), "test",
                                                              EnumSet.of(ConsumerImpl.Option.EXCLUSIVE), 0);

        }
        catch (MessageSource.ExistingConsumerPreventsExclusive e)
        {
           ex = e;
        }
        assertNotNull(ex);
    }


    public void testResend() throws Exception
    {
        Long id = new Long(26);
        ServerMessage message = createMessage(id);

        _consumer = (QueueConsumer<?>) _queue.addConsumer(_consumerTarget, null, message.getClass(), "test",
                                                          EnumSet.of(ConsumerImpl.Option.ACQUIRES, ConsumerImpl.Option.SEES_REQUEUES),
                                                          0);

        _queue.enqueue(message, new Action<MessageInstance>()
        {
            @Override
            public void performAction(final MessageInstance object)
            {
                QueueEntryImpl entry = (QueueEntryImpl) object;
                entry.setRedelivered();
                _consumer.resend(entry);

            }
        }, null);



    }

    public void testGetFirstMessageId() throws Exception
    {
        // Create message
        Long messageId = new Long(23);
        ServerMessage message = createMessage(messageId);

        // Put message on queue
        _queue.enqueue(message, null, null);
        // Get message id
        Long testmsgid = _queue.getMessagesOnTheQueue(1).get(0);

        // Check message id
        assertEquals("Message ID was wrong", messageId, testmsgid);
    }

    public void testGetFirstFiveMessageIds() throws Exception
    {
        for (int i = 0 ; i < 5; i++)
        {
            // Create message
            Long messageId = new Long(i);
            ServerMessage message = createMessage(messageId);
            // Put message on queue
            _queue.enqueue(message, null, null);
        }
        // Get message ids
        List<Long> msgids = _queue.getMessagesOnTheQueue(5);

        // Check message id
        for (int i = 0; i < 5; i++)
        {
            Long messageId = new Long(i);
            assertEquals("Message ID was wrong", messageId, msgids.get(i));
        }
    }

    public void testGetLastFiveMessageIds() throws Exception
    {
        for (int i = 0 ; i < 10; i++)
        {
            // Create message
            Long messageId = new Long(i);
            ServerMessage message = createMessage(messageId);
            // Put message on queue
            _queue.enqueue(message, null, null);
        }
        // Get message ids
        List<Long> msgids = _queue.getMessagesOnTheQueue(5, 5);

        // Check message id
        for (int i = 0; i < 5; i++)
        {
            Long messageId = new Long(i+5);
            assertEquals("Message ID was wrong", messageId, msgids.get(i));
        }
    }

    public void testGetMessagesRangeOnTheQueue() throws Exception
    {
        for (int i = 1 ; i <= 10; i++)
        {
            // Create message
            Long messageId = new Long(i);
            ServerMessage message = createMessage(messageId);
            // Put message on queue
            _queue.enqueue(message, null, null);
        }

        // Get non-existent 0th QueueEntry & check returned list was empty
        // (the position parameters in this method are indexed from 1)
        List<? extends QueueEntry> entries = _queue.getMessagesRangeOnTheQueue(0, 0);
        assertTrue(entries.size() == 0);

        // Check that when 'from' is 0 it is ignored and the range continues from 1
        entries = _queue.getMessagesRangeOnTheQueue(0, 2);
        assertTrue(entries.size() == 2);
        long msgID = entries.get(0).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 1L);
        msgID = entries.get(1).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 2L);

        // Check that when 'from' is greater than 'to' the returned list is empty
        entries = _queue.getMessagesRangeOnTheQueue(5, 4);
        assertTrue(entries.size() == 0);

        // Get first QueueEntry & check id
        entries = _queue.getMessagesRangeOnTheQueue(1, 1);
        assertTrue(entries.size() == 1);
        msgID = entries.get(0).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 1L);

        // Get 5th,6th,7th entries and check id's
        entries = _queue.getMessagesRangeOnTheQueue(5, 7);
        assertTrue(entries.size() == 3);
        msgID = entries.get(0).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 5L);
        msgID = entries.get(1).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 6L);
        msgID = entries.get(2).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 7L);

        // Get 10th QueueEntry & check id
        entries = _queue.getMessagesRangeOnTheQueue(10, 10);
        assertTrue(entries.size() == 1);
        msgID = entries.get(0).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 10L);

        // Get non-existent 11th QueueEntry & check returned set was empty
        entries = _queue.getMessagesRangeOnTheQueue(11, 11);
        assertTrue(entries.size() == 0);

        // Get 9th,10th, and non-existent 11th entries & check result is of size 2 with correct IDs
        entries = _queue.getMessagesRangeOnTheQueue(9, 11);
        assertTrue(entries.size() == 2);
        msgID = entries.get(0).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 9L);
        msgID = entries.get(1).getMessage().getMessageNumber();
        assertEquals("Message ID was wrong", msgID, 10L);
    }

    /**
     * Tests that dequeued message is not present in the list returned form
     * {@link AbstractQueue#getMessagesOnTheQueue()}
     */
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
        assertEquals(messageNumber - 1, entries.size());
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
                    new Long(expectedId), id);
            expectedId++;
        }
    }

    /**
     * Tests that dequeued message is not present in the list returned form
     * {@link AbstractQueue#getMessagesOnTheQueue(QueueEntryFilter)}
     */
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
            public boolean accept(QueueEntry entry)
            {
                return true;
            }

            public boolean filterComplete()
            {
                return false;
            }
        });

        // assert entries on the queue
        assertEquals(messageNumber - 1, entries.size());
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
                    new Long(expectedId), id);
            expectedId++;
        }
    }

    /**
     * Tests that all messages including dequeued one are deleted from the queue
     * on invocation of {@link AbstractQueue#clearQueue()}
     */
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
        assertEquals(0, entries.size());
    }


    public void testNotificationFiredOnEnqueue() throws Exception
    {
        QueueNotificationListener listener = mock(QueueNotificationListener .class);

        _queue.setNotificationListener(listener);
        _queue.setAttributes(Collections.<String, Object>singletonMap(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES,
                                                                      Integer.valueOf(2)));

        _queue.enqueue(createMessage(new Long(24)), null, null);
        verifyZeroInteractions(listener);

        _queue.enqueue(createMessage(new Long(25)), null, null);

        verify(listener, atLeastOnce()).notifyClients(eq(NotificationCheck.MESSAGE_COUNT_ALERT), eq(_queue), contains("Maximum count on queue threshold"));
    }

    public void testNotificationFiredAsync() throws Exception
    {
        QueueNotificationListener  listener = mock(QueueNotificationListener .class);

        _queue.enqueue(createMessage(new Long(24)), null, null);
        _queue.enqueue(createMessage(new Long(25)), null, null);
        _queue.enqueue(createMessage(new Long(26)), null, null);

        _queue.setNotificationListener(listener);
        _queue.setAttributes(Collections.<String, Object>singletonMap(Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES,
                                                                      Integer.valueOf(2)));

        verifyZeroInteractions(listener);

        _queue.checkMessageStatus();

        verify(listener, atLeastOnce()).notifyClients(eq(NotificationCheck.MESSAGE_COUNT_ALERT), eq(_queue), contains("Maximum count on queue threshold"));
    }


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

        assertTrue("TTL has not been overridden", tooLateExpiration != getExpirationOnQueue(queue, 0l, tooLateExpiration));

        long acceptableExpiration = System.currentTimeMillis() + 5000l;

        assertEquals("TTL has been incorrectly overriden", acceptableExpiration, getExpirationOnQueue(queue, 0l, acceptableExpiration));

        // Test the scenarios where only the minimum TTL has been set

        attributes = new HashMap<>(_arguments);
        attributes.put(Queue.NAME,"testTtlOverrideMinimumTTl");
        attributes.put(Queue.MINIMUM_MESSAGE_TTL, 10000l);

        queue = _virtualHost.createChild(Queue.class, attributes);

        assertEquals("TTL has been overridden incorrectly", 0l, getExpirationOnQueue(queue, 50000l, 0l));

        assertEquals("TTL has been overridden incorrectly", 65000l, getExpirationOnQueue(queue, 50000l, 65000l));

        assertEquals("TTL has not been overriden", 60000l, getExpirationOnQueue(queue, 50000l, 55000l));

        long unacceptableExpiration = System.currentTimeMillis() + 5000l;

        assertTrue("TTL has not been overridden", unacceptableExpiration != getExpirationOnQueue(queue, 0l, tooLateExpiration));

        acceptableExpiration = System.currentTimeMillis() + 20000l;

        assertEquals("TTL has been incorrectly overridden", acceptableExpiration, getExpirationOnQueue(queue, 0l, acceptableExpiration));


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

    public void testOldestMessage()
    {
        Queue<?> queue = getQueue();
        queue.enqueue(createMessage(1l, (byte)1, Collections.singletonMap("sortKey", (Object) "Z"), 10l), null, null);
        queue.enqueue(createMessage(2l, (byte)4, Collections.singletonMap("sortKey", (Object) "M"), 100l), null, null);
        queue.enqueue(createMessage(3l, (byte)9, Collections.singletonMap("sortKey", (Object) "A"), 1000l), null, null);

        assertEquals(10l,queue.getOldestMessageArrivalTime());
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
        assertEquals("Expected only one entry in the queue", 1, entries.size());

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
        assertEquals(messageNumber, entries.size());
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
        try
        {
            Thread.sleep(2000L);
        }
        catch (InterruptedException e)
        {
            _logger.error("Thread interrupted", e);
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
                    expected.size(), delivered.size());

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

    public MockConsumer getConsumer()
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

    protected ServerMessage createMessage(Long id)
    {
        AMQMessageHeader header = mock(AMQMessageHeader.class);
        when(header.getMessageId()).thenReturn(String.valueOf(id));
        ServerMessage message = mock(ServerMessage.class);
        when(message.getMessageNumber()).thenReturn(id);
        when(message.getMessageHeader()).thenReturn(header);

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

    public MockConsumer getConsumerTarget()
    {
        return _consumerTarget;
    }

    public long getQueueRunnerWaitTime()
    {
        return _queueRunnerWaitTime;
    }

}
