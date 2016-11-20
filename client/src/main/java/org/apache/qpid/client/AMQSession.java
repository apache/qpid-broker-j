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
package org.apache.qpid.client;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.*;
import javax.jms.IllegalStateException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.AMQChannelClosedException;
import org.apache.qpid.AMQDisconnectedException;
import org.apache.qpid.AMQException;
import org.apache.qpid.AMQInvalidArgumentException;
import org.apache.qpid.AMQInvalidRoutingKeyException;
import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQDestination.DestSyntax;
import org.apache.qpid.client.failover.FailoverException;
import org.apache.qpid.client.failover.FailoverNoopSupport;
import org.apache.qpid.client.failover.FailoverProtectedOperation;
import org.apache.qpid.client.failover.FailoverRetrySupport;
import org.apache.qpid.client.message.AMQMessageDelegateFactory;
import org.apache.qpid.client.message.AMQPEncodedListMessage;
import org.apache.qpid.client.message.AMQPEncodedMapMessage;
import org.apache.qpid.client.message.AbstractJMSMessage;
import org.apache.qpid.client.message.CloseConsumerMessage;
import org.apache.qpid.client.message.JMSBytesMessage;
import org.apache.qpid.client.message.JMSMapMessage;
import org.apache.qpid.client.message.JMSObjectMessage;
import org.apache.qpid.client.message.JMSStreamMessage;
import org.apache.qpid.client.message.JMSTextMessage;
import org.apache.qpid.client.message.MessageEncryptionHelper;
import org.apache.qpid.client.message.MessageFactoryRegistry;
import org.apache.qpid.client.message.UnprocessedMessage;
import org.apache.qpid.client.messaging.address.Link;
import org.apache.qpid.client.messaging.address.Node;
import org.apache.qpid.client.util.FlowControllingBlockingQueue;
import org.apache.qpid.client.util.JMSExceptionHelper;
import org.apache.qpid.common.AMQPFilterTypes;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.jms.ListMessage;
import org.apache.qpid.jms.Session;
import org.apache.qpid.protocol.ErrorCodes;
import org.apache.qpid.thread.Threading;
import org.apache.qpid.transport.SessionException;
import org.apache.qpid.transport.TransportException;
import org.apache.qpid.util.Strings;

/*
 * TODO  Different FailoverSupport implementation are needed on the same method call, in different situations. For
 * example, when failing-over and reestablishing the bindings, the bind cannot be interrupted by a second
 * fail-over, if it fails with an exception, the fail-over process should also fail. When binding outside of
 * the fail-over process, the retry handler could be used to automatically retry the operation once the connection
 * has been reestablished. All fail-over protected operations should be placed in private methods, with
 * FailoverSupport passed in by the caller to provide the correct support for the calling context. Sometimes the
 * fail-over process sets a nowait flag and uses an async method call instead.
 * TODO  Two new objects created on every failover supported method call. Consider more efficient ways of doing this,
 * after looking at worse bottlenecks first.
 */
public abstract class AMQSession<C extends BasicMessageConsumer, P extends BasicMessageProducer> extends Closeable implements Session, QueueSession, TopicSession
{
    /** Used for debugging. */
    private static final Logger _logger = LoggerFactory.getLogger(AMQSession.class);

    /** System property to configure dispatcher shutdown timeout in milliseconds. */
    public static final String DISPATCHER_SHUTDOWN_TIMEOUT_MS = "DISPATCHER_SHUTDOWN_TIMEOUT_MS";
    /** Dispatcher shutdown timeout default setting. */
    public static final String DISPATCHER_SHUTDOWN_TIMEOUT_MS_DEFAULT = "1000";

    /** System property to enable strict AMQP compliance. */
    public static final String STRICT_AMQP = "STRICT_AMQP";

    /** Strict AMQP default setting. */
    public static final String STRICT_AMQP_DEFAULT = "false";

    /** System property to enable failure if strict AMQP compliance is violated. */
    public static final String STRICT_AMQP_FATAL = "STRICT_AMQP_FATAL";

    /** Strict AMQP failure default. */
    public static final String STRICT_AMQP_FATAL_DEFAULT = "true";

    /** System property to enable immediate message prefetching. */
    public static final String IMMEDIATE_PREFETCH = "IMMEDIATE_PREFETCH";

    /** Immediate message prefetch default. */
    public static final String IMMEDIATE_PREFETCH_DEFAULT = "false";

    private final boolean _declareQueues =
        Boolean.parseBoolean(System.getProperty(ClientProperties.QPID_DECLARE_QUEUES_PROP_NAME, "true"));

    private final boolean _declareExchanges =
        Boolean.parseBoolean(System.getProperty(ClientProperties.QPID_DECLARE_EXCHANGES_PROP_NAME, "true"));

    private final boolean _bindQueues =
            Boolean.parseBoolean(System.getProperty(ClientProperties.QPID_BIND_QUEUES_PROP_NAME, "true"));

    private final boolean _useAMQPEncodedMapMessage;

    private final boolean _useAMQPEncodedStreamMessage;

    /**
     * Flag indicating to start dispatcher as a daemon thread
     */
    protected final boolean DAEMON_DISPATCHER_THREAD = Boolean.getBoolean(ClientProperties.DAEMON_DISPATCHER);

    private final Set<AMQDestination>
            _resolvedDestinations = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<AMQDestination, Boolean>()));

    private final long _dispatcherShutdownTimeoutMs;

    /** The connection to which this session belongs. */
    private AMQConnection _connection;

    /** Used to indicate whether or not this is a transactional session. */
    private final boolean _transacted;

    /** Holds the sessions acknowledgement mode. */
    private final int _acknowledgeMode;

    /** Holds this session unique identifier, used to distinguish it from other sessions. */
    private int _channelId;

    private int _ticket;

    /** Holds the high mark for prefetched message, at which the session is suspended. */
    private final int _prefetchHighMark;

    /** Holds the low mark for prefetched messages, below which the session is resumed. */
    private final int _prefetchLowMark;

    /** Holds the message listener, if any, which is attached to this session. */
    private MessageListener _messageListener = null;

    /** Used to indicate that this session has been started at least once. */
    private AtomicBoolean _startedAtLeastOnce = new AtomicBoolean(false);

    private final ConcurrentMap<String, TopicSubscriberAdaptor<C>> _subscriptions =
            new ConcurrentHashMap<String, TopicSubscriberAdaptor<C>>();

    private final ConcurrentMap<C, String> _reverseSubscriptionMap = new ConcurrentHashMap<C, String>();

    private final Lock _subscriberDetails = new ReentrantLock(true);
    private final Lock _subscriberAccess = new ReentrantLock(true);

    private final FlowControllingBlockingQueue<Dispatchable> _queue;

    private final AtomicLong _highestDeliveryTag = new AtomicLong(-1);
    private final AtomicLong _rollbackMark = new AtomicLong(-1);

    private ConcurrentLinkedQueue<Long> _prefetchedMessageTags = new ConcurrentLinkedQueue<Long>();

    private ConcurrentLinkedQueue<Long> _unacknowledgedMessageTags = new ConcurrentLinkedQueue<Long>();

    private ConcurrentLinkedQueue<Long> _deliveredMessageTags = new ConcurrentLinkedQueue<Long>();

    private volatile Dispatcher _dispatcher;

    private volatile Thread _dispatcherThread;

    private MessageFactoryRegistry _messageFactoryRegistry;

    /** Holds all of the producers created by this session, keyed by their unique identifiers. */
    private final Map<Long, MessageProducer> _producers = new ConcurrentHashMap<Long, MessageProducer>();

    /**
     * Used as a source of unique identifiers so that the consumers can be tagged to match them to BasicConsume
     * methods.
     */
    private int _nextTag = 1;

    private final Map<String,C> _consumers = new ConcurrentHashMap<>();

    /** Provides a count of consumers on destinations, in order to be able to know if a destination has consumers. */
    private ConcurrentMap<Destination, AtomicInteger> _destinationConsumerCount =
            new ConcurrentHashMap<Destination, AtomicInteger>();

    /**
     * Used as a source of unique identifiers for producers within the session.
     *
     * <p/> Access to this id does not require to be synchronized since according to the JMS specification only one
     * thread of control is allowed to create producers for any given session instance.
     */
    private long _nextProducerId;

    /**
     * Set when recover is called. This is to handle the case where recover() is called by application code during
     * onMessage() processing to ensure that an auto ack is not sent.
     */
    private volatile boolean _sessionInRecovery;

    private volatile boolean _usingDispatcherForCleanup;

    /** Used to indicates that the connection to which this session belongs, has been stopped. */
    private final AtomicBoolean _connectionStopped = new AtomicBoolean();

    /** Used to indicate that this session has a message listener attached to it. */
    private boolean _hasMessageListeners;

    /** Used to indicate that this session has been suspended. */
    private boolean _suspended;

    /**
     * Used to protect the suspension of this session, so that critical code can be executed during suspension,
     * without the session being resumed by other threads.
     */
    private final Object _suspensionLock = new Object();

    private final AtomicBoolean _firstDispatcher = new AtomicBoolean(true);

    private final boolean _immediatePrefetch;

    private final boolean _strictAMQP;

    private final boolean _strictAMQPFATAL;
    private final Lock _messageDeliveryLock = new ReentrantLock(true);

    /** Session state : used to detect if commit is a) required b) allowed , i.e. does the tx span failover. */
    private boolean _dirty;
    /** Has failover occured on this session with outstanding actions to commit? */
    private boolean _failedOverDirty;

    private MessageEncryptionHelper _messageEncryptionHelper;

    /** Holds the highest received delivery tag. */
    protected AtomicLong getHighestDeliveryTag()
    {
        return _highestDeliveryTag;
    }

    /** Pre-fetched message tags */
    protected ConcurrentLinkedQueue<Long> getPrefetchedMessageTags()
    {
        return _prefetchedMessageTags;
    }

    /** All the not yet acknowledged message tags */
    protected ConcurrentLinkedQueue<Long> getUnacknowledgedMessageTags()
    {
        return _unacknowledgedMessageTags;
    }

    /** All the delivered message tags */
    protected ConcurrentLinkedQueue<Long> getDeliveredMessageTags()
    {
        return _deliveredMessageTags;
    }

    /** Holds the dispatcher thread for this session. */
    protected Dispatcher getDispatcher()
    {
        return _dispatcher;
    }

    protected Thread getDispatcherThread()
    {
        return _dispatcherThread;
    }

    /** Holds the message factory factory for this session. */
    protected MessageFactoryRegistry getMessageFactoryRegistry()
    {
        return _messageFactoryRegistry;
    }

    private final ExecutorService _flowControlNoAckTaskPool;

    /**
     * Consumers associated with this session
     */
    protected Collection<C> getConsumers()
    {
        return new ArrayList<>(_consumers.values());
    }

    protected void setUsingDispatcherForCleanup(boolean usingDispatcherForCleanup)
    {
        _usingDispatcherForCleanup = usingDispatcherForCleanup;
    }

    /** Used to indicate that the session should start pre-fetching messages as soon as it is started. */
    protected boolean isImmediatePrefetch()
    {
        return _immediatePrefetch;
    }

    abstract void handleNodeDelete(final AMQDestination dest) throws QpidException;

    abstract void handleLinkDelete(final AMQDestination dest) throws QpidException;

    /**
     * Creates a new session on a connection.
     * @param con                     The connection on which to create the session.
     * @param channelId               The unique identifier for the session.
     * @param transacted              Indicates whether or not the session is transactional.
     * @param acknowledgeMode         The acknowledgement mode for the session.
     * @param defaultPrefetchHighMark The maximum number of messages to prefetched before suspending the session.
     * @param defaultPrefetchLowMark  The number of prefetched messages at which to resume the session.
     */
    protected AMQSession(AMQConnection con,
                         int channelId,
                         boolean transacted,
                         int acknowledgeMode,
                         int defaultPrefetchHighMark,
                         int defaultPrefetchLowMark)
    {
        _useAMQPEncodedMapMessage = con == null || !con.isUseLegacyMapMessageFormat();
        _useAMQPEncodedStreamMessage = con != null && !con.isUseLegacyStreamMessageFormat();
        _strictAMQP = Boolean.parseBoolean(System.getProperties().getProperty(STRICT_AMQP, STRICT_AMQP_DEFAULT));
        _strictAMQPFATAL =
                Boolean.parseBoolean(System.getProperties().getProperty(STRICT_AMQP_FATAL, STRICT_AMQP_FATAL_DEFAULT));
        _immediatePrefetch =
                _strictAMQP
                || Boolean.parseBoolean(System.getProperties().getProperty(IMMEDIATE_PREFETCH, IMMEDIATE_PREFETCH_DEFAULT));
        _dispatcherShutdownTimeoutMs = Integer.parseInt(System.getProperty(DISPATCHER_SHUTDOWN_TIMEOUT_MS, DISPATCHER_SHUTDOWN_TIMEOUT_MS_DEFAULT));

        _connection = con;
        _transacted = transacted;
        if (transacted)
        {
            _acknowledgeMode = javax.jms.Session.SESSION_TRANSACTED;
        }
        else
        {
            _acknowledgeMode = acknowledgeMode;
        }
        _messageEncryptionHelper = new MessageEncryptionHelper(this);
        _channelId = channelId;
        _messageFactoryRegistry = MessageFactoryRegistry.newDefaultRegistry(this);

        if (_acknowledgeMode == NO_ACKNOWLEDGE)
        {
            _prefetchHighMark = defaultPrefetchHighMark;
            _prefetchLowMark = defaultPrefetchLowMark == defaultPrefetchHighMark && defaultPrefetchHighMark > 0
                   ? Math.max(defaultPrefetchHighMark / 2, 1)
                    : defaultPrefetchLowMark;

            // we coalesce suspend jobs using single threaded pool executor with queue length of one
            // and discarding policy
            _flowControlNoAckTaskPool = new ThreadPoolExecutor(1, 1,
                                                               0L, TimeUnit.MILLISECONDS,
                                                               new LinkedBlockingQueue<Runnable>(1),
                                                               new ThreadFactory()
            {
                @Override
                public Thread newThread(final Runnable r)
                {
                    Thread thread = new Thread(r, "Connection_" + _connection.getConnectionNumber() + "_session_" + _channelId);
                    if (!thread.isDaemon())
                    {
                        thread.setDaemon(true);
                    }

                    return thread;
                }
            }, new ThreadPoolExecutor.DiscardPolicy());

            final FlowControllingBlockingQueue.ThresholdListener listener =
                    new FlowControllingBlockingQueue.ThresholdListener()
                    {
                        private final AtomicBoolean _suspendState = new AtomicBoolean();

                        public void aboveThreshold(int currentValue)
                        {
                            if (!(AMQSession.this.isClosed() || AMQSession.this.isClosing()))
                            {
                                // Only execute change if previous state was false
                                if (!_suspendState.getAndSet(true))
                                {
                                    _logger.debug(
                                            "Above threshold ({}) so suspending channel. Current value is {}",
                                            _prefetchHighMark,
                                            currentValue);

                                    doSuspend();
                                }
                            }
                        }

                        public void underThreshold(int currentValue)
                        {
                            if (!(AMQSession.this.isClosed() || AMQSession.this.isClosing()))
                            {
                                // Only execute change if previous state was true
                                if (_suspendState.getAndSet(false))
                                {
                                    _logger.debug(
                                            "Below threshold ({}) so unsuspending channel. Current value is {}",
                                            _prefetchLowMark,
                                            currentValue);
                                    doSuspend();
                                }
                            }
                        }

                        private void doSuspend()
                        {
                            _flowControlNoAckTaskPool.execute(new SuspenderRunner(_suspendState));
                        }
                    };
            _queue = new FlowControllingBlockingQueue<>(_prefetchHighMark, _prefetchLowMark, listener);
        }
        else
        {
            _prefetchHighMark = defaultPrefetchHighMark;
            _prefetchLowMark = defaultPrefetchLowMark;
            _flowControlNoAckTaskPool = null;
            _queue = new FlowControllingBlockingQueue<>(_prefetchHighMark, null);
        }

        // Add creation logging to tie in with the existing close logging
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Created session:" + this);
        }
    }

    // ===== JMS Session methods.

    /**
     * Closes the session with no timeout.
     *
     * @throws JMSException If the JMS provider fails to close the session due to some internal error.
     */
    @Override
    public void close() throws JMSException
    {
        close(-1);
    }

    public abstract QpidException getLastException();

    public void checkNotClosed() throws JMSException
    {
        try
        {
            super.checkNotClosed();
        }
        catch (IllegalStateException ise)
        {
            QpidException ex = getLastException();
            if (ex != null)
            {
                int code = 0;

                if (ex instanceof AMQException)
                {
                    code = ((AMQException) ex).getErrorCode();
                }

                throw JMSExceptionHelper.chainJMSException(new IllegalStateException("Session has been closed",
                                                                                     code == 0 ? null : Integer.toString(code)), ex);
            }
            else
            {
                throw ise;
            }
        }
    }

    public BytesMessage createBytesMessage() throws JMSException
    {
        checkNotClosed();
        JMSBytesMessage msg = new JMSBytesMessage(getMessageDelegateFactory());
        msg.setAMQSession(this);
        return msg;
    }

    /**
     * Acknowledges all unacknowledged messages on the session, for all message consumers on the session.
     *
     * @throws IllegalStateException If the session is closed.
     * @throws JMSException if there is a problem during acknowledge process.
     */
    public void acknowledge() throws IllegalStateException, JMSException
    {
        if (isClosed())
        {
            throw new IllegalStateException("Session is already closed");
        }
        else if (hasFailedOverDirty())
        {
            //perform an implicit recover in this scenario
            recover();

            //notify the consumer
            throw new IllegalStateException("has failed over");
        }

        try
        {
            acknowledgeImpl();
            markClean();
        }
        catch (TransportException e)
        {
            throw toJMSException("Exception while acknowledging message(s):" + e.getMessage(), e);
        }
    }

    public void setLegacyFieldsForQueueType(AMQDestination dest)
    {
        // legacy support
        dest.setQueueName(dest.getAddressName());
        dest.setExchangeName("");
        dest.setExchangeClass("");
        dest.setRoutingKey(dest.getAMQQueueName());
    }

    public void setLegacyFieldsForTopicType(AMQDestination dest)
    {
        // legacy support
        dest.setExchangeName(dest.getAddressName());
        Node node = dest.getNode();
        dest.setExchangeClass(node.getExchangeType() == null
                                      ? ExchangeDefaults.TOPIC_EXCHANGE_CLASS
                                      : node.getExchangeType());
        dest.setRoutingKey(dest.getSubject());
    }

    protected void verifySubject(AMQDestination dest) throws QpidException
    {
        if (dest.getSubject() == null || dest.getSubject().trim().equals(""))
        {

            if ("topic".equals(dest.getExchangeClass()))
            {
                dest.setRoutingKey("#");
                dest.setSubject(dest.getRoutingKey());
            }
            else
            {
                dest.setRoutingKey("");
                dest.setSubject("");
            }
        }
    }

    public abstract boolean isExchangeExist(AMQDestination dest, boolean assertNode) throws QpidException;

    public abstract boolean isQueueExist(AMQDestination dest, boolean assertNode) throws QpidException;

    /**
     * 1. Try to resolve the address type (queue or exchange)
     * 2. if type == queue,
     *       2.1 verify queue exists or create if create == true
     *       2.2 If not throw exception
     *
     * 3. if type == exchange,
     *       3.1 verify exchange exists or create if create == true
     *       3.2 if not throw exception
     *       3.3 if exchange exists (or created) create subscription queue.
     */

    @SuppressWarnings("deprecation")
    public void resolveAddress(AMQDestination dest,
                                              boolean isConsumer,
                                              boolean noLocal) throws QpidException
    {
        if (isResolved(dest))
        {
            return;
        }
        else
        {
            boolean assertNode = (dest.getAssert() == AMQDestination.AddressOption.ALWAYS) ||
                                 (isConsumer && dest.getAssert() == AMQDestination.AddressOption.RECEIVER) ||
                                 (!isConsumer && dest.getAssert() == AMQDestination.AddressOption.SENDER);

            boolean createNode = (dest.getCreate() == AMQDestination.AddressOption.ALWAYS) ||
                                 (isConsumer && dest.getCreate() == AMQDestination.AddressOption.RECEIVER) ||
                                 (!isConsumer && dest.getCreate() == AMQDestination.AddressOption.SENDER);


            int suppliedType = dest.getNode() == null ? AMQDestination.UNKNOWN_TYPE : dest.getNode().getType();
            int type = resolveAddressType(dest);
            switch (type)
            {
                case AMQDestination.QUEUE_TYPE:

                    setLegacyFieldsForQueueType(dest);
                    if(createNode)
                    {
                        handleQueueNodeCreation(dest,noLocal);
                        break;
                    }
                    else if (isQueueExist(dest,assertNode) || suppliedType == AMQDestination.QUEUE_TYPE)
                    {
                        break;
                    }
                case AMQDestination.TOPIC_TYPE:
                    if(suppliedType != AMQDestination.QUEUE_TYPE)
                    {
                        setLegacyFieldsForTopicType(dest);
                        if (createNode)
                        {
                            verifySubject(dest);
                            handleExchangeNodeCreation(dest);
                            break;
                        }
                        else if (isExchangeExist(dest, assertNode) || suppliedType == AMQDestination.TOPIC_TYPE)
                        {
                            verifySubject(dest);
                            break;
                        }
                    }
                default:
                    throw new QpidException(
                            "The name '" + dest.getAddressName() +
                            "' supplied in the address doesn't resolve to an exchange or a queue");
            }
            setResolved(dest);
        }
    }

    void setResolved(final AMQDestination dest)
    {
        _resolvedDestinations.add(dest);
    }

    void setUnresolved(final AMQDestination dest)
    {
        _resolvedDestinations.remove(dest);
    }

    private void clearResolvedDestinations()
    {
        _resolvedDestinations.clear();
    }

    boolean isResolved(final AMQDestination dest)
    {
        return _resolvedDestinations.contains(dest);
    }

    public abstract int resolveAddressType(AMQDestination dest) throws QpidException;

    protected abstract void acknowledgeImpl() throws JMSException;

    /**
     * Acknowledge one or many messages.
     *
     * @param deliveryTag The tag of the last message to be acknowledged.
     * @param multiple    <tt>true</tt> to acknowledge all messages up to and including the one specified by the
     *                    delivery tag, <tt>false</tt> to just acknowledge that message.
     *
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    public abstract void acknowledgeMessage(long deliveryTag, boolean multiple);

    /**
     * Binds the named queue, with the specified routing key, to the named exchange.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     * @param queueName    The name of the queue to bind.
     * @param routingKey   The routing key to bind the queue with.
     * @param arguments    Additional arguments.
     * @param exchangeName The exchange to bind the queue on.
     *
     * @throws QpidException If the queue cannot be bound for any reason.
     * TODO  Be aware of possible changes to parameter order as versions change.
     * TODO  Document the additional arguments that may be passed in the field table. Are these for headers exchanges?
     */
    public void bindQueue(final String queueName, final String routingKey, final Map<String,Object> arguments,
                          final String exchangeName, final AMQDestination destination) throws QpidException
    {
        bindQueue(queueName, routingKey, arguments, exchangeName, destination, false);
    }

    public void bindQueue(final String queueName, final String routingKey, final Map<String,Object> arguments,
                          final String exchangeName, final AMQDestination destination,
                          final boolean nowait) throws QpidException
    {
        /*new FailoverRetrySupport<Object, AMQException>(new FailoverProtectedOperation<Object, AMQException>()*/
        new FailoverNoopSupport<Object, QpidException>(new FailoverProtectedOperation<Object, QpidException>()
        {
            public Object execute() throws QpidException, FailoverException
            {
                sendQueueBind(queueName, routingKey, arguments, exchangeName, destination, nowait);
                return null;
            }
        }, _connection).execute();
    }

    public void addBindingKey(C consumer, AMQDestination amqd, String routingKey) throws QpidException
    {
        if (consumer.getQueuename() != null)
        {
            bindQueue(consumer.getQueuename(), routingKey, new HashMap<String, Object>(), amqd.getExchangeName(), amqd);
        }
    }

    public abstract void sendQueueBind(final String queueName, final String routingKey, final Map<String,Object> arguments,
                                       final String exchangeName, AMQDestination destination,
                                       final boolean nowait) throws QpidException, FailoverException;

    /**
     * Closes the session.
     * <p>
     * Note that this operation succeeds automatically if a fail-over interrupts the synchronous request to close
     * the channel. This is because the channel is marked as closed before the request to close it is made, so the
     * fail-over should not re-open it.
     *
     * @param timeout The timeout in milliseconds to wait for the session close acknowledgement from the broker.
     *
     * @throws JMSException If the JMS provider fails to close the session due to some internal error.
     * TODO  Be aware of possible changes to parameter order as versions change.
     * TODO Not certain about the logic of ignoring the failover exception, because the channel won't be
     * re-opened. May need to examine this more carefully.
     * TODO  Note that taking the failover mutex doesn't prevent this operation being interrupted by a failover,
     * because the failover process sends the failover event before acquiring the mutex itself.
     */
    public void close(long timeout) throws JMSException
    {
        setClosing(true);
        lockMessageDelivery();
        try
        {
            // We must close down all producers and consumers in an orderly fashion. This is the only method
            // that can be called from a different thread of control from the one controlling the session.
            synchronized (getFailoverMutex())
            {
                close(timeout, true);
            }
        }
        finally
        {
            unlockMessageDelivery();
        }
    }

    private void close(long timeout, boolean sendClose) throws JMSException
    {
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Closing session: " + this);
        }

        // Ensure we only try and close an open session.
        if (!setClosed())
        {
            setClosing(true);
            // we pass null since this is not an error case
            closeProducersAndConsumers(null);

            try
            {
                // If the connection is open or we are in the process
                // of closing the connection then send a cance
                // no point otherwise as the connection will be gone
                if (!_connection.isClosed() || _connection.isClosing())
                {
                    if (sendClose)
                    {
                        sendClose(timeout);
                    }
                }
            }
            catch (QpidException e)
            {
                throw JMSExceptionHelper.chainJMSException(new JMSException("Error closing session: " + e), e);
            }
            // This is ignored because the channel is already marked as closed so the fail-over process will
            // not re-open it.
            catch (FailoverException e)
            {
                _logger.debug(
                        "Got FailoverException during channel close, ignored as channel already marked as closed.");
            }
            catch (TransportException e)
            {
                throw toJMSException("Error closing session:" + e.getMessage(), e);
            }
            finally
            {
                shutdownFlowControlNoAckTaskPool();
                _connection.deregisterSession(_channelId);
            }
        }
    }

    public abstract void sendClose(long timeout) throws QpidException, FailoverException;

    /**
     * Called when the server initiates the closure of the session unilaterally.
     *
     * @param e the exception that caused this session to be closed. Null causes the
     */
    public void closed(Throwable e) throws JMSException
    {
        // This method needs to be improved. Throwables only arrive here from the mina : exceptionRecived
        // calls through connection.closeAllSessions which is also called by the public connection.close()
        // with a null cause
        // When we are closing the Session due to a protocol session error we simply create a new AMQException
        // with the correct error code and text this is cleary WRONG as the instanceof check below will fail.
        // We need to determine here if the connection should be

        if (e instanceof AMQDisconnectedException)
        {
            // Failover failed and ain't coming back. Knife the dispatcher.
            stopDispatcherThread();

       }

        //if we don't have an exception then we can perform closing operations
        setClosing(e == null);

        if (!setClosed())
        {
            // An AMQException has an error code and message already and will be passed in when closure occurs as a
            // result of a channel close request
            QpidException amqe;
            if (e instanceof QpidException)
            {
                amqe = (QpidException) e;
            }
            else
            {
                amqe = new QpidException("Closing session forcibly", e);
            }

            _connection.deregisterSession(_channelId);
            closeProducersAndConsumers(amqe);
            shutdownFlowControlNoAckTaskPool();
        }

    }

    protected void stopDispatcherThread()
    {
        if (_dispatcherThread != null)
        {
            _dispatcherThread.interrupt();
        }
    }
    /**
     * Commits all messages done in this transaction and releases any locks currently held.
     * <p>
     * If the commit fails, because the commit itself is interrupted by a fail-over between requesting that the
     * commit be done, and receiving an acknowledgement that it has been done, then a JMSException will be thrown.
     * The client will be unable to determine whether or not the commit actually happened on the broker in this case.
     *
     * @throws JMSException If the JMS provider fails to commit the transaction due to some internal error. This does
     *                      not mean that the commit is known to have failed, merely that it is not known whether it
     *                      failed or not.
     */
    public void commit() throws JMSException
    {
        checkTransacted();

        //Check that we are clean to commit.
        if (_failedOverDirty)
        {
            if (_logger.isDebugEnabled())
            {
                _logger.debug("Session " + _channelId + " was dirty whilst failing over. Rolling back.");
            }
            rollback();

            throw new TransactionRolledBackException("Connection failover has occured with uncommitted transaction activity." +
                                                     "The session transaction was rolled back.");
        }

        try
        {
            commitImpl();
            markClean();
        }
        catch (QpidException e)
        {
            throw toJMSException("Exception during commit: " + e.getMessage() + (e.getCause() == null ? "" : " (Caused by : " + e.getCause() + ")"), e);
        }
        catch (FailoverException e)
        {
            throw JMSExceptionHelper.chainJMSException(new JMSException(
                    "Fail-over interrupted commit. Status of the commit is uncertain."), e);
        }
        catch(TransportException e)
        {
            throw toJMSException("Session exception occurred while trying to commit: " + e.getMessage(), e);
        }
    }

    protected abstract void commitImpl() throws QpidException, FailoverException, TransportException;




    public void confirmConsumerCancelled(String consumerTag)
    {
        C consumer = _consumers.get(consumerTag);
        if (consumer != null)
        {
            if (!consumer.isBrowseOnly())  // Normal Consumer
            {
                // Clean the Maps up first
                // Flush any pending messages for this consumerTag
                if (_dispatcher != null)
                {
                    _logger.debug("Dispatcher is not null");
                }
                else
                {
                    _logger.debug("Dispatcher is null so created stopped dispatcher");
                    startDispatcherIfNecessary(true);
                }

                rejectPending(consumer);
            }
            else // Queue Browser
            {
                // Just close the consumer
                // fixme  the CancelOK is being processed before the arriving messages..
                // The dispatcher is still to process them so the server sent in order but the client
                // has yet to receive before the close comes in

                if (consumer.isAutoClose())
                {
                    // There is a small window where the message is between the two queues in the dispatcher.
                    if (consumer.isClosed())
                    {
                        if (_logger.isDebugEnabled())
                        {
                            _logger.debug("Closing consumer:" + consumer.debugIdentity());
                        }

                        deregisterConsumer(consumer);
                    }
                    else
                    {
                        _queue.add(new CloseConsumerMessage(consumer));
                    }
                }
            }
        }
    }

    private void rejectPending(C consumer)
    {
        // Reject messages on pre-receive queue
        consumer.releasePendingMessages();

        // Reject messages on pre-dispatch queue
        rejectMessagesForConsumerTag(consumer.getConsumerTag());

        // closeConsumer
        consumer.markClosed();
    }

    public QueueBrowser createBrowser(Queue queue) throws JMSException
    {
        if (isStrictAMQP())
        {
            throw new UnsupportedOperationException();
        }

        return createBrowser(queue, null);
    }

    /**
     * Create a queue browser if the destination is a valid queue.
     */
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException
    {
        if (isStrictAMQP())
        {
            throw new UnsupportedOperationException();
        }

        checkNotClosed();
        checkValidQueue(queue);

        return new AMQQueueBrowser(this, queue, messageSelector);
    }

    protected MessageConsumer createBrowserConsumer(Destination destination, String messageSelector, boolean noLocal)
            throws JMSException
    {
        checkValidDestination(destination);

        return createConsumerImpl(destination, _prefetchHighMark, _prefetchLowMark, noLocal, false,
                                  messageSelector, null, true, true);
    }

    public MessageConsumer createConsumer(Destination destination) throws JMSException
    {
        checkValidDestination(destination);

        return createConsumerImpl(destination, _prefetchHighMark, _prefetchLowMark, false, (destination instanceof Topic), null, null,
                                  isBrowseOnlyDestination(destination), false);
    }

    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException
    {
        checkValidDestination(destination);

        return createConsumerImpl(destination, _prefetchHighMark, _prefetchLowMark, false, (destination instanceof Topic),
                                  messageSelector, null, isBrowseOnlyDestination(destination), false);
    }

    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal)
            throws JMSException
    {
        checkValidDestination(destination);

        return createConsumerImpl(destination, _prefetchHighMark, _prefetchLowMark, noLocal, (destination instanceof Topic),
                                  messageSelector, null, isBrowseOnlyDestination(destination), false);
    }

    public MessageConsumer createConsumer(Destination destination, int prefetch, boolean noLocal, boolean exclusive,
                                          String selector) throws JMSException
    {
        checkValidDestination(destination);

        return createConsumerImpl(destination, prefetch, prefetch / 2, noLocal, exclusive, selector, null, isBrowseOnlyDestination(destination), false);
    }

    public MessageConsumer createConsumer(Destination destination, int prefetchHigh, int prefetchLow, boolean noLocal,
                                              boolean exclusive, String selector) throws JMSException
    {
        checkValidDestination(destination);

        return createConsumerImpl(destination, prefetchHigh, prefetchLow, noLocal, exclusive, selector, null, isBrowseOnlyDestination(destination), false);
    }

    public MessageConsumer createConsumer(Destination destination, int prefetchHigh, int prefetchLow, boolean noLocal,
                                          boolean exclusive, String selector, Map<String,Object> rawSelector) throws JMSException
    {
        checkValidDestination(destination);

        return createConsumerImpl(destination, prefetchHigh, prefetchLow, noLocal, exclusive, selector, rawSelector, isBrowseOnlyDestination(destination),
                                  false);
    }

    public  TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException
    {
        // Delegate the work to the {@link #createDurableSubscriber(Topic, String, String, boolean)} method
        return createDurableSubscriber(topic, name, null, false);
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String selector, boolean noLocal)
            throws JMSException
    {
        checkNotClosed();
        Topic origTopic = checkValidTopic(topic, true);

        AMQTopic dest = AMQTopic.createDurableTopic(origTopic, name, _connection);
        if (dest.getDestSyntax() == DestSyntax.ADDR && !isResolved(dest))
        {
            try
            {
                resolveAddress(dest,false,noLocal);
                if (dest.getAddressType() !=  AMQDestination.TOPIC_TYPE)
                {
                    throw new JMSException("Durable subscribers can only be created for Topics");
                }
            }
            catch(QpidException e)
            {
                throw toJMSException("Error when verifying destination",e);
            }
            catch(TransportException e)
            {
                throw toJMSException("Error when verifying destination", e);
            }
        }

        String messageSelector = ((selector == null) || (selector.trim().length() == 0)) ? null : selector;

        _subscriberDetails.lock();
        try
        {
            TopicSubscriberAdaptor<C> subscriber = _subscriptions.get(name);

            // Not subscribed to this name in the current session
            if (subscriber == null)
            {
                // After the address is resolved routing key will not be null.
                String topicName = dest.getRoutingKey();

                if (_strictAMQP)
                {
                    if (_strictAMQPFATAL)
                    {
                        throw new UnsupportedOperationException("JMS Durable not currently supported by AMQP.");
                    }
                    else
                    {
                        _logger.warn("Unable to determine if subscription already exists for '" + topicName
                                        + "' for creation durableSubscriber. Requesting queue deletion regardless.");
                    }

                    deleteQueue(dest.getAMQQueueName());
                }
                else
                {
                    Map<String,Object> args = new HashMap<String,Object>();

                    // We must always send the selector argument even if empty, so that we can tell when a selector is removed from a
                    // durable topic subscription that the broker arguments don't match any more. This is because it is not otherwise
                    // possible to determine  when querying the broker whether there are no arguments or just a non-matching selector
                    // argument, as specifying null for the arguments when querying means they should not be checked at all
                    args.put(AMQPFilterTypes.JMS_SELECTOR.getValue(), messageSelector == null ? "" : messageSelector);
                    if(noLocal)
                    {
                        args.put(AMQPFilterTypes.NO_LOCAL.getValue(), true);
                    }

                    // if the queue is bound to the exchange but NOT for this topic and selector, then the JMS spec
                    // says we must trash the subscription.
                    boolean isQueueBound = isQueueBound(dest.getExchangeName(), dest.getAMQQueueName());
                    boolean isQueueBoundForTopicAndSelector =
                                isQueueBound(dest.getExchangeName(),
                                             dest.getAMQQueueName(),
                                             topicName, args);

                    if (isQueueBound && !isQueueBoundForTopicAndSelector)
                    {
                        deleteQueue(dest.getAMQQueueName());
                    }
                    else if(isQueueBound) // todo - this is a hack for 0-8/9/9-1 which cannot check if arguments on a binding match
                    {
                        try
                        {
                            bindQueue(dest.getAMQQueueName(),
                                      dest.getRoutingKey(),
                                      args,
                                      dest.getExchangeName(),
                                      dest,
                                      true);
                        }
                        catch(QpidException e)
                        {
                            throw toJMSException("Error when checking binding",e);
                        }
                    }
                }
            }
            else
            {
                // Subscribed with the same topic and no current / previous or same selector
                if (subscriber.getTopic().equals(topic)
                    && ((messageSelector == null && subscriber.getMessageSelector() == null)
                            || (messageSelector != null && messageSelector.equals(subscriber.getMessageSelector()))))
                {
                    throw new IllegalStateException("Already subscribed to topic " + topic + " with subscription name " + name
                            + (messageSelector != null ? " and selector " + messageSelector : ""));
                }
                else
                {
                    unsubscribe(name, true);
                }

            }

            _subscriberAccess.lock();
            try
            {
                C consumer = (C) createConsumer(dest, messageSelector, noLocal);
                consumer.markAsDurableSubscriber();
                subscriber = new TopicSubscriberAdaptor<C>(dest, consumer);

                // Save subscription information
                _subscriptions.put(name, subscriber);
                _reverseSubscriptionMap.put(subscriber.getMessageConsumer(), name);
            }
            finally
            {
                _subscriberAccess.unlock();
            }

            return subscriber;
        }
        catch (TransportException e)
        {
            throw toJMSException("Exception while creating durable subscriber:" + e.getMessage(), e);
        }
        finally
        {
            _subscriberDetails.unlock();
        }
    }

    public ListMessage createListMessage() throws JMSException
    {
        checkNotClosed();
        AMQPEncodedListMessage msg = new AMQPEncodedListMessage(getMessageDelegateFactory());
        msg.setAMQSession(this);
        return msg;
    }

    public MapMessage createMapMessage() throws JMSException
    {
        checkNotClosed();
        if (_useAMQPEncodedMapMessage)
        {
            AMQPEncodedMapMessage msg = new AMQPEncodedMapMessage(getMessageDelegateFactory());
            msg.setAMQSession(this);
            return msg;
        }
        else
        {
            JMSMapMessage msg = new JMSMapMessage(getMessageDelegateFactory());
            msg.setAMQSession(this);
            return msg;
        }
    }

    public javax.jms.Message createMessage() throws JMSException
    {
        return createBytesMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException
    {
        checkNotClosed();
         JMSObjectMessage msg = new JMSObjectMessage(getAMQConnection(), getMessageDelegateFactory());
         msg.setAMQSession(this);
         return msg;
    }

    public ObjectMessage createObjectMessage(Serializable object) throws JMSException
    {
        ObjectMessage msg = createObjectMessage();
        msg.setObject(object);

        return msg;
    }

    public P createProducer(Destination destination) throws JMSException
    {
        return createProducerImpl(destination, null, null);
    }

    public P createProducer(Destination destination, boolean immediate) throws JMSException
    {
        return createProducerImpl(destination, null, immediate);
    }

    public P createProducer(Destination destination, boolean mandatory, boolean immediate)
            throws JMSException
    {
        return createProducerImpl(destination, mandatory, immediate);
    }

    public TopicPublisher createPublisher(Topic topic) throws JMSException
    {
        checkNotClosed();

        return new TopicPublisherAdapter((P) createProducer(topic, false, false), topic);
    }

    public Queue createQueue(String queueName) throws JMSException
    {
        checkNotClosed();
        try
        {
            if (queueName.indexOf('/') == -1 && queueName.indexOf(';') == -1)
            {
                DestSyntax syntax = AMQDestination.getDestType(queueName);
                if (syntax == AMQDestination.DestSyntax.BURL)
                {
                    // For testing we may want to use the prefix
                    return new AMQQueue(getDefaultQueueExchangeName(),
                                        AMQDestination.stripSyntaxPrefix(queueName));
                }
                else
                {
                    AMQQueue queue = new AMQQueue(queueName);
                    return queue;

                }
            }
            else
            {
                return new AMQQueue(queueName);
            }
        }
        catch (URISyntaxException urlse)
        {
            _logger.error("", urlse);
            throw JMSExceptionHelper.chainJMSException(new JMSException(urlse.getReason()), urlse);
        }

    }

    /**
     * Declares the named queue.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     * @param name       The name of the queue to declare.
     * @param autoDelete
     * @param durable    Flag to indicate that the queue is durable.
     * @param exclusive  Flag to indicate that the queue is exclusive to this client.
     *
     * @throws QpidException If the queue cannot be declared for any reason.
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    public void createQueue(final String name, final boolean autoDelete, final boolean durable,
                            final boolean exclusive) throws QpidException
    {
        createQueue(name, autoDelete, durable, exclusive, null);
    }

    /**
     * Declares the named queue.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     * @param name       The name of the queue to declare.
     * @param autoDelete
     * @param durable    Flag to indicate that the queue is durable.
     * @param exclusive  Flag to indicate that the queue is exclusive to this client.
     * @param arguments  Arguments used to set special properties of the queue
     *
     * @throws QpidException If the queue cannot be declared for any reason.
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    public void createQueue(final String name, final boolean autoDelete, final boolean durable,
                            final boolean exclusive, final Map<String, Object> arguments) throws QpidException
    {
        new FailoverRetrySupport<Object, QpidException>(new FailoverProtectedOperation<Object, QpidException>()
        {
            public Object execute() throws QpidException, FailoverException
            {
                sendCreateQueue(name, autoDelete, durable, exclusive, arguments);
                return null;
            }
        }, _connection).execute();
    }

    public abstract void sendCreateQueue(String name, final boolean autoDelete, final boolean durable,
                                         final boolean exclusive, final Map<String, Object> arguments) throws
                                                                                                       QpidException, FailoverException;

    /**
     * Creates a QueueReceiver
     *
     * @param destination
     *
     * @return QueueReceiver - a wrapper around our MessageConsumer
     *
     * @throws JMSException
     */
    public QueueReceiver createQueueReceiver(Destination destination) throws JMSException
    {
        checkValidDestination(destination);
        Queue dest = validateQueue(destination);
        C consumer = (C) createConsumer(dest);
        consumer.setAddressType(AMQDestination.QUEUE_TYPE);
        return new QueueReceiverAdaptor(dest, consumer);
    }

    /**
     * Creates a QueueReceiver using a message selector
     *
     * @param destination
     * @param messageSelector
     *
     * @return QueueReceiver - a wrapper around our MessageConsumer
     *
     * @throws JMSException
     */
    public QueueReceiver createQueueReceiver(Destination destination, String messageSelector) throws JMSException
    {
        checkValidDestination(destination);
        Queue dest = validateQueue(destination);
        C consumer = (C) createConsumer(dest, messageSelector);
        consumer.setAddressType(AMQDestination.QUEUE_TYPE);
        return new QueueReceiverAdaptor(dest, consumer);
    }

    /**
     * Creates a QueueReceiver wrapping a MessageConsumer
     *
     * @param queue
     *
     * @return QueueReceiver
     *
     * @throws JMSException
     */
    public QueueReceiver createReceiver(Queue queue) throws JMSException
    {
        checkNotClosed();
        Queue dest = validateQueue(queue);
        C consumer = (C) createConsumer(dest);
        consumer.setAddressType(AMQDestination.QUEUE_TYPE);
        return new QueueReceiverAdaptor(dest, consumer);
    }

    /**
     * Creates a QueueReceiver wrapping a MessageConsumer using a message selector
     *
     * @param queue
     * @param messageSelector
     *
     * @return QueueReceiver
     *
     * @throws JMSException
     */
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException
    {
        checkNotClosed();
        Queue dest = validateQueue(queue);
        C consumer = (C) createConsumer(dest, messageSelector);
        consumer.setAddressType(AMQDestination.QUEUE_TYPE);
        return new QueueReceiverAdaptor(dest, consumer);
    }

    private Queue validateQueue(Destination dest) throws InvalidDestinationException
    {
        if (dest instanceof AMQDestination && dest instanceof javax.jms.Queue)
        {
            return (Queue)dest;
        }
        else
        {
            throw new InvalidDestinationException("The destination object used is not from this provider or of type javax.jms.Queue");
        }
    }

    public QueueSender createSender(Queue queue) throws JMSException
    {
        checkNotClosed();

        return new QueueSenderAdapter(createProducer(queue), queue);
    }

    public StreamMessage createStreamMessage() throws JMSException
    {
        checkNotClosed();
        if (_useAMQPEncodedStreamMessage)
        {
            AMQPEncodedListMessage msg = new AMQPEncodedListMessage(getMessageDelegateFactory());
            msg.setAMQSession(this);
            return msg;
        }
        else
        {
            JMSStreamMessage msg = new JMSStreamMessage(getMessageDelegateFactory());
            msg.setAMQSession(this);
            return msg;
        }
    }

    /**
     * Creates a non-durable subscriber
     *
     * @param topic
     *
     * @return TopicSubscriber - a wrapper round our MessageConsumer
     *
     * @throws JMSException
     */
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException
    {
        checkNotClosed();
        checkValidTopic(topic);

        return new TopicSubscriberAdaptor<C>(topic,
                createConsumerImpl(topic, _prefetchHighMark, _prefetchLowMark, false, true, null, null, false, false));
    }

    /**
     * Creates a non-durable subscriber with a message selector
     *
     * @param topic
     * @param messageSelector
     * @param noLocal
     *
     * @return TopicSubscriber - a wrapper round our MessageConsumer
     *
     * @throws JMSException
     */
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException
    {
        checkNotClosed();
        checkValidTopic(topic);

        return new TopicSubscriberAdaptor<C>(topic,
                                             createConsumerImpl(topic, _prefetchHighMark, _prefetchLowMark, noLocal,
                                                                true, messageSelector, null, false, false));
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException
    {
        checkNotClosed();
        try
        {
            AMQTemporaryQueue result = new AMQTemporaryQueue(this);

            // this is done so that we can produce to a temporary queue before we create a consumer
            result.setQueueName(result.getRoutingKey());
            Map<String, Object> args;
            if(_connection.getDelegate().isQueueLifetimePolicySupported())
            {
                args = new HashMap<>();
                args.put("qpid.lifetime_policy", "DELETE_ON_CONNECTION_CLOSE");
                args.put("qpid.exclusivity_policy", "CONNECTION");
            }
            else
            {
                args = null;
            }
            createQueue(result.getAMQQueueName(), result.isAutoDelete(),
                        result.isDurable(), result.isExclusive(),
                        args);
            bindQueue(result.getAMQQueueName(), result.getRoutingKey(),
                    new HashMap<String, Object>(), result.getExchangeName(), result);
            return result;
        }
        catch (QpidException e)
        {
           throw toJMSException("Cannot create temporary queue",e);
        }
        catch(TransportException e)
        {
            throw toJMSException("Cannot create temporary queue: " + e.getMessage(), e);
        }
        catch(Exception e)
        {
            throw JMSExceptionHelper.chainJMSException(new JMSException("Cannot create temporary queue: "
                                                                        + e.getMessage()), e);
        }
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException
    {
        checkNotClosed();

        return new AMQTemporaryTopic(this);
    }

    public TextMessage createTextMessage() throws JMSException
    {
        checkNotClosed();

        JMSTextMessage msg = new JMSTextMessage(getMessageDelegateFactory());
        msg.setAMQSession(this);
        return msg;
    }

    protected Object getFailoverMutex()
    {
        return _connection.getFailoverMutex();
    }

    public TextMessage createTextMessage(String text) throws JMSException
    {

        TextMessage msg = createTextMessage();
        msg.setText(text);

        return msg;
    }

    public Topic createTopic(String topicName) throws JMSException
    {
        checkNotClosed();
        try
        {
            if (topicName.indexOf('/') == -1 && topicName.indexOf(';') == -1)
            {
                DestSyntax syntax = AMQDestination.getDestType(topicName);
                // for testing we may want to use the prefix to indicate our choice.
                topicName = AMQDestination.stripSyntaxPrefix(topicName);
                if (syntax == AMQDestination.DestSyntax.BURL)
                {
                    return new AMQTopic(getDefaultTopicExchangeName(), topicName);
                }
                else
                {
                    return new AMQTopic("ADDR:" + getDefaultTopicExchangeName() + "/" + topicName);
                }
            }
            else
            {
                return new AMQTopic(topicName);
            }

        }
        catch (URISyntaxException urlse)
        {
            _logger.error("", urlse);
            throw JMSExceptionHelper.chainJMSException(new JMSException(urlse.getReason()), urlse);
        }
    }

    public void declareExchange(String name, String type, boolean nowait) throws QpidException
    {
        declareExchange(name, type, nowait, false, false, false);
    }

    @Override
    public void deleteExchange(final String exchangeName) throws JMSException
    {
        try
        {
            new FailoverRetrySupport<>(new FailoverProtectedOperation<Object, QpidException>()
            {
                public Object execute() throws QpidException, FailoverException
                {
                    sendExchangeDelete(exchangeName, false);
                    return null;
                }
            }, _connection).execute();
        }
        catch (QpidException e)
        {
            throw toJMSException("The exchange deletion failed: " + e.getMessage(), e);
        }
    }

    abstract void sendExchangeDelete(final String name, final boolean nowait) throws QpidException, FailoverException;

    abstract public void sync() throws QpidException;

    public int getAcknowledgeMode()
    {
        return _acknowledgeMode;
    }

    public AMQConnection getAMQConnection()
    {
        return _connection;
    }

    public int getChannelId()
    {
        return _channelId;
    }

    public int getDefaultPrefetch()
    {
        return _prefetchHighMark;
    }

    public int getDefaultPrefetchHigh()
    {
        return _prefetchHighMark;
    }

    public int getDefaultPrefetchLow()
    {
        return _prefetchLowMark;
    }

    public int getPrefetch()
    {
        return _prefetchHighMark;
    }

    public String getDefaultQueueExchangeName()
    {
        return _connection.getDefaultQueueExchangeName();
    }

    public String getDefaultTopicExchangeName()
    {
        return _connection.getDefaultTopicExchangeName();
    }

    public MessageListener getMessageListener() throws JMSException
    {
        return _messageListener;
    }

    public String getTemporaryQueueExchangeName()
    {
        return _connection.getTemporaryQueueExchangeName();
    }

    public String getTemporaryTopicExchangeName()
    {
        return _connection.getTemporaryTopicExchangeName();
    }

    public int getTicket()
    {
        return _ticket;
    }

    /**
     * Indicates whether the session is in transacted mode.
     *
     * @return true if the session is in transacted mode
     * @throws IllegalStateException - if session is closed.
     */
    public boolean getTransacted() throws JMSException
    {
        // Sun TCK checks that javax.jms.IllegalStateException is thrown for closed session
        // nowhere else this behavior is documented
        checkNotClosed();
        return _transacted;
    }

    /**
     * Indicates whether the session is in transacted mode.
     */
    public boolean isTransacted()
    {
        return _transacted;
    }

    public boolean hasConsumer(Destination destination)
    {
        AtomicInteger counter = _destinationConsumerCount.get(destination);

        return (counter != null) && (counter.get() != 0);
    }

    /** Indicates that warnings should be generated on violations of the strict AMQP. */
    public boolean isStrictAMQP()
    {
        return _strictAMQP;
    }

    public boolean isSuspended()
    {
        return _suspended;
    }

    protected void addUnacknowledgedMessage(long id)
    {
        _unacknowledgedMessageTags.add(id);
    }

    protected void addDeliveredMessage(long id)
    {
        _deliveredMessageTags.add(id);
    }

    /**
     * Invoked by the MINA IO thread (indirectly) when a message is received from the transport. Puts the message onto
     * the queue read by the dispatcher.
     *
     * @param message the message that has been received
     */
    public void messageReceived(UnprocessedMessage message)
    {
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Message[" + message.toString() + "] received in session");
        }
        _highestDeliveryTag.set(message.getDeliveryTag());
        _queue.add(message);
    }

    public void declareAndBind(AMQDestination amqd)
            throws
            QpidException
    {
        declareAndBind(amqd, new HashMap<String,Object>());
    }


    public void declareAndBind(AMQDestination amqd, Map<String,Object> arguments)
            throws
            QpidException
    {
        declareExchange(amqd, false);
        String queueName = declareQueue(amqd, false);
        bindQueue(queueName, amqd.getRoutingKey(), arguments, amqd.getExchangeName(), amqd);
    }

    /**
     * Stops message delivery in this session, and restarts message delivery with the oldest unacknowledged message.
     * <p>
     * All consumers deliver messages in a serial order. Acknowledging a received message automatically acknowledges
     * all messages that have been delivered to the client.
     * <p>
     * Restarting a session causes it to take the following actions:
     *
     * <ul>
     * <li>Stop message delivery.</li>
     * <li>Mark all messages that might have been delivered but not acknowledged as "redelivered".
     * <li>Restart the delivery sequence including all unacknowledged messages that had been previously delivered.
     * Redelivered messages do not have to be delivered in exactly their original delivery order.</li>
     * </ul>
     *
     * <p>
     * If the recover operation is interrupted by a fail-over, between asking that the broker begin recovery and
     * receiving acknowledgment that it has then a JMSException will be thrown. In this case it will not be possible
     * for the client to determine whether the broker is going to recover the session or not.
     *
     * @throws JMSException If the JMS provider fails to stop and restart message delivery due to some internal error.
     *                      Not that this does not necessarily mean that the recovery has failed, but simply that it is
     *                      not possible to tell if it has or not.
     * TODO  Be aware of possible changes to parameter order as versions change.
     *
     * Strategy for handling recover.
     * Flush any acks not yet sent.
     * Stop the message flow.
     * Clear the dispatch queue and the consumer queues.
     * Release/Reject all messages received but not yet acknowledged.
     * Start the message flow.
     */
    public void recover() throws JMSException
    {
        // Ensure that the session is open.
        checkNotClosed();

        // Ensure that the session is not transacted.
        checkNotTransacted();


        try
        {
            // flush any acks we are holding in the buffer.
            flushAcknowledgments();

            // this is only set true here, and only set false when the consumers preDeliver method is called
            _sessionInRecovery = true;

            boolean isSuspended = isSuspended();

            if (!isSuspended)
            {
                suspendChannel(true);
            }

            // Set to true to short circuit delivery of anything currently
            //in the pre-dispatch queue.
            _usingDispatcherForCleanup = true;

            syncDispatchQueue(false);

            // Set to false before sending the recover as 0-8/9/9-1 will
            //send messages back before the recover completes, and we
            //probably shouldn't clean those! ;-)
            _usingDispatcherForCleanup = false;

            if (_dispatcher != null)
            {
                _dispatcher.recover();
            }

            sendRecover();

            markClean();

            if (!isSuspended)
            {
                suspendChannel(false);
            }
        }
        catch (QpidException e)
        {
            throw toJMSException("Recover failed: " + e.getMessage(), e);
        }
        catch (FailoverException e)
        {
            throw JMSExceptionHelper.chainJMSException(new JMSException(
                    "Recovery was interrupted by fail-over. Recovery status is not known."), e);
        }
        catch(TransportException e)
        {
            throw toJMSException("Recover failed: " + e.getMessage(), e);
        }
    }

    protected abstract void sendRecover() throws QpidException, FailoverException;

    protected abstract void flushAcknowledgments();

    public void rejectMessage(UnprocessedMessage message, boolean requeue)
    {

        if (_logger.isDebugEnabled())
        {
            _logger.debug("Rejecting Unacked message:" + message.getDeliveryTag());
        }

        rejectMessage(message.getDeliveryTag(), requeue);
    }

    public void rejectMessage(AbstractJMSMessage message, boolean requeue)
    {
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Rejecting Abstract message:" + message.getDeliveryTag());
        }

        rejectMessage(message.getDeliveryTag(), requeue);

    }

    public abstract void rejectMessage(long deliveryTag, boolean requeue);

    /**
     * Commits all messages done in this transaction and releases any locks currently held.
     * <p>
     * If the rollback fails, because the rollback itself is interrupted by a fail-over between requesting that the
     * rollback be done, and receiving an acknowledgement that it has been done, then a JMSException will be thrown.
     * The client will be unable to determine whether or not the rollback actually happened on the broker in this case.
     *
     * @throws JMSException If the JMS provider fails to rollback the transaction due to some internal error. This does
     *                      not mean that the rollback is known to have failed, merely that it is not known whether it
     *                      failed or not.
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    public void rollback() throws JMSException
    {
        synchronized (_suspensionLock)
        {
            checkTransacted();

            try
            {
                boolean isSuspended = isSuspended();

                if (!isSuspended)
                {
                    suspendChannel(true);
                }

                setRollbackMark();

                syncDispatchQueue(false);

                _dispatcher.rollback();

                releaseForRollback();

                sendRollback();

                markClean();

                if (!isSuspended)
                {
                    suspendChannel(false);
                }
            }
            catch (QpidException e)
            {
                throw toJMSException("Failed to rollback: " + e, e);
            }
            catch (FailoverException e)
            {
                throw JMSExceptionHelper.chainJMSException(new JMSException(
                        "Fail-over interrupted rollback. Status of the rollback is uncertain."), e);
            }
            catch (TransportException e)
            {
                throw toJMSException("Failure to rollback:" + e.getMessage(), e);
            }
        }
    }

    public abstract void releaseForRollback();

    public abstract void sendRollback() throws QpidException, FailoverException;

    public void run()
    {
        throw new java.lang.UnsupportedOperationException();
    }

    public void setMessageListener(MessageListener listener) throws JMSException
    {
    }

    /**
     * @see #unsubscribe(String, boolean)
     */
    public void unsubscribe(String name) throws JMSException
    {
        try
        {
            unsubscribe(name, false);
        }
        catch (TransportException e)
        {
            throw toJMSException("Exception while unsubscribing:" + e.getMessage(), e);
        }
    }

    /**
     * Unsubscribe from a subscription.
     *
     * @param name the name of the subscription to unsubscribe
     * @param safe allows safe unsubscribe operation that will not throw an {@link InvalidDestinationException} if the
     * queue is not bound, possibly due to the subscription being closed.
     * @throws JMSException on
     * @throws InvalidDestinationException
     */
    private void unsubscribe(String name, boolean safe) throws JMSException
    {
        TopicSubscriberAdaptor<C> subscriber;

        _subscriberDetails.lock();
        try
        {
            checkNotClosed();
            subscriber = _subscriptions.get(name);
            if (subscriber != null)
            {
                // Remove saved subscription information
                _subscriptions.remove(name);
                _reverseSubscriptionMap.remove(subscriber.getMessageConsumer());
            }
        }
        finally
        {
            _subscriberDetails.unlock();
        }

        if (subscriber != null)
        {
            subscriber.close();

            // send a queue.delete for the subscription
            deleteQueue(AMQTopic.getDurableTopicQueueName(name, _connection));
        }
        else
        {
            if (_strictAMQP)
            {
                if (_strictAMQPFATAL)
                {
                    throw new UnsupportedOperationException("JMS Durable not currently supported by AMQP.");
                }
                else
                {
                    _logger.warn("Unable to determine if subscription already exists for '" + name + "' for unsubscribe."
                                 + " Requesting queue deletion regardless.");
                }

                deleteQueue(AMQTopic.getDurableTopicQueueName(name, _connection));
            }
            else // Queue Browser
            {

                if (isQueueBound(getDefaultTopicExchangeName(), AMQTopic.getDurableTopicQueueName(name, _connection)))
                {
                    deleteQueue(AMQTopic.getDurableTopicQueueName(name, _connection));
                }
                else if (!safe)
                {
                    throw new InvalidDestinationException("Unknown subscription name: " + name);
                }
            }
        }
    }

    protected C createConsumerImpl(final Destination destination, final int prefetchHigh,
                                   final int prefetchLow, final boolean noLocal,
                                   final boolean exclusive, String selector, final Map<String,Object> rawSelector,
                                   final boolean noConsume, final boolean autoClose) throws JMSException
    {
        checkTemporaryDestination(destination);

        if(!noConsume && isBrowseOnlyDestination(destination))
        {
            throw new InvalidDestinationException("The consumer being created is not 'noConsume'," +
                                                  "but a 'browseOnly' Destination has been supplied.");
        }

        final String messageSelector;

        if (_strictAMQP && !((selector == null) || selector.equals("")))
        {
            if (_strictAMQPFATAL)
            {
                throw new UnsupportedOperationException("Selectors not currently supported by AMQP.");
            }
            else
            {
                messageSelector = null;
            }
        }
        else
        {
            messageSelector = selector;
        }

        return new FailoverRetrySupport<C, JMSException>(
                new FailoverProtectedOperation<C, JMSException>()
                {
                    public C execute() throws JMSException, FailoverException
                    {
                        checkNotClosed();

                        AMQDestination amqd = (AMQDestination) destination;

                        C consumer;
                        try
                        {
                            consumer = createMessageConsumer(amqd, prefetchHigh, prefetchLow,
                                                             noLocal, exclusive, messageSelector, rawSelector, noConsume, autoClose);
                        }
                        catch(TransportException e)
                        {
                            throw toJMSException("Exception while creating consumer: " + e.getMessage(), e);
                        }

                        if (_messageListener != null)
                        {
                            consumer.setMessageListener(_messageListener);
                        }

                        try
                        {
                            registerConsumer(consumer, false);
                        }
                        catch (AMQInvalidArgumentException ise)
                        {
                            throw JMSExceptionHelper.chainJMSException(new InvalidSelectorException(ise.getMessage()),
                                                                       ise);
                        }
                        catch (AMQInvalidRoutingKeyException e)
                        {
                            throw JMSExceptionHelper.chainJMSException(new InvalidDestinationException(
                                    "Invalid routing key:"
                                    + amqd.getRoutingKey()
                            ), e);
                        }
                        catch (QpidException e)
                        {
                            if (e instanceof AMQChannelClosedException)
                            {
                                close(-1, false);
                            }

                            throw toJMSException("Error registering consumer: " + e,e);
                        }
                        catch (TransportException e)
                        {
                            throw toJMSException("Exception while registering consumer:" + e.getMessage(), e);
                        }
                        return consumer;
                    }
                }, _connection).execute();
    }

    public abstract C createMessageConsumer(final AMQDestination destination,
                                            final int prefetchHigh,
                                            final int prefetchLow,
                                            final boolean noLocal,
                                            final boolean exclusive, String selector,
                                            final Map<String,Object> arguments,
                                            final boolean noConsume,
                                            final boolean autoClose) throws JMSException;

    /**
     * Called by the MessageConsumer when closing, to deregister the consumer from the map from consumerTag to consumer
     * instance.
     *
     * @param consumer the consum
     */
    void deregisterConsumer(C consumer)
    {
        if (_consumers.remove(consumer.getConsumerTag()) != null)
        {
            _subscriberAccess.lock();
            try
            {
                String subscriptionName = _reverseSubscriptionMap.remove(consumer);
                if (subscriptionName != null)
                {
                    _subscriptions.remove(subscriptionName);
                }
            }
            finally
            {
                _subscriberAccess.unlock();
            }

            Destination dest = consumer.getDestination();
            synchronized (dest)
            {
                // Provide additional NPE check
                // This would occur if the consumer was closed before it was
                // fully opened.
                if (_destinationConsumerCount.get(dest) != null)
                {
                    if (_destinationConsumerCount.get(dest).decrementAndGet() == 0)
                    {
                        _destinationConsumerCount.remove(dest);
                    }
                }
            }
        }
    }

    void deregisterProducer(long producerId)
    {
        _producers.remove(producerId);
    }

    boolean isInRecovery()
    {
        return _sessionInRecovery;
    }

    boolean isQueueBound(String exchangeName, String queueName) throws JMSException
    {
        return isQueueBound(exchangeName, queueName, null);
    }

    /**
     * Tests whether or not the specified queue is bound to the specified exchange under a particular routing key.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     * @param exchangeName The exchange name to test for binding against.
     * @param queueName    The queue name to check if bound.
     * @param routingKey   The routing key to check if the queue is bound under.
     *
     * @return <tt>true</tt> if the queue is bound to the exchange and routing key, <tt>false</tt> if not.
     *
     * @throws JMSException If the query fails for any reason.
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    public abstract boolean isQueueBound(final String exchangeName, final String queueName, final String routingKey)
            throws JMSException;

    public abstract boolean isQueueBound(final AMQDestination destination) throws JMSException;

    public abstract boolean isQueueBound(String exchangeName, String queueName, String bindingKey, Map<String,Object> args) throws JMSException;

    /**
     * Called to mark the session as being closed. Useful when the session needs to be made invalid, e.g. after failover
     * when the client has vetoed resubscription.
     */
    void markClosed()
    {
        setClosed();
        _connection.deregisterSession(_channelId);
        markClosedProducersAndConsumers();

    }


    void syncDispatchQueue(final boolean holdDispatchLock)
    {
        if (Thread.currentThread() == _dispatcherThread || holdDispatchLock)
        {
            while (!super.isClosed() && !_queue.isEmpty())
            {
                Dispatchable disp;
                try
                {
                    disp = _queue.take();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }

                // Check just in case _queue becomes empty, it shouldn't but
                // better than an NPE.
                if (disp == null)
                {
                    _logger.debug("_queue became empty during sync.");
                    break;
                }

                disp.dispatch(AMQSession.this);
            }
        }
        else
        {
            startDispatcherIfNecessary();

            final CountDownLatch signal = new CountDownLatch(1);

            _queue.add(new Dispatchable()
            {
                public void dispatch(AMQSession ssn)
                {
                    signal.countDown();
                }
            });

            try
            {
                signal.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    void drainDispatchQueue()
    {
        if (Thread.currentThread() == _dispatcherThread)
        {
            while (!super.isClosed() && !_queue.isEmpty())
            {
                Dispatchable disp;
                try
                {
                    disp = _queue.take();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }

                // Check just in case _queue becomes empty, it shouldn't but
                // better than an NPE.
                if (disp == null)
                {
                    _logger.debug("_queue became empty during sync.");
                    break;
                }

                disp.dispatch(AMQSession.this);
            }
        }
        else
        {
            startDispatcherIfNecessary(false);

            final CountDownLatch signal = new CountDownLatch(1);

            _queue.add(new Dispatchable()
            {
                public void dispatch(AMQSession ssn)
                {
                    signal.countDown();
                }
            });

            try
            {
                signal.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Resubscribes all producers and consumers. This is called when performing failover.
     *
     * @throws QpidException
     */
    void resubscribe() throws QpidException
    {
        if (_dirty)
        {
            _failedOverDirty = true;
        }

        // Also reset the delivery tag tracker, to insure we dont
        // return the first <total number of msgs received on session>
        // messages sent by the brokers following the first rollback
        // after failover
        _highestDeliveryTag.set(-1);

        _unacknowledgedMessageTags.clear();
        _prefetchedMessageTags.clear();

        _rollbackMark.set(-1);
        clearResolvedDestinations();
        resubscribeProducers();
        resubscribeConsumers();
    }

    void setHasMessageListeners()
    {
        _hasMessageListeners = true;
    }

    void setInRecovery(boolean inRecovery)
    {
        _sessionInRecovery = inRecovery;
    }

    boolean isStarted()
    {
        return _startedAtLeastOnce.get();
    }

    /**
     * Starts the session, which ensures that it is not suspended and that its event dispatcher is running.
     *
     * @throws QpidException If the session cannot be started for any reason.
     * TODO  This should be controlled by _stopped as it pairs with the stop method fixme or check the
     * FlowControlledBlockingQueue _queue to see if we have flow controlled. will result in sending Flow messages
     * for each subsequent call to flow.. only need to do this if we have called stop.
     */
    void start() throws QpidException
    {
        // Check if the session has previously been started and suspended, in which case it must be unsuspended.
        if (_startedAtLeastOnce.getAndSet(true))
        {
            suspendChannel(false);
        }

        // If the event dispatcher is not running then start it too.
        if (hasMessageListeners())
        {
            startDispatcherIfNecessary();
        }
    }

    void startDispatcherIfNecessary()
    {
        //If we are the dispatcher then we don't need to check we are started
        if (Thread.currentThread() == _dispatcherThread)
        {
            return;
        }

        // If IMMEDIATE_PREFETCH is not set then we need to start fetching
        // This is final per session so will be multi-thread safe.
        if (!_immediatePrefetch)
        {
            // We do this now if this is the first call on a started connection
            if (isSuspended() && _startedAtLeastOnce.get() && _firstDispatcher.getAndSet(false))
            {
                try
                {
                    suspendChannel(false);
                }
                catch (QpidException e)
                {
                    _logger.info("Unsuspending channel threw an exception:", e);
                }
            }
        }

        startDispatcherIfNecessary(false);
    }

    synchronized void startDispatcherIfNecessary(boolean initiallyStopped)
    {
        if (_dispatcher == null)
        {
            _dispatcher = new Dispatcher();
            try
            {
                _dispatcherThread = Threading.getThreadFactory().createThread(_dispatcher);

            }
            catch(Exception e)
            {
                throw new Error("Error creating Dispatcher thread",e);
            }

            String dispatcherThreadName = "Dispatcher-" + _channelId + "-Conn-" + _connection.getConnectionNumber();

            _dispatcherThread.setName(dispatcherThreadName);
            _dispatcherThread.setDaemon(DAEMON_DISPATCHER_THREAD);
            _dispatcher.setConnectionStopped(initiallyStopped);
            _dispatcherThread.start();
            if (_dispatcherLogger.isDebugEnabled())
            {
                _dispatcherLogger.debug(_dispatcherThread.getName() + " created");
            }
        }
        else
        {
            _dispatcher.setConnectionStopped(initiallyStopped);
        }
    }

    void stop() throws QpidException
    {
        // Stop the server delivering messages to this session.
        suspendChannelIfNotClosing();
        stopExistingDispatcher();
    }

    private void checkNotTransacted() throws JMSException
    {
        if (getTransacted())
        {
            throw new IllegalStateException("Session is transacted");
        }
    }

    private void checkTemporaryDestination(Destination destination) throws JMSException
    {
        if ((destination instanceof TemporaryDestination))
        {
            _logger.debug("destination is temporary");
            final TemporaryDestination tempDest = (TemporaryDestination) destination;
            if (tempDest.getSession().getAMQConnection() != this.getAMQConnection())
            {
                _logger.debug("destination is on different conection");
                throw new JMSException("Cannot consume from a temporary destination created on another connection");
            }

            if (tempDest.isDeleted())
            {
                _logger.debug("destination is deleted");
                throw new JMSException("Cannot consume from a deleted destination");
            }
        }
    }

    protected void checkTransacted() throws JMSException
    {
        if (!getTransacted())
        {
            throw new IllegalStateException("Session is not transacted");
        }
    }

    private void checkValidDestination(Destination destination) throws InvalidDestinationException
    {
        if (destination == null)
        {
            throw new javax.jms.InvalidDestinationException("Invalid Queue");
        }
    }

    private void checkValidQueue(Queue queue) throws InvalidDestinationException
    {
        if (queue == null)
        {
            throw new javax.jms.InvalidDestinationException("Invalid Queue");
        }
    }

    /*
     * I could have combined the last 3 methods, but this way it improves readability
     */
    protected Topic checkValidTopic(Topic topic, boolean durable) throws JMSException
    {
        if (topic == null)
        {
            throw new javax.jms.InvalidDestinationException("Invalid Topic");
        }

        if ((topic instanceof TemporaryDestination) && (((TemporaryDestination) topic).getSession() != this))
        {
            throw new javax.jms.InvalidDestinationException(
                    "Cannot create a subscription on a temporary topic created in another session");
        }

        if ((topic instanceof TemporaryDestination) && durable)
        {
            throw new javax.jms.InvalidDestinationException
                ("Cannot create a durable subscription with a temporary topic: " + topic);
        }

        if (!(topic instanceof AMQDestination))
        {
            throw new javax.jms.InvalidDestinationException(
                    "Cannot create a subscription on topic created for another JMS Provider, class of topic provided is: "
                    + topic.getClass().getName());
        }

        return topic;
    }

    protected Topic checkValidTopic(Topic topic) throws JMSException
    {
        return checkValidTopic(topic, false);
    }

    /**
     * Called to close message consumers cleanly. This may or may <b>not</b> be as a result of an error.
     *
     * @param error not null if this is a result of an error occurring at the connection level
     */
    private void closeConsumers(Throwable error) throws JMSException
    {
        // we need to clone the list of consumers since the close() method updates the _consumers collection
        // which would result in a concurrent modification exception
        final ArrayList<C> clonedConsumers = new ArrayList<C>(_consumers.values());

        final Iterator<C> it = clonedConsumers.iterator();
        while (it.hasNext())
        {
            final C con = it.next();
            if (error != null)
            {
                con.notifyError(error);
            }
            else
            {
                con.close(false);
            }
        }
        // at this point the _consumers map will be empty
        if (_dispatcher != null)
        {
            _dispatcher.close();
            _dispatcher = null;
        }
    }

    /**
     * Called to close message producers cleanly. This may or may <b>not</b> be as a result of an error. There is
     * currently no way of propagating errors to message producers (this is a JMS limitation).
     */
    private void closeProducers() throws JMSException
    {
        // we need to clone the list of producers since the close() method updates the _producers collection
        // which would result in a concurrent modification exception
        final ArrayList clonedProducers = new ArrayList(_producers.values());

        final Iterator it = clonedProducers.iterator();
        while (it.hasNext())
        {
            final P prod = (P) it.next();
            prod.close();
        }
        // at this point the _producers map is empty
    }

    /**
     * Close all producers or consumers. This is called either in the error case or when closing the session normally.
     *
     * @param amqe the exception, may be null to indicate no error has occurred
     */
    private void closeProducersAndConsumers(QpidException amqe) throws JMSException
    {
        JMSException jmse = null;
        try
        {
            closeProducers();
        }
        catch (JMSException e)
        {
            _logger.error("Error closing session: " + e, e);
            jmse = e;
        }

        try
        {
            closeConsumers(amqe);
        }
        catch (JMSException e)
        {
            _logger.error("Error closing session: " + e, e);
            if (jmse == null)
            {
                jmse = e;
            }
        }

        if (jmse != null)
        {
            throw jmse;
        }
    }

    /**
     * Register to consume from the queue.
     * @param queueName
     */
    private void consumeFromQueue(C consumer, String queueName, boolean nowait) throws QpidException, FailoverException
    {
        Link link = consumer.getDestination().getLink();
        String linkName;
        if(link != null && link.getName() != null && consumer.getDestination().getAddressType() == AMQDestination.QUEUE_TYPE)
        {
            linkName = link.getName();
        }
        else
        {
            linkName = String.valueOf(_nextTag++);
        }

        consumer.setConsumerTag(linkName);
        // we must register the consumer in the map before we actually start listening
        _consumers.put(linkName, consumer);

        synchronized (consumer.getDestination())
        {
            _destinationConsumerCount.putIfAbsent(consumer.getDestination(), new AtomicInteger());
            _destinationConsumerCount.get(consumer.getDestination()).incrementAndGet();
        }


        try
        {
            sendConsume(consumer, queueName, nowait);
        }
        catch (QpidException e)
        {
            // clean-up the map in the event of an error
            _consumers.remove(linkName);
            throw e;
        }
    }

    void handleLinkCreation(AMQDestination dest) throws QpidException
    {
        createBindings(dest, dest.getLink().getBindings());
    }


    void createBindings(AMQDestination dest, List<AMQDestination.Binding> bindings) throws QpidException
    {
        String defaultExchangeForBinding = dest.getAddressType() == AMQDestination.TOPIC_TYPE ? dest
                .getAddressName() : "amq.topic";

        String defaultQueueName = null;
        if (AMQDestination.QUEUE_TYPE == dest.getAddressType())
        {
            defaultQueueName = dest.getQueueName();
        }
        else
        {
            defaultQueueName = dest.getLink().getName() != null ? dest.getLink().getName() : dest.getQueueName();
        }

        for (AMQDestination.Binding binding: bindings)
        {
            String queue = binding.getQueue() == null?
                    defaultQueueName: binding.getQueue();

            String exchange = binding.getExchange() == null ?
                    defaultExchangeForBinding :
                    binding.getExchange();

            if (_logger.isDebugEnabled())
            {
                _logger.debug("Binding queue : " + queue +
                              " exchange: " + exchange +
                              " using binding key " + binding.getBindingKey() +
                              " with args " + Strings.printMap(binding.getArgs()));
            }
            doBind(dest, binding, queue, exchange);
        }
    }

    protected abstract void handleQueueNodeCreation(AMQDestination dest, boolean noLocal) throws QpidException;

    abstract void handleExchangeNodeCreation(AMQDestination dest) throws QpidException;

    abstract protected void doBind(final AMQDestination dest, final AMQDestination.Binding binding, final String queue, final String exchange)
            throws QpidException;

    public abstract void sendConsume(C consumer, String queueName,
                                     boolean nowait) throws QpidException, FailoverException;

    private P createProducerImpl(final Destination destination, final Boolean mandatory, final Boolean immediate)
            throws JMSException
    {
        return new FailoverRetrySupport<P, JMSException>(
                new FailoverProtectedOperation<P, JMSException>()
                {
                    public P execute() throws JMSException, FailoverException
                    {
                        checkNotClosed();
                        long producerId = getNextProducerId();

                        P producer;
                        try
                        {
                            producer = createMessageProducer(destination, mandatory,
                                    immediate, producerId);
                        }
                        catch (TransportException e)
                        {
                            throw toJMSException("Exception while creating producer:" + e.getMessage(), e);
                        }

                        registerProducer(producerId, producer);

                        return producer;
                    }
                }, _connection).execute();
    }

    public abstract P createMessageProducer(final Destination destination, final Boolean mandatory,
                                            final Boolean immediate, final long producerId) throws JMSException;

    private void declareExchange(AMQDestination amqd, boolean nowait) throws QpidException
    {
        declareExchange(amqd.getExchangeName(), amqd.getExchangeClass(), nowait, amqd.isExchangeDurable(),
                        amqd.isExchangeAutoDelete(), amqd.isExchangeInternal());
    }

    /**
     * Returns the number of messages currently queued for the given destination.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     * @param amqd The destination to be checked
     *
     * @return the number of queued messages.
     *
     * @throws QpidException If the queue cannot be declared for any reason.
     */
    public long getQueueDepth(final AMQDestination amqd)
            throws QpidException
    {
        return getQueueDepth(amqd, false);
    }

    /**
     * Returns the number of messages currently queued by the given
     * destination. Syncs session before receiving the queue depth if sync is
     * set to true.
     *
     * @param amqd AMQ destination to get the depth value
     * @param sync flag to sync session before receiving the queue depth
     * @return queue depth
     * @throws QpidException
     */
    public long getQueueDepth(final AMQDestination amqd, final boolean sync) throws QpidException
    {
        return new FailoverNoopSupport<Long, QpidException>(new FailoverProtectedOperation<Long, QpidException>()
        {
            public Long execute() throws QpidException, FailoverException
            {
                try
                {
                    return requestQueueDepth(amqd, sync);
                }
                catch (TransportException e)
                {
                    throw new AMQException(getErrorCode(e), e.getMessage(), e);
                }
            }
        }, _connection).execute();
    }

    protected abstract Long requestQueueDepth(AMQDestination amqd, boolean sync) throws QpidException, FailoverException;

    /**
     * Declares the named exchange and type of exchange.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     * @param name            The name of the exchange to declare.
     * @param type            The type of the exchange to declare.
     * @param nowait
     * @param durable
     * @param autoDelete
     * @param internal
     * @throws QpidException If the exchange cannot be declared for any reason.
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    void declareExchange(final String name, final String type,
                         final boolean nowait, final boolean durable,
                         final boolean autoDelete, final boolean internal) throws QpidException
    {
        new FailoverNoopSupport<Object, QpidException>(new FailoverProtectedOperation<Object, QpidException>()
        {
            public Object execute() throws QpidException, FailoverException
            {
                sendExchangeDeclare(name, type, nowait, durable, autoDelete, internal);
                return null;
            }
        }, _connection).execute();
    }

    void declareExchange(final String name, final String type,
                         final boolean nowait, final boolean durable,
                         final boolean autoDelete, final Map<String,Object> arguments,
                         final boolean passive) throws QpidException
    {
        new FailoverNoopSupport<Object, QpidException>(new FailoverProtectedOperation<Object, QpidException>()
        {
            public Object execute() throws QpidException, FailoverException
            {
                sendExchangeDeclare(name, type, nowait, durable, autoDelete, arguments, passive);
                return null;
            }
        }, _connection).execute();
    }

    protected String preprocessAddressTopic(final C consumer,
                                            String queueName) throws QpidException
    {
        if (DestSyntax.ADDR == consumer.getDestination().getDestSyntax())
        {
            if (AMQDestination.TOPIC_TYPE == consumer.getDestination().getAddressType())
            {
                String selector =  consumer.getMessageSelectorFilter() == null? null : consumer.getMessageSelectorFilter().getSelector();

                createSubscriptionQueue(consumer.getDestination(), consumer.isNoLocal(), selector);
                queueName = consumer.getDestination().getAMQQueueName();
                consumer.setQueuename(queueName);
            }
            handleLinkCreation(consumer.getDestination());
        }
        return queueName;
    }

    abstract void createSubscriptionQueue(AMQDestination dest, boolean noLocal, String messageSelector) throws
                                                                                                        QpidException;

    public abstract void sendExchangeDeclare(final String name, final String type, final boolean nowait,
                                             boolean durable, boolean autoDelete, boolean internal) throws
                                                                                                    QpidException, FailoverException;


    public abstract void sendExchangeDeclare(final String name,
                                             final String type,
                                             final boolean nowait,
                                             boolean durable,
                                             boolean autoDelete,
                                             Map<String,Object> arguments,
                                             final boolean passive) throws QpidException, FailoverException;

    /**
     * Declares a queue for a JMS destination.
     * <p>
     * Note that for queues but not topics the name is generated in the client rather than the server. This allows
     * the name to be reused on failover if required. In general, the destination indicates whether it wants a name
     * generated or not.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     *
     * @param amqd            The destination to declare as a queue.
     * @return The name of the decalred queue. This is useful where the broker is generating a queue name on behalf of
     *         the client.
     *
     *
     *
     * @throws QpidException If the queue cannot be declared for any reason.
     * TODO  Verify the destiation is valid or throw an exception.
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    protected String declareQueue(final AMQDestination amqd,
                                  final boolean noLocal) throws QpidException
    {
        return declareQueue(amqd, noLocal, false);
    }

    protected String declareQueue(final AMQDestination amqd,
                                  final boolean noLocal, final boolean nowait)
                throws QpidException
    {
        return declareQueue(amqd, noLocal, nowait, false);
    }

    protected abstract String declareQueue(final AMQDestination amqd,
                                           final boolean noLocal, final boolean nowait, final boolean passive) throws
                                                                                                               QpidException;

    /**
     * Undeclares the specified queue.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     * @param queueName The name of the queue to delete.
     *
     * @throws JMSException If the queue could not be deleted for any reason.
     */
    @Override
    public void deleteQueue(final String queueName) throws JMSException
    {
        try
        {
            new FailoverRetrySupport<Object, QpidException>(new FailoverProtectedOperation<Object, QpidException>()
            {
                public Object execute() throws QpidException, FailoverException
                {
                    sendQueueDelete(queueName);
                    return null;
                }
            }, _connection).execute();
        }
        catch (QpidException e)
        {
            throw toJMSException("The queue deletion failed: " + e.getMessage(), e);
        }
    }

    /**
     * Undeclares the specified temporary queue/topic.
     * <p>
     * Note that this operation automatically retries in the event of fail-over.
     *
     * @param amqQueue The name of the temporary destination to delete.
     *
     * @throws JMSException If the queue could not be deleted for any reason.
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    protected void deleteTemporaryDestination(final TemporaryDestination amqQueue) throws JMSException
    {
        deleteQueue(amqQueue.getAMQQueueName());
    }

    public abstract void sendQueueDelete(final String queueName) throws QpidException, FailoverException;

    private long getNextProducerId()
    {
        return ++_nextProducerId;
    }

    protected boolean hasMessageListeners()
    {
        return _hasMessageListeners;
    }

    private void markClosedConsumers() throws JMSException
    {
        if (_dispatcher != null)
        {
            _dispatcher.close();
            _dispatcher = null;
        }
        // we need to clone the list of consumers since the close() method updates the _consumers collection
        // which would result in a concurrent modification exception
        final ArrayList<C> clonedConsumers = new ArrayList<C>(_consumers.values());

        final Iterator<C> it = clonedConsumers.iterator();
        while (it.hasNext())
        {
            final C con = it.next();
            con.markClosed();
        }
        // at this point the _consumers map will be empty
    }

    private void markClosedProducersAndConsumers()
    {
        try
        {
            // no need for a markClosed* method in this case since there is no protocol traffic closing a producer
            closeProducers();
        }
        catch (JMSException e)
        {
            _logger.error("Error closing session: " + e, e);
        }

        try
        {
            markClosedConsumers();
        }
        catch (JMSException e)
        {
            _logger.error("Error closing session: " + e, e);
        }
    }

    /**
     * Callers must hold the failover mutex before calling this method.
     *
     * @param consumer
     *
     * @throws QpidException
     */
    private void registerConsumer(C consumer, boolean nowait) throws QpidException
    {
        AMQDestination amqd = consumer.getDestination();

        if (amqd.getDestSyntax() == DestSyntax.ADDR)
        {
            resolveAddress(amqd,true,consumer.isNoLocal());
        }
        else
        {
            if (_declareExchanges && !amqd.neverDeclare())
            {
                declareExchange(amqd, nowait);
            }

            if ((_declareQueues || amqd.isNameRequired()) && !amqd.neverDeclare())
            {
                declareQueue(amqd, consumer.isNoLocal(), nowait);
            }
            if (_bindQueues && !amqd.neverDeclare() && !amqd.isDefaultExchange())
            {
                if(!isBound(amqd.getExchangeName(), amqd.getAMQQueueName(), amqd.getRoutingKey()))
                {
                    bindQueue(amqd.getAMQQueueName(), amqd.getRoutingKey(),
                            amqd instanceof AMQTopic ? consumer.getArguments() : null, amqd.getExchangeName(), amqd, nowait);
                }
            }

        }

        String queueName = amqd.getAMQQueueName();

        // store the consumer queue name
        consumer.setQueuename(queueName);

        // If IMMEDIATE_PREFETCH is not required then suspsend the channel to delay prefetch
        if (!_immediatePrefetch)
        {
            // The dispatcher will be null if we have just created this session
            // so suspend the channel before we register our consumer so that we don't
            // start prefetching until a receive/mListener is set.
            if (_dispatcher == null)
            {
                if (!isSuspended())
                {
                    try
                    {
                        suspendChannel(true);
                        _logger.debug(
                                "Prefetching delayed existing messages will not flow until requested via receive*() or setML().");
                    }
                    catch (QpidException e)
                    {
                        _logger.info("Suspending channel threw an exception:", e);
                    }
                }
            }
        }
        else
        {
            _logger.debug("Immediately prefetching existing messages to new consumer.");
        }

        try
        {
            consumeFromQueue(consumer, queueName, nowait);
        }
        catch (FailoverException e)
        {
            throw new QpidException("Fail-over exception interrupted basic consume.", e);
        }
    }

    protected abstract boolean isBound(String exchangeName, String amqQueueName, String routingKey)
            throws QpidException;

    private void registerProducer(long producerId, MessageProducer producer)
    {
        _producers.put(producerId, producer);
    }

    private void rejectMessagesForConsumerTag(String consumerTag)
    {
        Iterator<Dispatchable> messages = _queue.iterator();
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Rejecting messages from _queue for Consumer tag(" + consumerTag + ")");

            if (messages.hasNext())
            {
                _logger.debug("Checking all messages in _queue for Consumer tag(" + consumerTag + ")");
            }
            else
            {
                _logger.debug("No messages in _queue to reject");
            }
        }
        while (messages.hasNext())
        {
            UnprocessedMessage message = (UnprocessedMessage) messages.next();

            if (message.getConsumerTag().equals(consumerTag))
            {

                if (_queue.remove(message))
                {
                    if (_logger.isDebugEnabled())
                    {
                        _logger.debug("Removing message(" + System.identityHashCode(message) + ") from _queue DT:"
                                      + message.getDeliveryTag());
                    }

                    rejectMessage(message, true);

                    if (_logger.isDebugEnabled())
                    {
                        _logger.debug("Rejected the message(" + message.toString() + ") for consumer :" + consumerTag);
                    }
                }
            }
        }
    }

    private void resubscribeConsumers() throws QpidException
    {
        ArrayList<C> consumers = new ArrayList<C>(_consumers.values());
        _consumers.clear();

        for (C consumer : consumers)
        {
            consumer.failedOverPre();
            registerConsumer(consumer, true);
            consumer.failedOverPost();
        }
    }

    private void resubscribeProducers() throws QpidException
    {
        ArrayList producers = new ArrayList(_producers.values());
        _logger.debug(MessageFormat.format("Resubscribing producers = {0} producers.size={1}",
                                           producers,
                                           producers.size())); // FIXME: removeKey
        for (Iterator it = producers.iterator(); it.hasNext();)
        {
            P producer = (P) it.next();
            producer.resubscribe();
        }
    }

    /**
     * Suspends or unsuspends this session.
     *
     * @param suspend true indicates that the session should be suspended, false indicates that it
     *                should be unsuspended.
     *
     * @throws QpidException If the session cannot be suspended for any reason.
     * TODO  Be aware of possible changes to parameter order as versions change.
     */
    protected void suspendChannel(boolean suspend) throws QpidException
    {
        synchronized (_suspensionLock)
        {
            try
            {
                if (_logger.isDebugEnabled())
                {
                    _logger.debug("Setting channel flow : " + (suspend ? "suspended" : "unsuspended"));
                }

                _suspended = suspend;
                sendSuspendChannel(suspend);
            }
            catch (FailoverException e)
            {
                throw new QpidException("Fail-over interrupted suspend/unsuspend channel.", e);
            }
            catch (TransportException e)
            {
                throw new AMQException(getErrorCode(e), e.getMessage(), e);
            }
        }
    }

    public abstract void sendSuspendChannel(boolean suspend) throws QpidException, FailoverException;

    boolean tryLockMessageDelivery()
    {
        try
        {
            // Use timeout of zero to respect fairness. See ReentrantLock#tryLock JavaDocs for details.
            return _messageDeliveryLock.tryLock(0, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    void lockMessageDelivery()
    {
        _messageDeliveryLock.lock();
    }

    void unlockMessageDelivery()
    {
        _messageDeliveryLock.unlock();
    }

    /**
     * Indicates whether this session consumers pre-fetche messages
     *
     * @return true if this session consumers pre-fetche messages false otherwise
     */
    public boolean prefetch()
    {
        return _prefetchHighMark > 0;
    }

    /** Signifies that the session has pending sends to commit. */
    public void markDirty()
    {
        _dirty = true;
    }

    /** Signifies that the session has no pending sends to commit. */
    public void markClean()
    {
        _dirty = false;
        _failedOverDirty = false;
    }

    /**
     * Check to see if failover has occured since the last call to markClean(commit or rollback).
     *
     * @return boolean true if failover has occured.
     */
    public boolean hasFailedOverDirty()
    {
        return _failedOverDirty;
    }

    public void setTicket(int ticket)
    {
        _ticket = ticket;
    }

    /**
     * Tests whether flow to this session is blocked.
     *
     * @return true if flow is blocked or false otherwise.
     */
    public abstract boolean isFlowBlocked();

    public abstract void setFlowControl(final boolean active);

    Object getDispatcherLock()
    {
        Dispatcher dispatcher = _dispatcher;
        return dispatcher == null ? null : dispatcher._lock;
    }

    public String createTemporaryQueueName()
    {
        String prefix = _connection.getTemporaryQueuePrefix();
        assert(prefix.isEmpty() || prefix.endsWith("/"));
        return prefix + "TempQueue" + UUID.randomUUID();
    }

    public interface Dispatchable
    {
        void dispatch(AMQSession ssn);
    }

    public void dispatch(UnprocessedMessage message)
    {
        if (_dispatcher == null)
        {
            throw new java.lang.IllegalStateException("dispatcher is not started");
        }

        _dispatcher.dispatchMessage(message);
    }

    /** Used for debugging in the dispatcher. */
    private static final Logger _dispatcherLogger = LoggerFactory.getLogger("org.apache.qpid.client.AMQSession.Dispatcher");

    /** Responsible for decoding a message fragment and passing it to the appropriate message consumer. */
    class Dispatcher implements Runnable
    {

        /** Track the 'stopped' state of the dispatcher, a session starts in the stopped state. */
        private final AtomicBoolean _closed = new AtomicBoolean(false);
        private final CountDownLatch _closeCompleted = new CountDownLatch(1);

        private final Object _lock = new Object();
        private final String dispatcherID = "" + System.identityHashCode(this);

        public Dispatcher()
        {
        }

        public void close()
        {
            _closed.set(true);
            _queue.close();
            _dispatcherThread.interrupt();

            // If we are not the dispatcherThread we need to await the exiting of the Dispatcher#run(). See QPID-6672.
            if (Thread.currentThread() != _dispatcherThread)
            {
                try
                {
                    if(!_closeCompleted.await(_dispatcherShutdownTimeoutMs, TimeUnit.MILLISECONDS))
                    {
                        throw new RuntimeException("Dispatcher did not close down within the timeout of " + _dispatcherShutdownTimeoutMs + " ms.");
                    }
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private AtomicBoolean getClosed()
        {
            return _closed;
        }


        public void rollback()
        {

            synchronized (_lock)
            {
                boolean isStopped = connectionStopped();

                if (!isStopped)
                {
                    setConnectionStopped(true);
                }

                setRollbackMark();

                _dispatcherLogger.debug("Session Pre Dispatch Queue cleared");

                for (C consumer : _consumers.values())
                {
                    if (!consumer.isBrowseOnly())
                    {
                        consumer.releasePendingMessages();
                    }
                    else
                    {
                        // should perhaps clear the _SQ here.
                        consumer.clearReceiveQueue();
                    }

                }

                setConnectionStopped(isStopped);
            }

        }

        public void recover()
        {

            synchronized (_lock)
            {
                boolean isStopped = connectionStopped();

                if (!isStopped)
                {
                    setConnectionStopped(true);
                }

                _dispatcherLogger.debug("Session clearing the consumer queues");

                for (C consumer : _consumers.values())
                {
                    List<Long> tags = consumer.drainReceiverQueueAndRetrieveDeliveryTags();
                    _prefetchedMessageTags.addAll(tags);
                }

                setConnectionStopped(isStopped);
            }

        }


        public void run()
        {
            try
            {
                if (_dispatcherLogger.isDebugEnabled())
                {
                    _dispatcherLogger.debug(_dispatcherThread.getName() + " started");
                }

                // Allow dispatcher to start stopped
                synchronized (_lock)
                {
                    while (!_closed.get() && connectionStopped())
                    {
                        try
                        {
                            _lock.wait();
                        }
                        catch (InterruptedException e)
                        {
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                try
                {

                    while (((_queue.blockingPeek()) != null) && !_closed.get())
                    {
                        synchronized (_lock)
                        {
                            if (!isClosed() && !isClosing() && !_closed.get())
                            {
                                Dispatchable disp = _queue.nonBlockingTake();

                                if(disp != null)
                                {
                                    disp.dispatch(AMQSession.this);
                                }
                            }
                        }
                    }
                }
                catch (InterruptedException e)
                {
                    // ignored as run will exit immediately
                }
            }
            finally
            {
                _closeCompleted.countDown();
                if (_dispatcherLogger.isDebugEnabled())
                {
                    _dispatcherLogger.debug(_dispatcherThread.getName() + " thread terminating for channel " + _channelId + ":" + AMQSession.this);
                }
            }
        }

        // only call while holding lock
        final boolean connectionStopped()
        {
            return _connectionStopped.get();
        }

        boolean setConnectionStopped(boolean connectionStopped)
        {
            boolean currently = _connectionStopped.get();
            if(connectionStopped != currently)
            {
                synchronized (_lock)
                {
                    _connectionStopped.set(connectionStopped);
                    _lock.notify();

                    if (_dispatcherLogger.isDebugEnabled())
                    {
                        _dispatcherLogger.debug("Set Dispatcher Connection " + (connectionStopped
                                ? "Stopped"
                                : "Started")
                                                + ": Currently " + (currently ? "Stopped" : "Started"));
                    }
                }
            }
            return currently;
        }

        private void dispatchMessage(UnprocessedMessage message)
        {
            long deliveryTag = message.getDeliveryTag();

            synchronized (_lock)
            {

                try
                {
                    while (connectionStopped())
                    {
                        _lock.wait();
                    }
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }

                if (!(message instanceof CloseConsumerMessage)
                    && tagLE(deliveryTag, _rollbackMark.get()))
                {
                    if (_logger.isDebugEnabled())
                    {
                        _logger.debug("Rejecting message because delivery tag " + deliveryTag
                                + " <= rollback mark " + _rollbackMark.get());
                    }
                    rejectMessage(message, true);
                }
                else if (_usingDispatcherForCleanup)
                {
                    _prefetchedMessageTags.add(deliveryTag);
                }
                else
                {
                    while (!isClosed() && !isClosing())
                    {
                        if (tryLockMessageDelivery())
                        {
                            try
                            {
                                notifyConsumer(message);
                                break;
                            }
                            finally
                            {
                                unlockMessageDelivery();
                            }
                        }
                    }
                }
            }

            long current = _rollbackMark.get();
            if (updateRollbackMark(current, deliveryTag))
            {
                _rollbackMark.compareAndSet(current, deliveryTag);
            }
        }

        private void notifyConsumer(UnprocessedMessage message)
        {
            final C consumer = _consumers.get(message.getConsumerTag());

            if ((consumer == null) || consumer.isClosed() || consumer.isClosing())
            {
                if (_dispatcherLogger.isInfoEnabled())
                {
                    if (consumer == null)
                    {
                        _dispatcherLogger.info("Dispatcher(" + dispatcherID + ")Received a message("
                                               + System.identityHashCode(message) + ")" + "["
                                               + message.getDeliveryTag() + "] from queue "
                                               + message.getConsumerTag() + " )without a handler - rejecting(requeue)...");
                    }
                    else
                    {
                        if (consumer.isBrowseOnly())
                        {
                            _dispatcherLogger.info("Received a message("
                                                   + System.identityHashCode(message) + ")" + "["
                                                   + message.getDeliveryTag() + "] from queue " + " consumer("
                                                   + message.getConsumerTag() + ") is closed and a browser so dropping...");
                            //DROP MESSAGE
                            return;

                        }
                        else
                        {
                            _dispatcherLogger.info("Received a message("
                                                   + System.identityHashCode(message) + ")" + "["
                                                   + message.getDeliveryTag() + "] from queue " + " consumer("
                                                   + message.getConsumerTag() + ") is closed rejecting(requeue)...");
                        }
                    }
                }
                // Don't reject if we're already closing
                if (!_closed.get())
                {
                    if (_logger.isDebugEnabled())
                    {
                        _logger.debug("Rejecting message with delivery tag " + message.getDeliveryTag()
                                + " for closing consumer " + String.valueOf(consumer == null? null: consumer.getConsumerTag()));
                    }
                    rejectMessage(message, true);
                }
            }
            else
            {
                consumer.notifyMessage(message);
            }
        }
    }

    protected abstract boolean tagLE(long tag1, long tag2);

    protected abstract boolean updateRollbackMark(long current, long deliveryTag);

    public abstract AMQMessageDelegateFactory getMessageDelegateFactory();

    private class SuspenderRunner implements Runnable
    {
        private AtomicBoolean _suspend;

        public SuspenderRunner(AtomicBoolean suspend)
        {
            _suspend = suspend;
        }

        public void run()
        {
            try
            {
                synchronized (_suspensionLock)
                {
                    // If the session has closed by the time we get here
                    // then we should not attempt to write to the session/channel.
                    if (!(AMQSession.this.isClosed() || AMQSession.this.isClosing()))
                    {
                        suspendChannel(_suspend.get());
                    }
                }
            }
            catch (QpidException e)
            {
                _logger.warn("Unable to " + (_suspend.get() ? "suspend" : "unsuspend") + " session " + AMQSession.this + " due to: ", e);
                if (_logger.isDebugEnabled())
                {
                    _logger.debug("Is the _queue empty?" + _queue.isEmpty());
                    _logger.debug("Is the dispatcher closed?" + (_dispatcher == null ? "it's Null" : _dispatcher.getClosed()));
                }
            }
        }
    }

    /**
     * Checks if the Session and its parent connection are closed
     *
     * @return <tt>true</tt> if this is closed, <tt>false</tt> otherwise.
     */
    @Override
    public boolean isClosed()
    {
        return super.isClosed() || _connection.isClosed();
    }

    /**
     * Checks if the Session and its parent connection are capable of performing
     * closing operations
     *
     * @return <tt>true</tt> if we are closing, <tt>false</tt> otherwise.
     */
    @Override
    public boolean isClosing()
    {
        return super.isClosing() || _connection.isClosing();
    }

    public boolean isDeclareExchanges()
    {
    	return _declareExchanges;
    }

    JMSException toJMSException(String message, TransportException e)
    {
        int code = getErrorCode(e);
        return JMSExceptionHelper.chainJMSException(new JMSException(message, Integer.toString(code)), e);
    }

    private int getErrorCode(TransportException e)
    {
        int code = ErrorCodes.INTERNAL_ERROR;
        if (e instanceof SessionException)
        {
            SessionException se = (SessionException) e;
            if(se.getException() != null && se.getException().getErrorCode() != null)
            {
                code = se.getException().getErrorCode().getValue();
            }
        }
        return code;
    }

    JMSException toJMSException(String message, QpidException e)
    {
        JMSException ex;
        int errorCode = 0;

        if (e instanceof AMQException)
        {
            errorCode = ((AMQException) e).getErrorCode();
        }

        if (errorCode == ErrorCodes.ACCESS_REFUSED)
        {
            ex = JMSExceptionHelper.chainJMSException(new JMSSecurityException(message,
                                                                               Integer.toString(errorCode)), e);
        }
        else
        {
            ex = JMSExceptionHelper.chainJMSException(new JMSException(message, errorCode == 0 ? null : Integer.toString(errorCode)), e);
        }
        return ex;
    }

    private boolean isBrowseOnlyDestination(Destination destination)
    {
        return ((destination instanceof AMQDestination)  && ((AMQDestination)destination).isBrowseOnly());
    }

    private void setRollbackMark()
    {
        // Let the dispatcher know that all the incomming messages
        // should be rolled back(reject/release)
        _rollbackMark.set(_highestDeliveryTag.get());
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Rollback mark is set to " + _rollbackMark.get());
        }
    }


    public MessageEncryptionHelper getMessageEncryptionHelper()
    {
        return _messageEncryptionHelper;
    }

    protected void drainDispatchQueueWithDispatcher()
    {
        if (!_queue.isEmpty())
        {
            try
            {
                setUsingDispatcherForCleanup(true);
                drainDispatchQueue();
            }
            finally
            {
                setUsingDispatcherForCleanup(false);
            }
        }
    }

    protected void stopExistingDispatcher()
    {
        Dispatcher dispatcher = _dispatcher;
        if (dispatcher != null)
        {
            dispatcher.setConnectionStopped(true);
        }
    }

    protected void suspendChannelIfNotClosing() throws QpidException
    {
        if (!(isClosed() || isClosing()))
        {
            suspendChannel(true);
        }
    }

    protected void clearDispatchQueue()
    {
        _queue.clear();
    }

    private void shutdownFlowControlNoAckTaskPool()
    {
        if (_flowControlNoAckTaskPool != null)
        {
            _flowControlNoAckTaskPool.shutdown();
        }
    }

}

