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

import static org.apache.qpid.server.logging.subjects.LogSubjectFormat.SUBSCRIPTION_FORMAT;

import java.text.MessageFormat;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.filter.JMSSelectorFilter;
import org.apache.qpid.server.filter.MessageFilter;
import org.apache.qpid.server.filter.SelectorParsingException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.SubscriptionMessages;
import org.apache.qpid.server.logging.subjects.QueueLogSubject;
import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.util.StateChangeListener;

class QueueConsumerImpl<T extends ConsumerTarget>
    extends AbstractConfiguredObject<QueueConsumerImpl<T>>
        implements QueueConsumer<QueueConsumerImpl<T>,T>, LogSubject
{
    private final static Logger LOGGER = LoggerFactory.getLogger(QueueConsumerImpl.class);
    private final AtomicBoolean _closed = new AtomicBoolean(false);
    private final long _consumerNumber;
    private final long _createTime = System.currentTimeMillis();
    private final MessageInstance.StealableConsumerAcquiredState<QueueConsumerImpl<T>>
            _owningState = new MessageInstance.StealableConsumerAcquiredState<>(this);
    private final WaitingOnCreditMessageListener _waitingOnCreditMessageListener = new WaitingOnCreditMessageListener();
    private final boolean _acquires;
    private final boolean _seesRequeues;
    private final boolean _isTransient;
    private final AtomicLong _deliveredCount = new AtomicLong(0);
    private final AtomicLong _deliveredBytes = new AtomicLong(0);
    private final FilterManager _filters;
    private final Class<? extends ServerMessage> _messageClass;
    private final Object _sessionReference;
    private final AbstractQueue _queue;

    private final T _target;
    private volatile QueueContext _queueContext;


    @ManagedAttributeField
    private boolean _exclusive;
    @ManagedAttributeField
    private boolean _noLocal;
    @ManagedAttributeField
    private String _distributionMode;
    @ManagedAttributeField
    private String _settlementMode;
    @ManagedAttributeField
    private String _selector;
    @ManagedAttributeField
    private int _priority;

    private final String _linkName;

    private volatile QueueConsumerNode _queueConsumerNode;
    private volatile boolean _nonLive;

    QueueConsumerImpl(final AbstractQueue<?> queue,
                      T target,
                      final String consumerName,
                      final FilterManager filters,
                      final Class<? extends ServerMessage> messageClass,
                      EnumSet<ConsumerOption> optionSet,
                      final Integer priority)
    {
        super(queue,
              createAttributeMap(target.getSession(), consumerName, filters, optionSet, priority));
        _messageClass = messageClass;
        _sessionReference = target.getSession().getConnectionReference();
        _consumerNumber = CONSUMER_NUMBER_GENERATOR.getAndIncrement();
        _filters = filters;
        _acquires = optionSet.contains(ConsumerOption.ACQUIRES);
        _seesRequeues = optionSet.contains(ConsumerOption.SEES_REQUEUES);
        _isTransient = optionSet.contains(ConsumerOption.TRANSIENT);
        _target = target;
        _queue = queue;
        _linkName = consumerName;

        // Access control
        authorise(Operation.CREATE);

        open();

        setupLogging();
    }

    private static Map<String, Object> createAttributeMap(final AMQPSession<?,?> session,
                                                          String linkName,
                                                          FilterManager filters,
                                                          EnumSet<ConsumerOption> optionSet,
                                                          Integer priority)
    {
        Map<String,Object> attributes = new HashMap<String, Object>();
        attributes.put(ID, UUID.randomUUID());
        String name = session.getAMQPConnection().getConnectionId()
                      + "|"
                      + session.getChannelId()
                      + "|"
                      + linkName;
        attributes.put(NAME, name);
        attributes.put(EXCLUSIVE, optionSet.contains(ConsumerOption.EXCLUSIVE));
        attributes.put(NO_LOCAL, optionSet.contains(ConsumerOption.NO_LOCAL));
        attributes.put(DISTRIBUTION_MODE, optionSet.contains(ConsumerOption.ACQUIRES) ? "MOVE" : "COPY");
        attributes.put(DURABLE,optionSet.contains(ConsumerOption.DURABLE));
        attributes.put(LIFETIME_POLICY, LifetimePolicy.DELETE_ON_SESSION_END);
        if(priority != null)
        {
            attributes.put(PRIORITY, priority);
        }
        if(filters != null)
        {
            Iterator<MessageFilter> iter = filters.filters();
            while(iter.hasNext())
            {
                MessageFilter filter = iter.next();
                if(filter instanceof JMSSelectorFilter)
                {
                    attributes.put(SELECTOR, ((JMSSelectorFilter) filter).getSelector());
                    break;
                }
            }
        }

        return attributes;
    }

    @Override
    public T getTarget()
    {
        return _target;
    }

    @Override
    public String getLinkName()
    {
        return _linkName;
    }

    @Override
    public void awaitCredit(final QueueEntry node)
    {
        _waitingOnCreditMessageListener.update(node);
    }

    @Override
    public boolean isNotifyWorkDesired()
    {
        return !isNonLive() && _target.isNotifyWorkDesired();
    }

    @Override
    public void externalStateChange()
    {
        _target.notifyWork();
    }

    @Override
    public long getUnacknowledgedBytes()
    {
        return _target.getUnacknowledgedBytes();
    }

    @Override
    public long getUnacknowledgedMessages()
    {
        return _target.getUnacknowledgedMessages();
    }

    @Override
    public AMQPSession<?,?> getSession()
    {
        return _target.getSession();
    }

    @Override
    public Object getIdentifier()
    {
        return getConsumerNumber();
    }

    @Override
    public boolean isSuspended()
    {
        return _target.isSuspended();
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        if(_closed.compareAndSet(false,true))
        {
            getEventLogger().message(getLogSubject(), SubscriptionMessages.CLOSE());

            _waitingOnCreditMessageListener.remove();

            return doAfter(_target.consumerRemoved(this),
                           () -> {
                               _queue.unregisterConsumer(QueueConsumerImpl.this);
                           }).then(this::deleteNoChecks);
        }
        else
        {
            return Futures.immediateFuture(null);
        }
    }

    @Override
    public int getPriority()
    {
        return _priority;
    }

    @Override
    public void flushBatched()
    {
        _target.flushBatched();
    }

    @Override
    public void notifyWork()
    {
        _target.notifyWork();
    }

    @Override
    public void setQueueConsumerNode(final QueueConsumerNode node)
    {
        _queueConsumerNode = node;
    }

    @Override
    public QueueConsumerNode getQueueConsumerNode()
    {
        return _queueConsumerNode;
    }

    @Override
    public void queueDeleted()
    {
        _target.queueDeleted(getQueue(), this);
    }

    @Override
    public boolean allocateCredit(final QueueEntry msg)
    {
        return _target.allocateCredit(msg.getMessage());
    }

    @Override
    public void restoreCredit(final QueueEntry queueEntry)
    {
        _target.restoreCredit(queueEntry.getMessage());
    }

    @Override
    public void noMessagesAvailable()
    {
        _target.noMessagesAvailable();
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(SubscriptionMessages.OPERATION(operation));
    }

    @Override
    public final Queue<?> getQueue()
    {
        return _queue;
    }

    private void setupLogging()
    {
        final String filterLogString = getFilterLogString();
        getEventLogger().message(this,
                                 SubscriptionMessages.CREATE(filterLogString, _queue.isDurable() && _exclusive,
                                                             filterLogString.length() > 0));
    }

    protected final LogSubject getLogSubject()
    {
        return this;
    }

    @Override
    public MessageContainer pullMessage()
    {
        MessageContainer messageContainer = _queue.deliverSingleMessage(this);
        if (messageContainer != null)
        {
            _deliveredCount.incrementAndGet();
            _deliveredBytes.addAndGet(messageContainer.getMessageInstance().getMessage().getSizeIncludingHeader());
        }
        return messageContainer;
    }

    @Override
    public void setNotifyWorkDesired(final boolean desired)
    {
        _queue.setNotifyWorkDesired(this, desired);
    }

    @Override
    public final long getConsumerNumber()
    {
        return _consumerNumber;
    }

    @Override
    public final QueueContext getQueueContext()
    {
        return _queueContext;
    }

    final void setQueueContext(QueueContext queueContext)
    {
        _queueContext = queueContext;
    }

    @Override
    public final boolean isActive()
    {
        return _target.getState() == ConsumerTarget.State.OPEN;
    }

    @Override
    public final boolean isClosed()
    {
        return _target.getState() == ConsumerTarget.State.CLOSED;
    }

    @Override
    public final boolean hasInterest(QueueEntry entry)
    {
       //check that the message hasn't been rejected
        if (entry.isRejectedBy(this) || entry.checkHeld(System.currentTimeMillis()))
        {
            return false;
        }

        if (entry.getMessage().getClass() == _messageClass)
        {
            if(_noLocal)
            {
                Object connectionRef = entry.getMessage().getConnectionReference();
                if (connectionRef != null && connectionRef == _sessionReference)
                {
                    return false;
                }
            }
        }
        else
        {
            // no interest in messages we can't convert
            if(_messageClass != null && MessageConverterRegistry.getConverter(entry.getMessage().getClass(),
                                                                              _messageClass)==null)
            {
                return false;
            }
        }

        if (_filters == null)
        {
            return true;
        }
        else
        {
            MessageReference ref = entry.newMessageReference();
            if(ref != null)
            {
                try
                {

                    Filterable msg = entry.asFilterable();
                    try
                    {
                        return _filters.allAllow(msg);
                    }
                    catch (SelectorParsingException e)
                    {
                        LOGGER.info(this + " could not evaluate filter [" + _filters
                                    + "]  against message " + msg
                                    + ". Error was : " + e.getMessage());
                        return false;
                    }
                }
                finally
                {
                    ref.release();
                }
            }
            else
            {
                return false;
            }
        }
    }

    protected String getFilterLogString()
    {
        StringBuilder filterLogString = new StringBuilder();
        String delimiter = ", ";
        boolean hasEntries = false;
        if (_filters != null && _filters.hasFilters())
        {
            filterLogString.append(_filters.toString());
            hasEntries = true;
        }

        if (!acquires())
        {
            if (hasEntries)
            {
                filterLogString.append(delimiter);
            }
            filterLogString.append("Browser");
        }

        return filterLogString.toString();
    }

    public final long getCreateTime()
    {
        return _createTime;
    }

    @Override
    public final MessageInstance.StealableConsumerAcquiredState<QueueConsumerImpl<T>> getOwningState()
    {
        return _owningState;
    }

    @Override
    public final boolean acquires()
    {
        return _acquires;
    }

    @Override
    public final boolean seesRequeues()
    {
        return _seesRequeues;
    }

    public final boolean isTransient()
    {
        return _isTransient;
    }

    @Override
    public final long getBytesOut()
    {
        return _deliveredBytes.longValue();
    }

    @Override
    public final long getMessagesOut()
    {
        return _deliveredCount.longValue();
    }

    @Override
    public void acquisitionRemoved(final QueueEntry node)
    {
        _target.acquisitionRemoved(node);
    }

    @Override
    public String getDistributionMode()
    {
        return _distributionMode;
    }

    @Override
    public String getSettlementMode()
    {
        return _settlementMode;
    }

    @Override
    public boolean isExclusive()
    {
        return _exclusive;
    }

    @Override
    public boolean isNoLocal()
    {
        return _noLocal;
    }

    @Override
    public String getSelector()
    {
        return _selector;
    }

    @Override
    public boolean isNonLive()
    {
        return _nonLive;
    }

    public void setNonLive(final boolean nonLive)
    {
        _nonLive = nonLive;
    }

    @Override
    public String toLogString()
    {
        String logString;
        if(_queue == null)
        {
            logString = "[" + MessageFormat.format(SUBSCRIPTION_FORMAT, getConsumerNumber())
                        + "(UNKNOWN)"
                        + "] ";
        }
        else
        {
            String queueString = new QueueLogSubject(_queue).toLogString();
            logString = "[" + MessageFormat.format(SUBSCRIPTION_FORMAT, getConsumerNumber())
                                     + "("
                                     // queueString is [vh(/{0})/qu({1}) ] so need to trim
                                     //                ^                ^^
                                     + queueString.substring(1,queueString.length() - 3)
                                     + ")"
                                     + "] ";

        }

        return logString;
    }

    private EventLogger getEventLogger()
    {
        return _queue.getEventLogger();
    }

    public class WaitingOnCreditMessageListener implements StateChangeListener<MessageInstance, MessageInstance.EntryState>
    {
        private final AtomicReference<MessageInstance> _entry = new AtomicReference<>();

        public WaitingOnCreditMessageListener()
        {
        }

        public void update(final MessageInstance entry)
        {
            remove();
            // this only happens under send lock so only one thread can be setting to a non null value at any time
            _entry.set(entry);
            entry.addStateChangeListener(this);
            if(!entry.isAvailable())
            {
                _target.notifyWork();
                remove();
            }
        }

        public void remove()
        {
            MessageInstance instance;
            if((instance = _entry.getAndSet(null)) != null)
            {
                instance.removeStateChangeListener(this);
            }

        }

        @Override
        public void stateChanged(MessageInstance entry, MessageInstance.EntryState oldState, MessageInstance.EntryState newState)
        {
            entry.removeStateChangeListener(this);
            _entry.compareAndSet(entry, null);
            _target.notifyWork();
        }

    }
}
