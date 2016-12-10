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
package org.apache.qpid.server.protocol.v0_10;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.consumer.AbstractConsumerTarget;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstance.ConsumerAcquiredState;
import org.apache.qpid.server.message.MessageInstance.EntryState;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.StateChangeListener;
import org.apache.qpid.transport.DeliveryProperties;
import org.apache.qpid.transport.Header;
import org.apache.qpid.transport.MessageAcceptMode;
import org.apache.qpid.transport.MessageAcquireMode;
import org.apache.qpid.transport.MessageCreditUnit;
import org.apache.qpid.transport.MessageFlowMode;
import org.apache.qpid.transport.MessageProperties;
import org.apache.qpid.transport.MessageTransfer;
import org.apache.qpid.transport.Method;
import org.apache.qpid.transport.Option;
import org.apache.qpid.util.ByteBufferUtils;
import org.apache.qpid.util.GZIPUtils;

public class ConsumerTarget_0_10 extends AbstractConsumerTarget<ConsumerTarget_0_10>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTarget_0_10.class);

    private static final Option[] BATCHED = new Option[] { Option.BATCH };

    private final String _name;
    private final String _targetAddress;


    private FlowCreditManager_0_10 _creditManager;

    private final MessageAcceptMode _acceptMode;
    private final MessageAcquireMode _acquireMode;
    private MessageFlowMode _flowMode;
    private final ServerSession _session;
    private final AtomicBoolean _stopped = new AtomicBoolean(true);

    private final AtomicLong _unacknowledgedCount = new AtomicLong(0);
    private final AtomicLong _unacknowledgedBytes = new AtomicLong(0);

    private int _deferredMessageCredit;
    private long _deferredSizeCredit;

    private final StateChangeListener<MessageInstance, EntryState> _unacknowledgedMessageListener = new StateChangeListener<MessageInstance, EntryState>()
    {

        @Override
        public void stateChanged(MessageInstance entry, EntryState oldState, EntryState newState)
        {
            if (isConsumerAcquiredStateForThis(oldState) && !isConsumerAcquiredStateForThis(newState))
            {
                removeUnacknowledgedMessage(entry);
                entry.removeStateChangeListener(this);
            }
        }

        private boolean isConsumerAcquiredStateForThis(EntryState state)
        {
            return state instanceof ConsumerAcquiredState
                   && ((ConsumerAcquiredState) state).getConsumer().getTarget() == ConsumerTarget_0_10.this;
        }
    };

    public ConsumerTarget_0_10(ServerSession session,
                               String name,
                               MessageAcceptMode acceptMode,
                               MessageAcquireMode acquireMode,
                               MessageFlowMode flowMode,
                               FlowCreditManager_0_10 creditManager,
                               Map<String, Object> arguments,
                               boolean multiQueue)
    {
        super(multiQueue, session.getAMQPConnection());
        _session = session;
        _postIdSettingAction = new AddMessageDispositionListenerAction(session);
        _acceptMode = acceptMode;
        _acquireMode = acquireMode;
        _creditManager = creditManager;
        _flowMode = flowMode;
        _name = name;
        if(arguments != null && arguments.containsKey("local-address"))
        {
            _targetAddress = String.valueOf(arguments.get("local-address"));
        }
        else
        {
            _targetAddress = name;
        }
    }

    @Override
    public void updateNotifyWorkDesired()
    {
        final AMQPConnection_0_10 amqpConnection = _session.getAMQPConnection();

        boolean state = !amqpConnection.isTransportBlockedForWriting()
                        && getCreditManager().hasCredit();

        setNotifyWorkDesired(state);
    }

    public String getName()
    {
        return _name;
    }

    public void transportStateChanged()
    {
        _creditManager.restoreCredit(0, 0);
        updateNotifyWorkDesired();
    }

    public static class AddMessageDispositionListenerAction implements Runnable
    {
        private MessageTransfer _xfr;
        private ServerSession.MessageDispositionChangeListener _action;
        private ServerSession _session;

        public AddMessageDispositionListenerAction(ServerSession session)
        {
            _session = session;
        }

        public void setXfr(MessageTransfer xfr)
        {
            _xfr = xfr;
        }

        public void setAction(ServerSession.MessageDispositionChangeListener action)
        {
            _action = action;
        }

        public void run()
        {
            if(_action != null)
            {
                _session.onMessageDispositionChange(_xfr, _action);
            }
        }
    }

    private final AddMessageDispositionListenerAction _postIdSettingAction;

    public void doSend(final MessageInstanceConsumer consumer, final MessageInstance entry, boolean batch)
    {
        ServerMessage serverMsg = entry.getMessage();


        MessageTransfer xfr;

        DeliveryProperties deliveryProps;
        MessageProperties messageProps = null;

        MessageTransferMessage msg;
        MessageConverter<? super ServerMessage, MessageTransferMessage> converter = null;

        if(serverMsg instanceof MessageTransferMessage)
        {

            msg = (MessageTransferMessage) serverMsg;

        }
        else
        {
            converter = (MessageConverter<? super ServerMessage, MessageTransferMessage>) MessageConverterRegistry.getConverter(serverMsg.getClass(), MessageTransferMessage.class);
            msg = converter.convert(serverMsg, _session.getAddressSpace());
        }

        DeliveryProperties origDeliveryProps = msg.getHeader() == null ? null : msg.getHeader().getDeliveryProperties();
        messageProps = msg.getHeader() == null ? null : msg.getHeader().getMessageProperties();

        deliveryProps = new DeliveryProperties();
        if(origDeliveryProps != null)
        {
            if(origDeliveryProps.hasDeliveryMode())
            {
                deliveryProps.setDeliveryMode(origDeliveryProps.getDeliveryMode());
            }
            if(origDeliveryProps.hasExchange())
            {
                deliveryProps.setExchange(origDeliveryProps.getExchange());
            }
            if(origDeliveryProps.hasExpiration())
            {
                deliveryProps.setExpiration(origDeliveryProps.getExpiration());
            }
            if(origDeliveryProps.hasPriority())
            {
                deliveryProps.setPriority(origDeliveryProps.getPriority());
            }
            if(origDeliveryProps.hasRoutingKey())
            {
                deliveryProps.setRoutingKey(origDeliveryProps.getRoutingKey());
            }
            if(origDeliveryProps.hasTimestamp())
            {
                deliveryProps.setTimestamp(origDeliveryProps.getTimestamp());
            }
            if(origDeliveryProps.hasTtl())
            {
                deliveryProps.setTtl(origDeliveryProps.getTtl());
            }


        }

        deliveryProps.setRedelivered(entry.isRedelivered());

        boolean msgCompressed = messageProps != null && GZIPUtils.GZIP_CONTENT_ENCODING.equals(messageProps.getContentEncoding());


        Collection<QpidByteBuffer> bodyBuffers = msg.getBody();

        boolean compressionSupported = _session.getConnection().getConnectionDelegate().isCompressionSupported();

        if(msgCompressed && !compressionSupported && bodyBuffers != null)
        {
            Collection<QpidByteBuffer> uncompressedBuffers = inflateIfPossible(bodyBuffers);
            messageProps.setContentEncoding(null);
            for (QpidByteBuffer buf : bodyBuffers)
            {
                buf.dispose();
            }
            bodyBuffers = uncompressedBuffers;
        }
        else if(!msgCompressed
                && compressionSupported
                && (messageProps == null || messageProps.getContentEncoding() == null)
                && bodyBuffers != null
                && ByteBufferUtils.remaining(bodyBuffers) > _session.getConnection().getMessageCompressionThreshold())
        {
            Collection<QpidByteBuffer> compressedBuffers = deflateIfPossible(bodyBuffers);
            if(messageProps == null)
            {
                messageProps = new MessageProperties();
            }
            messageProps.setContentEncoding(GZIPUtils.GZIP_CONTENT_ENCODING);
            for (QpidByteBuffer buf : bodyBuffers)
            {
                buf.dispose();
            }
            bodyBuffers = compressedBuffers;
        }

        Header header = new Header(deliveryProps, messageProps, msg.getHeader() == null ? null : msg.getHeader().getNonStandardProperties());

        xfr = batch ? new MessageTransfer(_name,_acceptMode,_acquireMode,header, bodyBuffers, BATCHED)
                    : new MessageTransfer(_name,_acceptMode,_acquireMode,header, bodyBuffers);
        if (bodyBuffers != null)
        {
            for (QpidByteBuffer buf : bodyBuffers)
            {
                buf.dispose();
            }
            bodyBuffers = null;
        }
        if(_acceptMode == MessageAcceptMode.NONE && _acquireMode != MessageAcquireMode.PRE_ACQUIRED)
        {
            xfr.setCompletionListener(new MessageAcceptCompletionListener(this, consumer, _session, entry, _flowMode == MessageFlowMode.WINDOW));
        }
        else if(_flowMode == MessageFlowMode.WINDOW)
        {
            final long messageSize = entry.getMessage().getSize();
            xfr.setCompletionListener(new Method.CompletionListener()
                                        {
                                            public void onComplete(Method method)
                                            {
                                                deferredAddCredit(1, messageSize);
                                            }
                                        });
        }


        _postIdSettingAction.setXfr(xfr);
        if(_acceptMode == MessageAcceptMode.EXPLICIT)
        {
            _postIdSettingAction.setAction(new ExplicitAcceptDispositionChangeListener(entry, this, consumer));
        }
        else if(_acquireMode != MessageAcquireMode.PRE_ACQUIRED)
        {
            _postIdSettingAction.setAction(new ImplicitAcceptDispositionChangeListener(entry, this, consumer));
        }
        else
        {
            _postIdSettingAction.setAction(null);
        }


        _session.sendMessage(xfr, _postIdSettingAction);
        xfr.dispose();
        if(converter != null)
        {
            converter.dispose(msg);
        }
        _postIdSettingAction.setAction(null);
        _postIdSettingAction.setXfr(null);

        entry.incrementDeliveryCount();
        if(_acceptMode == MessageAcceptMode.NONE && _acquireMode == MessageAcquireMode.PRE_ACQUIRED)
        {
            forceDequeue(entry, false);
        }
        else if(_acquireMode == MessageAcquireMode.PRE_ACQUIRED)
        {
            addUnacknowledgedMessage(entry);
        }
    }

    void addUnacknowledgedMessage(MessageInstance entry)
    {
        _unacknowledgedCount.incrementAndGet();
        _unacknowledgedBytes.addAndGet(entry.getMessage().getSize());
        entry.addStateChangeListener(_unacknowledgedMessageListener);
    }

    private void removeUnacknowledgedMessage(MessageInstance entry)
    {
        _unacknowledgedBytes.addAndGet(-entry.getMessage().getSize());
        _unacknowledgedCount.decrementAndGet();
    }

    @Override
    public void acquisitionRemoved(final MessageInstance entry)
    {
    }

    private void deferredAddCredit(final int deferredMessageCredit, final long deferredSizeCredit)
    {
        _deferredMessageCredit += deferredMessageCredit;
        _deferredSizeCredit += deferredSizeCredit;

    }

    public void flushCreditState(boolean strict)
    {
        if(strict || !isSuspended() || _deferredMessageCredit >= 200
           || !(_creditManager instanceof WindowCreditManager)
           || ((WindowCreditManager)_creditManager).getMessageCreditLimit() < 400 )
        {
            restoreCredit(_deferredMessageCredit, _deferredSizeCredit);

            _deferredMessageCredit = 0;
            _deferredSizeCredit = 0l;
        }
    }

    private void forceDequeue(final MessageInstance entry, final boolean restoreCredit)
    {
        AutoCommitTransaction dequeueTxn = new AutoCommitTransaction(_session.getAddressSpace().getMessageStore());
        dequeueTxn.dequeue(entry.getEnqueueRecord(),
                           new ServerTransaction.Action()
                           {
                               public void postCommit()
                               {
                                   if (restoreCredit)
                                   {
                                       restoreCredit(entry.getMessage());
                                   }
                                   entry.delete();
                               }

                               public void onRollback()
                               {

                               }
                           });
   }

    void reject(final MessageInstanceConsumer consumer, final MessageInstance entry)
    {
        entry.setRedelivered();
        if (entry.makeAcquisitionUnstealable(consumer))
        {
            entry.routeToAlternate(null, null);
        }
    }

    void release(final MessageInstanceConsumer consumer,
                 final MessageInstance entry,
                 final boolean setRedelivered)
    {
        if (setRedelivered)
        {
            entry.setRedelivered();
        }

        if (getSessionModel().isClosing() || !setRedelivered)
        {
            entry.decrementDeliveryCount();
        }

        if (isMaxDeliveryLimitReached(entry))
        {
            sendToDLQOrDiscard(consumer, entry);
        }
        else
        {
            entry.release(consumer);
        }
    }

    protected void sendToDLQOrDiscard(final MessageInstanceConsumer consumer, MessageInstance entry)
    {
        final ServerMessage msg = entry.getMessage();

        int requeues = 0;
        if (entry.makeAcquisitionUnstealable(consumer))
        {
            requeues = entry.routeToAlternate(new Action<MessageInstance>()
            {
                @Override
                public void performAction(final MessageInstance requeueEntry)
                {
                    getEventLogger().message(ChannelMessages.DEADLETTERMSG(msg.getMessageNumber(),
                                                                           requeueEntry.getOwningResource()
                                                                                   .getName()));
                }
            }, null);
        }
        if (requeues == 0)
        {
            TransactionLogResource owningResource = entry.getOwningResource();
            if(owningResource instanceof Queue)
            {
                final Queue<?> queue = (Queue<?>)owningResource;
                final Exchange alternateExchange = queue.getAlternateExchange();

                if(alternateExchange != null)
                {
                    getEventLogger().message(ChannelMessages.DISCARDMSG_NOROUTE(msg.getMessageNumber(),
                                                                           alternateExchange.getName()));
                }
                else
                {
                    getEventLogger().message(ChannelMessages.DISCARDMSG_NOALTEXCH(msg.getMessageNumber(),
                                                                             queue.getName(),
                                                                             msg.getInitialRoutingAddress()));
                }
            }
        }
    }

    protected EventLogger getEventLogger()
    {
        return getSessionModel().getAMQPConnection().getEventLogger();
    }

    private boolean isMaxDeliveryLimitReached(MessageInstance entry)
    {
        final int maxDeliveryLimit = entry.getMaximumDeliveryCount();
        return (maxDeliveryLimit > 0 && entry.getDeliveryCount() >= maxDeliveryLimit);
    }

    public boolean allocateCredit(ServerMessage message)
    {
        boolean creditAllocated = _creditManager.useCreditForMessage(message.getSize());
        updateNotifyWorkDesired();
        return creditAllocated;
    }

    public void restoreCredit(ServerMessage message)
    {
        restoreCredit(1, message.getSize());
    }

    void restoreCredit(int count, long size)
    {
        _creditManager.restoreCredit(count, size);
        updateNotifyWorkDesired();
    }



    public FlowCreditManager_0_10 getCreditManager()
    {
        return _creditManager;
    }

    public void stop()
    {
        getCreditManager().clearCredit();
        updateNotifyWorkDesired();
    }

    public void addCredit(MessageCreditUnit unit, long value)
    {
        FlowCreditManager_0_10 creditManager = getCreditManager();
        switch (unit)
        {
            case MESSAGE:

                creditManager.addCredit(value, 0L);
                break;
            case BYTE:
                creditManager.addCredit(0l, value);
                break;
        }
        updateNotifyWorkDesired();
    }

    public void setFlowMode(MessageFlowMode flowMode)
    {
        switch(flowMode)
        {
            case CREDIT:
                _creditManager = new CreditCreditManager(0l, 0l);
                break;
            case WINDOW:
                _creditManager = new WindowCreditManager(0l, 0l);
                break;
            default:
                // this should never happen, as 0-10 is finalised and so the enum should never change
                throw new ConnectionScopedRuntimeException("Unknown message flow mode: " + flowMode);
        }
        _flowMode = flowMode;
        updateNotifyWorkDesired();
    }

    public boolean isFlowModeChangeAllowed()
    {
        return !_creditManager.hasCredit();
    }

    public void flush()
    {
        flushCreditState(true);
        while(sendNextMessage());
        stop();
    }

    public ServerSession getSessionModel()
    {
        return _session;
    }

    public boolean isDurable()
    {
        return false;
    }

    public void queueEmpty()
    {
    }

    public void flushBatched()
    {
    }

    @Override
    public String getTargetAddress()
    {
        return _targetAddress;
    }

    public long getUnacknowledgedBytes()
    {
        return _unacknowledgedBytes.longValue();
    }

    public long getUnacknowledgedMessages()
    {
        return _unacknowledgedCount.longValue();
    }

    @Override
    public String toString()
    {
        return "ConsumerTarget_0_10[name=" + _name + ", session=" + _session.toLogString() + "]";
    }


    private Collection<QpidByteBuffer> deflateIfPossible(final Collection<QpidByteBuffer> buffers)
    {
        try
        {
            return QpidByteBuffer.deflate(buffers);
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to compress message payload for consumer with gzip, message will be sent as is", e);
            return null;
        }
    }

    private Collection<QpidByteBuffer> inflateIfPossible(final Collection<QpidByteBuffer> buffers)
    {
        try
        {
            return QpidByteBuffer.inflate(buffers);
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to decompress message payload for consumer with gzip, message will be sent as is", e);
            return null;
        }
    }

}
