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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.consumer.AbstractConsumerTarget;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstance.ConsumerAcquiredState;
import org.apache.qpid.server.message.MessageInstance.EntryState;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageFlowMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.protocol.v0_10.transport.Option;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.GZIPUtils;
import org.apache.qpid.server.util.StateChangeListener;

public class ConsumerTarget_0_10 extends AbstractConsumerTarget<ConsumerTarget_0_10>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTarget_0_10.class);

    private static final Option[] BATCHED = new Option[] { Option.BATCH };

    private final String _name;
    private final String _targetAddress;


    private volatile FlowCreditManager_0_10 _creditManager;

    private final MessageAcceptMode _acceptMode;
    private final MessageAcquireMode _acquireMode;
    private volatile MessageFlowMode _flowMode;
    private final ServerSession _session;

    private volatile int _deferredMessageCredit;
    private volatile long _deferredSizeCredit;

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

        @Override
        public void run()
        {
            if(_action != null)
            {
                _session.onMessageDispositionChange(_xfr, _action);
            }
        }
    }

    private final AddMessageDispositionListenerAction _postIdSettingAction;

    @Override
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
            if (!serverMsg.checkValid())
            {
                throw new MessageConversionException(String.format("Cannot convert malformed message '%s'", serverMsg));
            }
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


        QpidByteBuffer bodyBuffer = msg.getBody();

        boolean compressionSupported = _session.getConnection().getConnectionDelegate().isCompressionSupported();

        if(msgCompressed && !compressionSupported && bodyBuffer != null)
        {
            QpidByteBuffer uncompressedBuffer = inflateIfPossible(bodyBuffer);
            messageProps.setContentEncoding(null);
            bodyBuffer.dispose();
            bodyBuffer = uncompressedBuffer;
        }
        else if(!msgCompressed
                && compressionSupported
                && (messageProps == null || messageProps.getContentEncoding() == null)
                && bodyBuffer != null
                && bodyBuffer.remaining() > _session.getConnection().getMessageCompressionThreshold())
        {
            QpidByteBuffer compressedBuffers = deflateIfPossible(bodyBuffer);
            if(messageProps == null)
            {
                messageProps = new MessageProperties();
            }
            messageProps.setContentEncoding(GZIPUtils.GZIP_CONTENT_ENCODING);
            bodyBuffer.dispose();
            bodyBuffer = compressedBuffers;
        }

        Header header = new Header(deliveryProps, messageProps, msg.getHeader() == null ? null : msg.getHeader().getNonStandardProperties());

        xfr = batch ? new MessageTransfer(_name, _acceptMode, _acquireMode, header, bodyBuffer, BATCHED)
                    : new MessageTransfer(_name, _acceptMode, _acquireMode, header, bodyBuffer);
        if (bodyBuffer != null)
        {
            bodyBuffer.dispose();
            bodyBuffer = null;
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
                                            @Override
                                            public void onComplete(Method method)
                                            {
                                                deferredAddCredit(1, messageSize);
                                            }
                                        });
        }


        _postIdSettingAction.setXfr(xfr);
        _postIdSettingAction.setAction(null);

        if (_acquireMode == MessageAcquireMode.PRE_ACQUIRED)
        {
            entry.incrementDeliveryCount();
        }

        if(_acceptMode == MessageAcceptMode.EXPLICIT)
        {
            _postIdSettingAction.setAction(new ExplicitAcceptDispositionChangeListener(entry, this, consumer));
        }
        else if(_acquireMode != MessageAcquireMode.PRE_ACQUIRED)
        {
            _postIdSettingAction.setAction(new ImplicitAcceptDispositionChangeListener(entry, this, consumer));
        }

        _session.sendMessage(xfr, _postIdSettingAction);
        xfr.dispose();
        if(converter != null)
        {
            converter.dispose(msg);
        }
        _postIdSettingAction.setAction(null);
        _postIdSettingAction.setXfr(null);

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
        _unacknowledgedBytes.addAndGet(entry.getMessage().getSizeIncludingHeader());
        entry.addStateChangeListener(_unacknowledgedMessageListener);
    }

    private void removeUnacknowledgedMessage(MessageInstance entry)
    {
        _unacknowledgedBytes.addAndGet(-entry.getMessage().getSizeIncludingHeader());
        _unacknowledgedCount.decrementAndGet();
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
                               @Override
                               public void postCommit()
                               {
                                   if (restoreCredit)
                                   {
                                       restoreCredit(entry.getMessage());
                                   }
                                   entry.delete();
                               }

                               @Override
                               public void onRollback()
                               {

                               }
                           });
   }

    void acknowledge(final MessageInstanceConsumer consumer, final MessageInstance entry)
    {
        _session.acknowledge(consumer, this, entry);
    }

    void reject(final MessageInstanceConsumer consumer, final MessageInstance entry)
    {
        if (entry.makeAcquisitionUnstealable(consumer))
        {
            entry.routeToAlternate(null, null, null);
        }
    }

    void release(final MessageInstanceConsumer consumer, final MessageInstance entry)
    {
        if (isMaxDeliveryLimitReached(entry))
        {
            sendToDLQOrDiscard(consumer, entry);
        }
        else
        {
            entry.release(consumer);
        }
    }

    private void sendToDLQOrDiscard(final MessageInstanceConsumer consumer, MessageInstance entry)
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
            }, null, null);
        }
        if (requeues == 0)
        {
            TransactionLogResource owningResource = entry.getOwningResource();
            if(owningResource instanceof Queue)
            {
                final Queue<?> queue = (Queue<?>)owningResource;
                final MessageDestination alternateBindingDestination = queue.getAlternateBindingDestination();

                if(alternateBindingDestination != null)
                {
                    getEventLogger().message(ChannelMessages.DISCARDMSG_NOROUTE(msg.getMessageNumber(),
                                                                           alternateBindingDestination.getName()));
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
        return getSession().getAMQPConnection().getEventLogger();
    }

    private boolean isMaxDeliveryLimitReached(MessageInstance entry)
    {
        final int maxDeliveryLimit = entry.getMaximumDeliveryCount();
        return (maxDeliveryLimit > 0 && entry.getDeliveryCount() >= maxDeliveryLimit);
    }

    @Override
    public boolean allocateCredit(ServerMessage message)
    {
        boolean creditAllocated = _creditManager.useCreditForMessage(message.getSize());
        updateNotifyWorkDesired();
        return creditAllocated;
    }

    @Override
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
                creditManager.addCredit(0L, value);
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

    @Override
    public Session_0_10 getSession()
    {
        return _session.getModelObject();
    }

    public boolean isDurable()
    {
        return false;
    }

    @Override
    public void noMessagesAvailable()
    {
    }

    @Override
    public void flushBatched()
    {
    }

    @Override
    public String getTargetAddress()
    {
        return _targetAddress;
    }

    @Override
    public String toString()
    {
        return "ConsumerTarget_0_10[name=" + _name + ", session=" + _session.toLogString() + "]";
    }


    private QpidByteBuffer deflateIfPossible(final QpidByteBuffer buffer)
    {
        try
        {
            return QpidByteBuffer.deflate(buffer);
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to compress message payload for consumer with gzip, message will be sent as is", e);
            return null;
        }
    }

    private QpidByteBuffer inflateIfPossible(final QpidByteBuffer buffer)
    {
        try
        {
            return QpidByteBuffer.inflate(buffer);
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to decompress message payload for consumer with gzip, message will be sent as is", e);
            return null;
        }
    }

    static abstract class AbstractDispositionChangeListener implements ServerSession.MessageDispositionChangeListener
    {
        final MessageInstance _entry;
        final ConsumerTarget_0_10 _target;
        final MessageInstanceConsumer _consumer;

        AbstractDispositionChangeListener(final MessageInstance entry,
                                          final ConsumerTarget_0_10 target,
                                          final MessageInstanceConsumer consumer)
        {
            _entry = entry;
            _target = target;
            _consumer = consumer;
        }

        @Override
        public final void onRelease(boolean setRedelivered, final boolean closing)
        {
            _target.release(_consumer, _entry);

            if (setRedelivered)
            {
                _entry.setRedelivered();
            }

            if (closing || !setRedelivered)
            {
                _entry.decrementDeliveryCount();
            }
        }

        @Override
        public final void onReject()
        {
            _entry.setRedelivered();
            _target.reject(_consumer, _entry);
        }
    }

    static class ImplicitAcceptDispositionChangeListener extends AbstractDispositionChangeListener
    {

        private static final Logger LOGGER = LoggerFactory.getLogger(ImplicitAcceptDispositionChangeListener.class);


        ImplicitAcceptDispositionChangeListener(final MessageInstance entry,
                                                final ConsumerTarget_0_10 target,
                                                final MessageInstanceConsumer consumer)
        {
            super(entry, target, consumer);
        }

        @Override
        public void onAccept()
        {
            LOGGER.warn("MessageAccept received for message which is using NONE as the accept mode (likely client error)");
        }

        @Override
        public boolean acquire()
        {
            boolean acquired = _entry.acquire(_consumer);
            if(acquired)
            {
                _entry.incrementDeliveryCount();
                _target.addUnacknowledgedMessage(_entry);
            }
            return acquired;
        }
    }

    static class ExplicitAcceptDispositionChangeListener extends AbstractDispositionChangeListener
    {

        ExplicitAcceptDispositionChangeListener(MessageInstance entry,
                                                ConsumerTarget_0_10 target,
                                                final MessageInstanceConsumer consumer)
        {
            super(entry, target, consumer);
        }

        @Override
        public void onAccept()
        {
            _target.acknowledge(_consumer, _entry);
        }
        @Override
        public boolean acquire()
        {
            final boolean acquired = _entry.acquire(_consumer);
            if (acquired)
            {
                _entry.incrementDeliveryCount();
            }
            return acquired;
        }

    }
}
