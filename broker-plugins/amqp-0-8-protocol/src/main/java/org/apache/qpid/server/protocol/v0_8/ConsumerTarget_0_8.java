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
package org.apache.qpid.server.protocol.v0_8;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.qpid.QpidException;
import org.apache.qpid.common.AMQPFilterTypes;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.server.consumer.AbstractConsumerTarget;
import org.apache.qpid.server.flow.FlowCreditManager;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstance.EntryState;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.StateChangeListener;

/**
 * Encapsulation of a subscription to a queue.
 * <p>
 * Ties together the protocol session of a subscriber, the consumer tag
 * that was given out by the broker and the channel id.
 */
public abstract class ConsumerTarget_0_8 extends AbstractConsumerTarget implements FlowCreditManager.FlowCreditManagerListener
{

    private final ClientDeliveryMethod _deliveryMethod;
    private final RecordDeliveryMethod _recordMethod;

    private final AtomicLong _unacknowledgedCount = new AtomicLong(0);
    private final AtomicLong _unacknowledgedBytes = new AtomicLong(0);
    private final List<MessageInstanceConsumer> _consumers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean _needToClose = new AtomicBoolean();
    private final String _targetAddress;


    public static ConsumerTarget_0_8 createBrowserTarget(AMQChannel channel,
                                                         AMQShortString consumerTag, FieldTable filters,
                                                         FlowCreditManager creditManager)
    {
        return new BrowserConsumer(channel, consumerTag, filters, creditManager, channel.getClientDeliveryMethod(), channel.getRecordDeliveryMethod());
    }

    public static ConsumerTarget_0_8 createGetNoAckTarget(final AMQChannel channel,
                                                          final AMQShortString consumerTag,
                                                          final FieldTable filters,
                                                          final FlowCreditManager creditManager,
                                                          final ClientDeliveryMethod deliveryMethod,
                                                          final RecordDeliveryMethod recordMethod)
    {
        return new GetNoAckConsumer(channel, consumerTag, filters, creditManager, deliveryMethod, recordMethod);
    }

    public List<MessageInstanceConsumer> getConsumers()
    {
        return _consumers;
    }


    static final class BrowserConsumer extends ConsumerTarget_0_8
    {
        public BrowserConsumer(AMQChannel channel,
                               AMQShortString consumerTag, FieldTable filters,
                               FlowCreditManager creditManager,
                               ClientDeliveryMethod deliveryMethod,
                               RecordDeliveryMethod recordMethod)
        {
            super(channel, consumerTag,
                  filters, creditManager, deliveryMethod, recordMethod);
        }

        /**
         * This method can be called by each of the publisher threads. As a result all changes to the channel object must be
         * thread safe.
         *
         *
         *
         * @param consumer
         * @param entry
         * @param batch
         * @throws QpidException
         */
        @Override
        public void doSend(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch)
        {
            // We don't decrement the reference here as we don't want to consume the message
            // but we do want to send it to the client.

            synchronized (getChannel())
            {
                long deliveryTag = getChannel().getNextDeliveryTag();
                sendToClient(consumer, entry.getMessage(), entry.getInstanceProperties(), deliveryTag);
            }

        }

    }

    public static ConsumerTarget_0_8 createNoAckTarget(AMQChannel channel,
                                                           AMQShortString consumerTag, FieldTable filters,
                                                           FlowCreditManager creditManager)
    {
        return new NoAckConsumer(channel, consumerTag, filters, creditManager, channel.getClientDeliveryMethod(), channel.getRecordDeliveryMethod());
    }

    public static ConsumerTarget_0_8 createNoAckTarget(AMQChannel channel,
                                                       AMQShortString consumerTag, FieldTable filters,
                                                       FlowCreditManager creditManager,
                                                       ClientDeliveryMethod deliveryMethod,
                                                       RecordDeliveryMethod recordMethod) throws QpidException
    {
        return new NoAckConsumer(channel, consumerTag, filters, creditManager, deliveryMethod, recordMethod);
    }

    public static class NoAckConsumer extends ConsumerTarget_0_8
    {
        private final AutoCommitTransaction _txn;

        public NoAckConsumer(AMQChannel channel,
                             AMQShortString consumerTag, FieldTable filters,
                             FlowCreditManager creditManager,
                             ClientDeliveryMethod deliveryMethod,
                             RecordDeliveryMethod recordMethod)
        {
            super(channel, consumerTag, filters, creditManager, deliveryMethod, recordMethod);

            _txn = new AutoCommitTransaction(channel.getAddressSpace().getMessageStore());
        }

        /**
         * This method can be called by each of the publisher threads. As a result all changes to the channel object must be
         * thread safe.
         *  @param consumer
         * @param entry   The message to send
         * @param batch
         */
        @Override
        public void doSend(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch)
        {
            // if we do not need to wait for client acknowledgements
            // we can decrement the reference count immediately.

            // By doing this _before_ the send we ensure that it
            // doesn't get sent if it can't be dequeued, preventing
            // duplicate delivery on recovery.

            // The send may of course still fail, in which case, as
            // the message is unacked, it will be lost.
            _txn.dequeue(entry.getEnqueueRecord(), NOOP);

            ServerMessage message = entry.getMessage();
            MessageReference ref = message.newReference();
            InstanceProperties props = entry.getInstanceProperties();
            entry.delete();
            long size;
            synchronized (getChannel())
            {
                getChannel().getConnection().setDeferFlush(batch);
                long deliveryTag = getChannel().getNextDeliveryTag();

                size = sendToClient(consumer, message, props, deliveryTag);

            }
            ref.release();

        }

        private static final ServerTransaction.Action NOOP =
                new ServerTransaction.Action()
                {
                    @Override
                    public void postCommit()
                    {
                    }

                    @Override
                    public void onRollback()
                    {
                    }
                };
    }

    /**
     * NoAck Subscription for use with BasicGet method.
     */
    public static final class GetNoAckConsumer extends NoAckConsumer
    {
        public GetNoAckConsumer(AMQChannel channel,
                                AMQShortString consumerTag, FieldTable filters,
                                FlowCreditManager creditManager,
                                ClientDeliveryMethod deliveryMethod,
                                RecordDeliveryMethod recordMethod)
        {
            super(channel, consumerTag, filters, creditManager, deliveryMethod, recordMethod);
        }

    }


    public static ConsumerTarget_0_8 createAckTarget(AMQChannel channel,
                                                         AMQShortString consumerTag, FieldTable filters,
                                                         FlowCreditManager creditManager)
    {
        return new AckConsumer(channel,consumerTag,filters,creditManager, channel.getClientDeliveryMethod(), channel.getRecordDeliveryMethod());
    }


    public static ConsumerTarget_0_8 createAckTarget(AMQChannel channel,
                                                         AMQShortString consumerTag, FieldTable filters,
                                                         FlowCreditManager creditManager,
                                                         ClientDeliveryMethod deliveryMethod,
                                                         RecordDeliveryMethod recordMethod)
    {
        return new AckConsumer(channel,consumerTag,filters,creditManager, deliveryMethod, recordMethod);
    }

    static final class AckConsumer extends ConsumerTarget_0_8
    {
        public AckConsumer(AMQChannel channel,
                           AMQShortString consumerTag, FieldTable filters,
                           FlowCreditManager creditManager,
                           ClientDeliveryMethod deliveryMethod,
                           RecordDeliveryMethod recordMethod)
        {
            super(channel, consumerTag, filters, creditManager, deliveryMethod, recordMethod);
        }

        /**
         * This method can be called by each of the publisher threads. As a result all changes to the channel object must be
         * thread safe.
         *  @param consumer
         * @param entry   The message to send
         * @param batch
         */
        @Override
        public void doSend(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch)
        {

            // put queue entry on a list and then notify the connection to read list.

            synchronized (getChannel())
            {
                getChannel().getConnection().setDeferFlush(batch);
                long deliveryTag = getChannel().getNextDeliveryTag();

                addUnacknowledgedMessage(entry);
                recordMessageDelivery(consumer, entry, deliveryTag);
                long size = sendToClient(consumer, entry.getMessage(), entry.getInstanceProperties(), deliveryTag);
                entry.incrementDeliveryCount();
            }


        }





    }

    private final AMQChannel _channel;

    private final AMQShortString _consumerTag;

    private final FlowCreditManager _creditManager;

    private final Boolean _autoClose;

    private final AtomicBoolean _deleted = new AtomicBoolean(false);




    public ConsumerTarget_0_8(AMQChannel channel,
                              AMQShortString consumerTag,
                              FieldTable arguments,
                              FlowCreditManager creditManager,
                              ClientDeliveryMethod deliveryMethod,
                              RecordDeliveryMethod recordMethod)
    {
        super(State.ACTIVE);

        _channel = channel;
        _consumerTag = consumerTag;

        _creditManager = creditManager;
        creditManager.addStateListener(this);

        _deliveryMethod = deliveryMethod;
        _recordMethod = recordMethod;

        if (arguments != null)
        {
            Object autoClose = arguments.get(AMQPFilterTypes.AUTO_CLOSE.getValue());
            if (autoClose != null)
            {
                _autoClose = (Boolean) autoClose;
            }
            else
            {
                _autoClose = false;
            }
            if(arguments.containsKey("local-address"))
            {
                _targetAddress = String.valueOf(arguments.get("local-address"));
            }
            else
            {
                _targetAddress = consumerTag.toString();
            }
        }
        else
        {
            _autoClose = false;
            _targetAddress = consumerTag.toString();

        }
    }

    @Override
    public void consumerRemoved(final MessageInstanceConsumer consumer)
    {
        _consumers.remove(consumer);
        if(_consumers.isEmpty())
        {
            close();
        }
    }

    @Override
    public void consumerAdded(final MessageInstanceConsumer consumer)
    {
        _consumers.add(consumer);
    }

    @Override
    public String getTargetAddress()
    {
        return _targetAddress;
    }

    public AMQSessionModel getSessionModel()
    {
        return _channel;
    }

    public String toString()
    {

        return "ConsumerTarget_0_8[channel=" + _channel +
                            ", consumerTag=" + _consumerTag +
                            ", session=" + getConnection().getRemoteAddressString() + "]";
    }

    @Override
    public boolean isFlowSuspended()
    {
        return getState()!=State.ACTIVE || _channel.isSuspended() || _deleted.get() || _channel.getAMQPConnection().isConnectionStopped();
    }

    /**
     * Callback indicating that a queue has been deleted.
     *
     */
    public void queueDeleted()
    {
        _deleted.set(true);
    }

    public boolean isAutoClose()
    {
        return _autoClose;
    }

    public FlowCreditManager getCreditManager()
    {
        return _creditManager;
    }

    @Override
    protected void afterCloseInternal()
    {

    }

    @Override
    protected void doCloseInternal()
    {
        _creditManager.removeListener(this);
    }

    public boolean allocateCredit(ServerMessage msg)
    {
        return _creditManager.useCreditForMessage(msg.getSize());
    }

    public AMQChannel getChannel()
    {
        return _channel;
    }

    public AMQShortString getConsumerTag()
    {
        return _consumerTag;
    }

    private AMQPConnection_0_8 getConnection()
    {
        return _channel.getConnection();
    }

    public void restoreCredit(final ServerMessage message)
    {
        _creditManager.restoreCredit(1, message.getSize());
    }

    public void creditStateChanged(boolean hasCredit)
    {

        if(hasCredit)
        {
            if(!updateState(State.SUSPENDED, State.ACTIVE))
            {
                // this is a hack to get round the issue of increasing bytes credit
                notifyCurrentState();
            }
        }
        else
        {
            updateState(State.ACTIVE, State.SUSPENDED);
        }
    }

    protected long sendToClient(final MessageInstanceConsumer consumer, final ServerMessage message,
                                final InstanceProperties props,
                                final long deliveryTag)
    {
        return _deliveryMethod.deliverToClient(consumer, message, props, deliveryTag);

    }


    protected void recordMessageDelivery(final MessageInstanceConsumer consumer,
                                         final MessageInstance entry,
                                         final long deliveryTag)
    {
        _recordMethod.recordMessageDelivery(consumer, entry, deliveryTag);
    }


    public void confirmAutoClose()
    {
        ProtocolOutputConverter converter = getChannel().getConnection().getProtocolOutputConverter();
        converter.confirmConsumerAutoClose(getChannel().getChannelId(), getConsumerTag());
    }

    public void queueEmpty()
    {
        if (isAutoClose())
        {
            _needToClose.set(true);
            getChannel().getConnection().notifyWork();
        }
    }

    @Override
    protected void processClosed()
    {
        if (hasClosed())
        {
            close();
            confirmAutoClose();
        }
    }

    @Override
    protected void processStateChanged()
    {

    }

    @Override
    protected boolean hasStateChanged()
    {
        return false;
    }

    @Override
    protected boolean hasClosed()
    {
        return (_needToClose.get() && getState() != State.CLOSED);
    }

    public void flushBatched()
    {
        _channel.getConnection().setDeferFlush(false);
    }

    protected void addUnacknowledgedMessage(MessageInstance entry)
    {
        final long size = entry.getMessage().getSize();
        _unacknowledgedBytes.addAndGet(size);
        _unacknowledgedCount.incrementAndGet();
        entry.addStateChangeListener(_unacknowledgedMessageListener);
    }

    private void removeUnacknowledgedMessage(MessageInstance entry)
    {

        final long _size = entry.getMessage().getSize();
        _unacknowledgedBytes.addAndGet(-_size);
        _unacknowledgedCount.decrementAndGet();

        _creditManager.restoreCredit(1, _size);
    }

    @Override
    public void acquisitionRemoved(final MessageInstance node)
    {
    }

    public long getUnacknowledgedBytes()
    {
        return _unacknowledgedBytes.longValue();
    }

    public long getUnacknowledgedMessages()
    {
        return _unacknowledgedCount.longValue();
    }

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
            return state instanceof MessageInstance.ConsumerAcquiredState
                   && ((MessageInstance.ConsumerAcquiredState) state).getConsumer().getTarget() == ConsumerTarget_0_8.this;
        }
    };
}
