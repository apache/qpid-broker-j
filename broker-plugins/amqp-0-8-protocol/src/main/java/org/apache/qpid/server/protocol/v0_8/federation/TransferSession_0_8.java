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
package org.apache.qpid.server.protocol.v0_8.federation;

import java.security.AccessControlContext;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.framing.AMQBody;
import org.apache.qpid.framing.AMQFrame;
import org.apache.qpid.framing.AMQMethodBody;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.BasicGetOkBody;
import org.apache.qpid.framing.ConfirmSelectBody;
import org.apache.qpid.framing.ConfirmSelectOkBody;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.MethodRegistry;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.MessageContentSourceBody;
import org.apache.qpid.server.transfer.TransferQueueConsumer;
import org.apache.qpid.server.transfer.TransferQueueEntry;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

class TransferSession_0_8 implements OutboundChannel
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransferSession_0_8.class);
    private static final Runnable NO_OP = new Runnable()
    {
        @Override
        public void run()
        {

        }
    };

    private final OutboundConnection_0_8 _connection;
    private final int _channelId;
    private final MethodRegistry _methodRegistry;
    private final LocalTransaction _txn;
    private Collection<String> _remoteHostGlobalDomains;
    private TransferTarget_0_8 _target;
    private TransferQueueConsumer _consumer;
    private final SortedMap<Long, TransferQueueEntry> _unconfirmed = new TreeMap<>();
    private volatile boolean _flowControlled;
    private volatile long _lastSent = -0L;

    private int _maxUnconfirmed = 200;

    enum Operation {
        EXCHANGE_DECLARE,
        EXCHANGE_DELETE,
        EXCHANGE_BOUND,
        QUEUE_BIND,
        QUEUE_UNBIND,
        QUEUE_DECLARE,
        QUEUE_DELETE,
        QUEUE_PURGE,
        BASIC_RECOVER_SYNC,
        BAISC_QOS,
        BASIC_CONSUME,
        BASIC_CANCEL,
        TX_SELECT,
        TX_COMMIT,
        TX_ROLLBACK,
        CHANNEL_FLOW,
        CONFIRM_SELECT,
        ACK,
        BASIC_GET
    }

    private static final class TimedSettableFuture<V> extends AbstractFuture<V>
    {
        private final long _timeoutTime;

        private TimedSettableFuture(final long timeoutTime)
        {
            _timeoutTime = timeoutTime;
        }

        public boolean set(V value)
        {
            return super.set(value);
        }

        @Override
        public boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        public long getTimeoutTime()
        {
            return _timeoutTime;
        }

        public boolean checkTimeout(long time)
        {
            if(time > _timeoutTime)
            {
                setException(new TimeoutException("Timed out waiting for response"));
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    private final Map<Operation, Queue<TimedSettableFuture<AMQMethodBody>>> _synchronousOperationListeners = new EnumMap<>(Operation.class);

    {
        for(Operation op : Operation.values())
        {
            _synchronousOperationListeners.put(op, new LinkedList<TimedSettableFuture<AMQMethodBody>>());
        }
    }

    enum State { AWAITING_OPEN, OPEN, AWAITING_CLOSE_OK }

    private State _state = State.AWAITING_OPEN;

    private interface MessageHandler
    {
        void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize);

        void receiveMessageContent(QpidByteBuffer data);
    }


    private final MessageHandler _unexpectedMessageHandler = new MessageHandler()
    {
        @Override
        public void receiveMessageHeader(final BasicContentHeaderProperties properties, final long bodySize)
        {
            throw new ConnectionScopedRuntimeException("Unexpected frame");
        }

        @Override
        public void receiveMessageContent(final QpidByteBuffer data)
        {
            throw new ConnectionScopedRuntimeException("Unexpected frame");
        }
    };

    private volatile MessageHandler _messageHandler = _unexpectedMessageHandler;


    TransferSession_0_8(final int channelId, final OutboundConnection_0_8 outboundConnection)
    {
        _connection = outboundConnection;
        _channelId = channelId;
        _methodRegistry = outboundConnection.getMethodRegistry();
        writeMethod(_methodRegistry.createChannelOpenBody(AMQShortString.EMPTY_STRING));
        _txn = new LocalTransaction(outboundConnection.getVirtualHost().getMessageStore());
    }

    private synchronized void changeState(final State currentState, final State newState)
    {
        if(_state != currentState)
        {
            throw new ConnectionScopedRuntimeException("Incorrect state");
        }
        _state = newState;
    }

    private synchronized void assertState(final State currentState)
    {
        if (_state != currentState)
        {
            throw new ConnectionScopedRuntimeException("Incorrect state");
        }
    }


    @Override
    public void receiveChannelOpenOk()
    {
        changeState(State.AWAITING_OPEN, State.OPEN);

        writeMethod(new ConfirmSelectBody(true));

        writeMethod(_methodRegistry.createBasicGetBody(0, AMQShortString.valueOf("$virtualhostProperties"), true));
        final FutureCallback<AMQMethodBody> callback = new FutureCallback<AMQMethodBody>()
        {
            @Override
            public void onSuccess(final AMQMethodBody result)
            {
                if (result instanceof BasicGetOkBody)
                {
                    _messageHandler = new VirtualHostPropertiesHandler();
                }
                else
                {
                    throw new ConnectionScopedRuntimeException("Unable to determine virtual host properties from remote host");
                }
            }

            @Override
            public void onFailure(final Throwable t)
            {
                if(t instanceof RuntimeException)
                {
                    throw ((RuntimeException)t);
                }
                else
                {
                    throw new ConnectionScopedRuntimeException(t);
                }
            }
        };
        addOperationResponse(Operation.BASIC_GET, callback);
    }

    private void addOperationResponse(Operation operation, FutureCallback<AMQMethodBody> callback)
    {
        final TimedSettableFuture<AMQMethodBody> future =
                new TimedSettableFuture<>(System.currentTimeMillis() + 30000L);
        Futures.addCallback(future, callback);
        _synchronousOperationListeners.get(operation).add(future);
    }

    private void performOperationResponse(Operation operation, AMQMethodBody body)
    {
        final TimedSettableFuture<AMQMethodBody> future = _synchronousOperationListeners.get(operation).poll();

        if(future == null)
        {
            throw new ConnectionScopedRuntimeException("Unexpected frame ");
        }
        else
        {
            future.set(body);
        }
    }


    private void setRemoteHostGlobalDomains(final Collection<String> remoteHostGlobalDomains)
    {
        LOGGER.debug("Setting remote host global domains: {}", remoteHostGlobalDomains);
        _remoteHostGlobalDomains = remoteHostGlobalDomains;

        _target = new TransferTarget_0_8(this, remoteHostGlobalDomains);
        _consumer = _connection.getVirtualHost().getTransferQueue().addConsumer(_target, "consumer");
    }



    @Override
    public void receiveChannelAlert(final int replyCode, final AMQShortString replyText, final FieldTable details)
    {

    }

    @Override
    public void receiveAccessRequestOk(final int ticket)
    {

    }

    @Override
    public void receiveExchangeDeclareOk()
    {
        performOperationResponse(Operation.EXCHANGE_DECLARE, _methodRegistry.createExchangeDeclareOkBody());
    }

    @Override
    public void receiveExchangeDeleteOk()
    {
        performOperationResponse(Operation.EXCHANGE_DELETE, _methodRegistry.createExchangeDeleteOkBody());
    }

    @Override
    public void receiveExchangeBoundOk(final int replyCode, final AMQShortString replyText)
    {
        performOperationResponse(Operation.EXCHANGE_BOUND, _methodRegistry.createExchangeBoundOkBody(replyCode, replyText));
    }

    @Override
    public void receiveQueueBindOk()
    {
        performOperationResponse(Operation.QUEUE_BIND, _methodRegistry.createQueueBindOkBody());
    }

    @Override
    public void receiveQueueUnbindOk()
    {
        performOperationResponse(Operation.QUEUE_UNBIND, _methodRegistry.createQueueUnbindOkBody());
    }

    @Override
    public void receiveQueueDeclareOk(final AMQShortString queue, final long messageCount, final long consumerCount)
    {
        performOperationResponse(Operation.QUEUE_DECLARE, _methodRegistry.createQueueDeclareOkBody(queue, messageCount, consumerCount));
    }

    @Override
    public void receiveQueuePurgeOk(final long messageCount)
    {
        performOperationResponse(Operation.QUEUE_PURGE, _methodRegistry.createQueuePurgeOkBody(messageCount));

    }

    @Override
    public void receiveQueueDeleteOk(final long messageCount)
    {
        performOperationResponse(Operation.QUEUE_DELETE, _methodRegistry.createQueueDeleteOkBody(messageCount));

    }

    @Override
    public void receiveBasicRecoverSyncOk()
    {
        performOperationResponse(Operation.BASIC_RECOVER_SYNC, _methodRegistry.createBasicRecoverSyncOkBody());
    }

    @Override
    public void receiveBasicQosOk()
    {
        performOperationResponse(Operation.BAISC_QOS, _methodRegistry.createBasicQosOkBody());
    }

    @Override
    public void receiveBasicConsumeOk(final AMQShortString consumerTag)
    {
        performOperationResponse(Operation.BASIC_CONSUME, _methodRegistry.createBasicConsumeOkBody(consumerTag));
    }

    @Override
    public void receiveBasicCancelOk(final AMQShortString consumerTag)
    {
        performOperationResponse(Operation.BASIC_CANCEL, _methodRegistry.createBasicCancelOkBody(consumerTag));
    }

    @Override
    public void receiveBasicReturn(final int replyCode,
                                   final AMQShortString replyText,
                                   final AMQShortString exchange,
                                   final AMQShortString routingKey)
    {

    }

    @Override
    public void receiveBasicDeliver(final AMQShortString consumerTag,
                                    final long deliveryTag,
                                    final boolean redelivered,
                                    final AMQShortString exchange,
                                    final AMQShortString routingKey)
    {

    }

    @Override
    public void receiveBasicGetOk(final long deliveryTag,
                                  final boolean redelivered,
                                  final AMQShortString exchange,
                                  final AMQShortString routingKey,
                                  final long messageCount)
    {
        performOperationResponse(Operation.BASIC_GET, _methodRegistry.createBasicGetOkBody(deliveryTag, redelivered, exchange, routingKey, messageCount));
    }

    @Override
    public void receiveBasicGetEmpty()
    {
        performOperationResponse(Operation.BASIC_GET, _methodRegistry.createBasicGetEmptyBody(AMQShortString.EMPTY_STRING));
    }

    @Override
    public void receiveTxSelectOk()
    {
        performOperationResponse(Operation.TX_SELECT, _methodRegistry.createTxSelectOkBody());
    }

    @Override
    public void receiveTxCommitOk()
    {
        performOperationResponse(Operation.TX_COMMIT, _methodRegistry.createTxCommitOkBody());
    }

    @Override
    public void receiveTxRollbackOk()
    {
        performOperationResponse(Operation.TX_ROLLBACK, _methodRegistry.createTxRollbackOkBody());
    }

    @Override
    public void receiveConfirmSelectOk()
    {
        performOperationResponse(Operation.CONFIRM_SELECT, ConfirmSelectOkBody.INSTANCE);
    }

    @Override
    public void receiveChannelFlow(final boolean active)
    {
        _flowControlled = !active;
        writeMethod(_methodRegistry.createChannelFlowOkBody(active));

    }

    @Override
    public void receiveChannelFlowOk(final boolean active)
    {
        performOperationResponse(Operation.CHANNEL_FLOW, _methodRegistry.createChannelFlowOkBody(active));
    }

    @Override
    public void receiveChannelClose(final int replyCode,
                                    final AMQShortString replyText,
                                    final int classId,
                                    final int methodId)
    {

    }

    @Override
    public void receiveChannelCloseOk()
    {
    }

    @Override
    public void receiveMessageContent(final QpidByteBuffer data)
    {
        _messageHandler.receiveMessageContent(data);
    }

    @Override
    public void receiveMessageHeader(final BasicContentHeaderProperties properties, final long bodySize)
    {
        _messageHandler.receiveMessageHeader(properties, bodySize);
    }

    @Override
    public boolean ignoreAllButCloseOk()
    {
        return false;
    }

    @Override
    public void receiveBasicNack(final long deliveryTag, final boolean multiple, final boolean requeue)
    {
        if(multiple)
        {
            Iterator<Map.Entry<Long, TransferQueueEntry>> iter = _unconfirmed.entrySet().iterator();
            while(iter.hasNext())
            {
                Map.Entry<Long, TransferQueueEntry> pair = iter.next();
                if(pair.getKey() > deliveryTag)
                {
                    break;
                }
                else
                {
                    final TransferQueueEntry entry = pair.getValue();

                    if(requeue)
                    {
                        entry.release();
                    }
                    else
                    {
                        _txn.dequeue(entry.getEnqueueRecord(), new ServerTransaction.Action()
                        {
                            @Override
                            public void postCommit()
                            {
                                entry.delete();
                            }

                            @Override
                            public void onRollback()
                            {
                                entry.release();
                            }
                        });

                    }
                }
            }
        }
        else
        {
            final TransferQueueEntry entry = _unconfirmed.remove(deliveryTag);
            if(requeue)
            {
                entry.release();
            }
            else
            {
                _txn.dequeue(entry.getEnqueueRecord(), new ServerTransaction.Action()
                {
                    @Override
                    public void postCommit()
                    {
                        entry.delete();
                    }

                    @Override
                    public void onRollback()
                    {
                        entry.release();
                    }
                });
            }
        }
    }

    @Override
    public void receiveBasicAck(final long deliveryTag, final boolean multiple)
    {
        if(multiple)
        {
            Iterator<Map.Entry<Long, TransferQueueEntry>> iter = _unconfirmed.entrySet().iterator();
            while(iter.hasNext())
            {
                Map.Entry<Long, TransferQueueEntry> pair = iter.next();
                if(pair.getKey() > deliveryTag)
                {
                    break;
                }
                else
                {
                    final TransferQueueEntry entry = pair.getValue();
                    _txn.dequeue(entry.getEnqueueRecord(), new ServerTransaction.Action()
                    {
                        @Override
                        public void postCommit()
                        {
                            entry.delete();
                        }

                        @Override
                        public void onRollback()
                        {
                            entry.release();
                        }
                    });
                }
            }
        }
        else
        {
            final TransferQueueEntry entry = _unconfirmed.remove(deliveryTag);
            _txn.dequeue(entry.getEnqueueRecord(), new ServerTransaction.Action()
            {
                @Override
                public void postCommit()
                {
                    entry.delete();
                }

                @Override
                public void onRollback()
                {
                    entry.release();
                }
            });

        }
    }

    boolean isSuspended()
    {
        return _flowControlled || _unconfirmed.size() > _maxUnconfirmed;
    }

    @Override
    public AccessControlContext getAccessControllerContext()
    {
        return null;
    }

    @Override
    public boolean processPending()
    {
        if(_consumer != null)
        {
            return _consumer.processPending();
        }
        return false;
    }

    void notifyWork()
    {
        _connection.notifyWork();
    }

    @Override
    public void receivedComplete()
    {
        _txn.commitAsync(NO_OP);
    }

    void writeMethod(AMQMethodBody body)
    {
        writeFrame(body.generateFrame(_channelId));
    }

    void writeFrame(AMQFrame frame)
    {
        _connection.writeFrame(frame);
    }


    private class VirtualHostPropertiesHandler implements MessageHandler
    {
        private long _remaining;
        private BasicContentHeaderProperties _properties;

        @Override
        public void receiveMessageHeader(final BasicContentHeaderProperties properties, final long bodySize)
        {
            _remaining = bodySize;
            _properties = properties;
            receiveVirtualHostProperties(_properties);
        }

        @Override
        public void receiveMessageContent(final QpidByteBuffer data)
        {
            if((_remaining -= data.remaining()) == 0l)
            {
                _messageHandler = _unexpectedMessageHandler;
            }
        }
    }

    private void receiveVirtualHostProperties(final BasicContentHeaderProperties properties)
    {
        Collection<String> remoteHostGlobalDomains =
                properties.getHeaders().getFieldArray("virtualhost.globalDomains");
        setRemoteHostGlobalDomains(remoteHostGlobalDomains);
    }


    void transfer(final TransferQueueEntry entry)
    {
        ServerMessage message = entry.getMessage();
        _unconfirmed.put(++_lastSent, entry);

        writeMethod(_methodRegistry.createBasicPublishBody(0, "", message.getInitialRoutingAddress(), false, false));
        if(!(message instanceof AMQMessage))
        {
            final MessageConverter converter =
                    MessageConverterRegistry.getConverter(message.getClass(), AMQMessage.class);
            message = converter.convert(message, _connection.getVirtualHost());
        }

        AMQMessage amqMessage = (AMQMessage) message;
        ContentHeaderBody contentHeaderBody = amqMessage.getContentHeaderBody();
        writeHeader(contentHeaderBody);

        int bodySize = (int) contentHeaderBody.getBodySize();

        if (bodySize > 0)
        {
            int maxBodySize = (int) _connection.getMaxFrameSize() - AMQFrame.getFrameOverhead();

            int writtenSize = 0;

            while (writtenSize < bodySize)
            {
                int capacity = bodySize - writtenSize > maxBodySize ? maxBodySize : bodySize - writtenSize;
                AMQBody body = new MessageContentSourceBody(message, writtenSize, capacity);
                writtenSize += capacity;

                writeFrame(new AMQFrame(_channelId, body));
            }
        }

    }





    private void writeHeader(final ContentHeaderBody headerBody)
    {
        _connection.writeFrame(new AMQFrame(_channelId, headerBody));
    }
}
