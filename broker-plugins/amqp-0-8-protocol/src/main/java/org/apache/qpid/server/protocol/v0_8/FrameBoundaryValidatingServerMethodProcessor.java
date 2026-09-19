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

import java.util.Objects;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

final class FrameBoundaryValidatingServerMethodProcessor
        implements ServerMethodProcessor<ServerChannelMethodProcessor>, ServerChannelMethodProcessor
{
    private static final int NO_CHANNEL = -1;

    private final ServerMethodProcessor<? extends ServerChannelMethodProcessor> _delegate;

    private QpidByteBuffer _frameBody;
    private int _channelId = NO_CHANNEL;
    private ServerChannelMethodProcessor _channelDelegate;
    private int _classId;
    private int _methodId;
    private boolean _currentMethodRecorded;
    private boolean _currentMethodPublished;
    private boolean _channelMethodValidationPerformed;
    private boolean _channelMethodRejected;

    FrameBoundaryValidatingServerMethodProcessor(final ServerMethodProcessor<? extends ServerChannelMethodProcessor> delegate)
    {
        _delegate = Objects.requireNonNull(delegate);
    }

    void begin(final QpidByteBuffer frameBody)
    {
        if (_frameBody != null)
        {
            throw new IllegalStateException("Method frame validation is already active");
        }
        _frameBody = Objects.requireNonNull(frameBody);
        _channelId = NO_CHANNEL;
        _channelDelegate = null;
        _classId = 0;
        _methodId = 0;
        _currentMethodRecorded = false;
        _currentMethodPublished = false;
        _channelMethodValidationPerformed = false;
        _channelMethodRejected = false;
    }

    void end()
    {
        _frameBody = null;
        _channelId = NO_CHANNEL;
        _channelDelegate = null;
        _classId = 0;
        _methodId = 0;
        _currentMethodRecorded = false;
        _currentMethodPublished = false;
        _channelMethodValidationPerformed = false;
        _channelMethodRejected = false;
    }

    void publishCurrentMethodIfFrameBodyConsumed()
    {
        ensureActive();
        if (!_frameBody.hasRemaining())
        {
            publishCurrentMethod();
        }
    }

    boolean isMethodBodyDecoded()
    {
        return _currentMethodPublished;
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        try
        {
            return _delegate.getProtocolVersion();
        }
        catch (final ConnectionScopedRuntimeException | ServerScopedRuntimeException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(final int channelId)
    {
        ensureActive();
        if (_channelId == NO_CHANNEL)
        {
            _channelId = channelId;
        }
        else if (_channelId != channelId)
        {
            throw new IllegalStateException("Multiple channels referenced by one method frame");
        }
        return this;
    }

    @Override
    public void receiveConnectionStartOk(final FieldTable clientProperties,
                                         final AMQShortString mechanism,
                                         final byte[] response,
                                         final AMQShortString locale)
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveConnectionStartOk(clientProperties, mechanism, response, locale);
        }
    }

    @Override
    public void receiveConnectionSecureOk(final byte[] response)
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveConnectionSecureOk(response);
        }
    }

    @Override
    public void receiveConnectionTuneOk(final int channelMax, final long frameMax, final int heartbeat)
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveConnectionTuneOk(channelMax, frameMax, heartbeat);
        }
    }

    @Override
    public void receiveConnectionOpen(final AMQShortString virtualHost,
                                      final AMQShortString capabilities,
                                      final boolean insist)
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveConnectionOpen(virtualHost, capabilities, insist);
        }
    }

    @Override
    public void receiveChannelOpen(final int channelId)
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveChannelOpen(channelId);
        }
    }

    @Override
    public void receiveConnectionClose(final int replyCode,
                                       final AMQShortString replyText,
                                       final int classId,
                                       final int methodId)
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveConnectionClose(replyCode, replyText, classId, methodId);
        }
    }

    @Override
    public void receiveConnectionCloseOk()
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveConnectionCloseOk();
        }
    }

    @Override
    public void receiveHeartbeat()
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveHeartbeat();
        }
    }

    @Override
    public void receiveProtocolHeader(final ProtocolInitiation protocolInitiation)
    {
        if (isFrameBodyConsumed())
        {
            _delegate.receiveProtocolHeader(protocolInitiation);
        }
    }

    @Override
    public void setCurrentMethod(final int classId, final int methodId)
    {
        ensureActive();
        if (_currentMethodRecorded)
        {
            throw new IllegalStateException("Current method is already recorded for this frame");
        }
        _classId = classId;
        _methodId = methodId;
        _currentMethodRecorded = true;
    }

    @Override
    public boolean ignoreAllButCloseOk()
    {
        if (_channelId == NO_CHANNEL)
        {
            return !isFrameBodyConsumed() || _delegate.ignoreAllButCloseOk();
        }
        return !isChannelMethodDispatchAllowed() || getChannelDelegate().ignoreAllButCloseOk();
    }

    @Override
    public void receiveAccessRequest(final AMQShortString realm,
                                     final boolean exclusive,
                                     final boolean passive,
                                     final boolean active,
                                     final boolean write,
                                     final boolean read)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveAccessRequest(realm, exclusive, passive, active, write, read);
        }
    }

    @Override
    public void receiveExchangeDeclare(final AMQShortString exchange,
                                       final AMQShortString type,
                                       final boolean passive,
                                       final boolean durable,
                                       final boolean autoDelete,
                                       final boolean internal,
                                       final boolean nowait,
                                       final FieldTable arguments)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveExchangeDeclare(exchange, type, passive, durable, autoDelete, internal,
                    nowait, arguments);
        }
    }

    @Override
    public void receiveExchangeDelete(final AMQShortString exchange, final boolean ifUnused, final boolean nowait)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveExchangeDelete(exchange, ifUnused, nowait);
        }
    }

    @Override
    public void receiveExchangeBound(final AMQShortString exchange,
                                     final AMQShortString routingKey,
                                     final AMQShortString queue)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveExchangeBound(exchange, routingKey, queue);
        }
    }

    @Override
    public void receiveQueueDeclare(final AMQShortString queue,
                                    final boolean passive,
                                    final boolean durable,
                                    final boolean exclusive,
                                    final boolean autoDelete,
                                    final boolean nowait,
                                    final FieldTable arguments)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveQueueDeclare(queue, passive, durable, exclusive, autoDelete, nowait, arguments);
        }
    }

    @Override
    public void receiveQueueBind(final AMQShortString queue,
                                 final AMQShortString exchange,
                                 final AMQShortString bindingKey,
                                 final boolean nowait,
                                 final FieldTable arguments)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveQueueBind(queue, exchange, bindingKey, nowait, arguments);
        }
    }

    @Override
    public void receiveQueuePurge(final AMQShortString queue, final boolean nowait)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveQueuePurge(queue, nowait);
        }
    }

    @Override
    public void receiveQueueDelete(final AMQShortString queue,
                                   final boolean ifUnused,
                                   final boolean ifEmpty,
                                   final boolean nowait)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveQueueDelete(queue, ifUnused, ifEmpty, nowait);
        }
    }

    @Override
    public void receiveQueueUnbind(final AMQShortString queue,
                                   final AMQShortString exchange,
                                   final AMQShortString bindingKey,
                                   final FieldTable arguments)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveQueueUnbind(queue, exchange, bindingKey, arguments);
        }
    }

    @Override
    public void receiveBasicRecover(final boolean requeue, final boolean sync)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicRecover(requeue, sync);
        }
    }

    @Override
    public void receiveBasicQos(final long prefetchSize, final int prefetchCount, final boolean global)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicQos(prefetchSize, prefetchCount, global);
        }
    }

    @Override
    public void receiveBasicConsume(final AMQShortString queue,
                                    final AMQShortString consumerTag,
                                    final boolean noLocal,
                                    final boolean noAck,
                                    final boolean exclusive,
                                    final boolean nowait,
                                    final FieldTable arguments)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicConsume(queue, consumerTag, noLocal, noAck, exclusive, nowait, arguments);
        }
    }

    @Override
    public void receiveBasicCancel(final AMQShortString consumerTag, final boolean noWait)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicCancel(consumerTag, noWait);
        }
    }

    @Override
    public void receiveBasicPublish(final AMQShortString exchange,
                                    final AMQShortString routingKey,
                                    final boolean mandatory,
                                    final boolean immediate)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicPublish(exchange, routingKey, mandatory, immediate);
        }
    }

    @Override
    public void receiveBasicGet(final AMQShortString queue, final boolean noAck)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicGet(queue, noAck);
        }
    }

    @Override
    public void receiveBasicAck(final long deliveryTag, final boolean multiple)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicAck(deliveryTag, multiple);
        }
    }

    @Override
    public void receiveBasicReject(final long deliveryTag, final boolean requeue)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicReject(deliveryTag, requeue);
        }
    }

    @Override
    public void receiveTxSelect()
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveTxSelect();
        }
    }

    @Override
    public void receiveTxCommit()
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveTxCommit();
        }
    }

    @Override
    public void receiveTxRollback()
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveTxRollback();
        }
    }

    @Override
    public void receiveConfirmSelect(final boolean nowait)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveConfirmSelect(nowait);
        }
    }

    @Override
    public void receiveChannelFlow(final boolean active)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveChannelFlow(active);
        }
    }

    @Override
    public void receiveChannelFlowOk(final boolean active)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveChannelFlowOk(active);
        }
    }

    @Override
    public void receiveChannelClose(final int replyCode,
                                    final AMQShortString replyText,
                                    final int classId,
                                    final int methodId)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveChannelClose(replyCode, replyText, classId, methodId);
        }
    }

    @Override
    public void receiveChannelCloseOk()
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveChannelCloseOk();
        }
    }

    @Override
    public void receiveMessageContent(final QpidByteBuffer data)
    {
        if (isFrameBodyConsumed())
        {
            getChannelDelegate().receiveMessageContent(data);
        }
    }

    @Override
    public void receiveMessageHeader(final BasicContentHeaderProperties properties, final long bodySize)
    {
        boolean propertiesTransferred = false;
        try
        {
            if (isFrameBodyConsumed())
            {
                final ServerChannelMethodProcessor channelDelegate = getChannelDelegate();
                propertiesTransferred = true;
                channelDelegate.receiveMessageHeader(properties, bodySize);
            }
        }
        finally
        {
            if (!propertiesTransferred)
            {
                properties.dispose();
            }
        }
    }

    @Override
    public void receiveBasicNack(final long deliveryTag, final boolean multiple, final boolean requeue)
    {
        if (isChannelMethodDispatchAllowed())
        {
            getChannelDelegate().receiveBasicNack(deliveryTag, multiple, requeue);
        }
    }

    private boolean isFrameBodyConsumed()
    {
        ensureActive();
        if (_frameBody.hasRemaining())
        {
            return false;
        }
        publishCurrentMethod();
        return true;
    }

    private boolean isChannelMethodDispatchAllowed()
    {
        if (!isFrameBodyConsumed())
        {
            return false;
        }
        if (!_channelMethodValidationPerformed)
        {
            _channelMethodRejected = getChannelDelegate().rejectMethodFrameIfContentIncomplete();
            _channelMethodValidationPerformed = true;
        }
        return !_channelMethodRejected;
    }

    private void publishCurrentMethod()
    {
        if (!_currentMethodRecorded)
        {
            throw new IllegalStateException("No current method was recorded for this frame");
        }
        if (!_currentMethodPublished)
        {
            _currentMethodPublished = true;
            _delegate.setCurrentMethod(_classId, _methodId);
        }
    }

    private void ensureActive()
    {
        if (_frameBody == null)
        {
            throw new IllegalStateException("Method frame validation is not active");
        }
    }

    private ServerChannelMethodProcessor getChannelDelegate()
    {
        if (_channelId == NO_CHANNEL)
        {
            throw new IllegalStateException("No channel was selected for the method frame");
        }
        if (_channelDelegate == null)
        {
            _channelDelegate = _delegate.getChannelMethodProcessor(_channelId);
        }
        return _channelDelegate;
    }
}
