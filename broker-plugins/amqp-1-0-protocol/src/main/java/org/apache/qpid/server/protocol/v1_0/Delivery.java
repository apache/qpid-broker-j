/*
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
 */

package org.apache.qpid.server.protocol.v1_0;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class Delivery
{
    private final UnsignedInteger _deliveryId;
    private final Binary _deliveryTag;
    private final List<Transfer> _transfers = new CopyOnWriteArrayList<>();
    private final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> _linkEndpoint;
    private final UnsignedInteger _messageFormat;
    private volatile boolean _complete;
    private volatile boolean _settled;
    private volatile boolean _aborted;
    private volatile DeliveryState _state;
    private volatile ReceiverSettleMode _receiverSettleMode;
    private volatile boolean _resume;
    private volatile long _totalPayloadSize;

    public Delivery(Transfer transfer, final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint)
    {
        _totalPayloadSize = 0L;
        _deliveryId = transfer.getDeliveryId();
        _deliveryTag = transfer.getDeliveryTag();
        _linkEndpoint = endpoint;
        _messageFormat = transfer.getMessageFormat() != null ? transfer.getMessageFormat() : UnsignedInteger.ZERO;
        addTransfer(transfer);
    }

    public UnsignedInteger getDeliveryId()
    {
        return _deliveryId;
    }

    public Binary getDeliveryTag()
    {
        return _deliveryTag;
    }

    public boolean isComplete()
    {
        return _complete;
    }

    public boolean isSettled()
    {
        return _settled;
    }

    public boolean isAborted()
    {
        return _aborted;
    }

    public DeliveryState getState()
    {
        return _state;
    }

    public ReceiverSettleMode getReceiverSettleMode()
    {
        return _receiverSettleMode;
    }

    public UnsignedInteger getMessageFormat()
    {
        return _messageFormat;
    }

    public boolean getResume()
    {
        return _resume;
    }

    public long getTotalPayloadSize()
    {
        return _totalPayloadSize;
    }

    final void addTransfer(Transfer transfer)
    {
        if (_aborted)
        {
            throw new IllegalStateException(String.format("Delivery '%s/%d' is already aborted",
                                                          _deliveryTag,
                                                          _deliveryId.intValue()));
        }

        if (_complete)
        {
            throw new IllegalStateException(String.format("Delivery '%s/%d' is already completed",
                                                          _deliveryTag,
                                                          _deliveryId.intValue()));
        }

        _transfers.add(transfer);
        if (Boolean.TRUE.equals(transfer.getAborted()))
        {
            _aborted = true;
            discard();
        }
        if(!Boolean.TRUE.equals(transfer.getMore()))
        {
            _complete = true;
        }
        if(Boolean.TRUE.equals(transfer.getSettled()))
        {
            _settled = true;
        }

        if(Boolean.TRUE.equals(transfer.getResume()))
        {
            _resume = true;
        }

        if (transfer.getState() != null)
        {
            DeliveryState currentState;
            if (_state instanceof TransactionalState)
            {
                currentState = ((TransactionalState) _state).getOutcome();
            }
            else
            {
                currentState = _state;
            }
            if (!(currentState instanceof Outcome))
            {
                _state = transfer.getState();
            }
        }

        if (transfer.getRcvSettleMode() != null)
        {
            if (_receiverSettleMode == null)
            {
                _receiverSettleMode = transfer.getRcvSettleMode();
            }
        }

        try (QpidByteBuffer payload = transfer.getPayload())
        {
            if (payload != null)
            {
                _totalPayloadSize += (long) payload.remaining();
            }
        }
    }

    public LinkEndpoint<? extends BaseSource, ? extends BaseTarget> getLinkEndpoint()
    {
        return _linkEndpoint;
    }

    public QpidByteBuffer getPayload()
    {
        List<QpidByteBuffer> transferBuffers = new ArrayList<>(_transfers.size());
        for (Transfer t : _transfers)
        {
            transferBuffers.add(t.getPayload());
            t.dispose();
        }
        _transfers.clear();
        final QpidByteBuffer combined = QpidByteBuffer.concatenate(transferBuffers);
        transferBuffers.forEach(QpidByteBuffer::dispose);
        return combined;
    }

    public void discard()
    {
        for (Transfer transfer: _transfers)
        {
            transfer.dispose();
        }
        _transfers.clear();
    }

}
