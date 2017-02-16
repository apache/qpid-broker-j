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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declare;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declared;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class TxnCoordinatorReceivingLinkEndpoint extends ReceivingLinkEndpoint
{
    private final SectionDecoderImpl _sectionDecoder;
    private final LinkedHashMap<Integer, ServerTransaction> _createdTransactions = new LinkedHashMap<>();
    private ArrayList<Transfer> _incompleteMessage;

    public TxnCoordinatorReceivingLinkEndpoint(final Session_1_0 session,
                                               final Attach attach)
    {
        super(session, attach);
        _sectionDecoder = new SectionDecoderImpl(getSession().getConnection().getDescribedTypeRegistry().getSectionDecoderRegistry());

    }

    @Override
    public void start()
    {
        setLinkCredit(UnsignedInteger.ONE);
        setCreditWindow();
    }

    @Override
    protected Error messageTransfer(Transfer xfr)
    {
        List<QpidByteBuffer> payload = new ArrayList<>();

        final Binary deliveryTag = xfr.getDeliveryTag();

        if(Boolean.TRUE.equals(xfr.getMore()) && _incompleteMessage == null)
        {
            _incompleteMessage = new ArrayList<Transfer>();
            _incompleteMessage.add(xfr);
            return null;
        }
        else if(_incompleteMessage != null)
        {
            _incompleteMessage.add(xfr);
            if(Boolean.TRUE.equals(xfr.getMore()))
            {
                return null;
            }

            for(Transfer t : _incompleteMessage)
            {
                final List<QpidByteBuffer> bufs = t.getPayload();
                if(bufs != null)
                {
                    payload.addAll(bufs);
                }
                t.dispose();
            }
            _incompleteMessage=null;

        }
        else
        {
            payload.addAll(xfr.getPayload());
            xfr.dispose();
        }

        // Only interested in the amqp-value section that holds the message to the coordinator
        try
        {
            List<EncodingRetainingSection<?>> sections = _sectionDecoder.parseAll(payload);
            boolean amqpValueSectionFound = false;
            for(EncodingRetainingSection section : sections)
            {
                if(section instanceof AmqpValueSection)
                {
                    if (amqpValueSectionFound)
                    {
                        throw new ConnectionScopedRuntimeException("Received more than one AmqpValue sections");
                    }
                    amqpValueSectionFound = true;
                    Object command = section.getValue();

                    if(command instanceof Declare)
                    {
                        final IdentifiedTransaction txn = getSession().getConnection().createLocalTransaction();
                        _createdTransactions.put(txn.getId(), txn.getServerTransaction());

                        Declared state = new Declared();

                        getSession().incrementStartedTransactions();

                        state.setTxnId(getSession().integerToBinary(txn.getId()));
                        updateDisposition(deliveryTag, state, true);

                    }
                    else if(command instanceof Discharge)
                    {
                        Discharge discharge = (Discharge) command;

                        final Error error = discharge(getSession().binaryToInteger(discharge.getTxnId()),
                                                      Boolean.TRUE.equals(discharge.getFail()));
                        updateDisposition(deliveryTag, error == null ? new Accepted() : null, true);
                        return error;
                    }
                    else
                    {
                        throw new ConnectionScopedRuntimeException(String.format("Received unknown command '%s'",
                                                                                 command.getClass().getSimpleName()));
                    }
                }
            }
            if (!amqpValueSectionFound)
            {
                throw new ConnectionScopedRuntimeException("Received no AmqpValue section");
            }
        }
        catch (AmqpErrorException e)
        {
            return e.getError();
        }
        finally
        {
            for(QpidByteBuffer buf : payload)
            {
                buf.dispose();
            }
        }
        return null;
    }

    private Error discharge(Integer transactionId, boolean fail)
    {
        Error error = null;
        ServerTransaction txn = _createdTransactions.get(transactionId);
        if(txn != null)
        {
            if(fail)
            {
                txn.rollback();
                getSession().incrementRolledBackTransactions();
            }
            else if(!(txn instanceof LocalTransaction && ((LocalTransaction)txn).isRollbackOnly()))
            {
                txn.commit();
                getSession().incrementCommittedTransactions();
            }
            else
            {
                txn.rollback();
                getSession().incrementRolledBackTransactions();
                error = new Error();
                error.setCondition(LinkError.DETACH_FORCED);
                error.setDescription("The transaction was marked as rollback only due to an earlier issue (e.g. a published message was sent settled but could not be enqueued)");
            }
            _createdTransactions.remove(transactionId);
            getSession().getConnection().removeTransaction(transactionId);
        }
        else
        {
            error = new Error();
            error.setCondition(AmqpError.NOT_FOUND);
            error.setDescription("Unknown transactionId" + transactionId);
        }
        return error;
    }

    @Override
    protected void remoteDetachedPerformDetach(Detach detach)
    {
        // force rollback of open transactions
        for(Map.Entry<Integer, ServerTransaction> entry : _createdTransactions.entrySet())
        {
            entry.getValue().rollback();
            getSession().incrementRolledBackTransactions();
            getSession().getConnection().removeTransaction(entry.getKey());
        }
        detach();
    }

    @Override
    protected void handle(final Binary deliveryTag, final DeliveryState state, final Boolean settled)
    {

    }


}
