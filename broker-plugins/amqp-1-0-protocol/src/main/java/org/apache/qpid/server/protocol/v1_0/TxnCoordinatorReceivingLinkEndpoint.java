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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declare;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declared;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionErrors;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class TxnCoordinatorReceivingLinkEndpoint extends AbstractReceivingLinkEndpoint<Coordinator>
{
    private final LinkedHashMap<Integer, ServerTransaction> _createdTransactions = new LinkedHashMap<>();
    private ArrayList<Transfer> _incompleteMessage;

    public TxnCoordinatorReceivingLinkEndpoint(final Session_1_0 session, final Link_1_0<Source, Coordinator> link)
    {
        super(session, link);
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
            List<EncodingRetainingSection<?>> sections = getSectionDecoder().parseAll(payload);
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

                    Session_1_0 session = getSession();
                    if(command instanceof Declare)
                    {
                        final IdentifiedTransaction txn = session.getConnection().createIdentifiedTransaction();
                        _createdTransactions.put(txn.getId(), txn.getServerTransaction());

                        Declared state = new Declared();

                        session.incrementStartedTransactions();

                        state.setTxnId(session.integerToBinary(txn.getId()));
                        updateDisposition(deliveryTag, state, true);

                    }
                    else if(command instanceof Discharge)
                    {
                        Discharge discharge = (Discharge) command;

                        Error error = discharge(discharge.getTxnId(), Boolean.TRUE.equals(discharge.getFail()));
                        final DeliveryState outcome;
                        if (error == null)
                        {
                            outcome = new Accepted();
                        }
                        else if (Arrays.asList(getSource().getOutcomes()).contains(Rejected.REJECTED_SYMBOL))
                        {
                            final Rejected rejected = new Rejected();
                            rejected.setError(error);
                            outcome = rejected;
                            error = null;
                        }
                        else
                        {
                            outcome = null;
                        }

                        if (error == null)
                        {
                            updateDisposition(deliveryTag, outcome, true);
                        }
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

    private Error discharge(Binary transactionIdAsBinary, boolean fail)
    {
        Error error = null;
        Integer transactionId = null;
        ServerTransaction txn = null;
        try
        {
            transactionId = getSession().binaryToInteger(transactionIdAsBinary);
            txn = _createdTransactions.get(transactionId);
        }
        catch (IllegalArgumentException e)
        {
           // pass
        }

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
                error.setCondition(TransactionErrors.TRANSACTION_ROLLBACK);
                error.setDescription("The transaction was marked as rollback only due to an earlier issue (e.g. a published message was sent settled but could not be enqueued)");
            }
            _createdTransactions.remove(transactionId);
            getSession().getConnection().removeTransaction(transactionId);
        }
        else
        {
            error = new Error();
            error.setCondition(TransactionErrors.UNKNOWN_ID);
            error.setDescription("Unknown transactionId " + transactionIdAsBinary.toString());
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
    protected void reattachLink(final Attach attach) throws AmqpErrorException
    {
        throw new AmqpErrorException(new Error(AmqpError.NOT_IMPLEMENTED, "Cannot reattach a Coordinator Link."));
    }

    @Override
    protected void resumeLink(final Attach attach) throws AmqpErrorException
    {
        throw new AmqpErrorException(new Error(AmqpError.NOT_IMPLEMENTED, "Cannot resume a Coordinator Link."));
    }

    @Override
    protected void establishLink(final Attach attach) throws AmqpErrorException
    {
        if (getSource() != null || getTarget() != null)
        {
            throw new IllegalStateException("LinkEndpoint and Termini should be null when establishing a Link.");
        }

        Coordinator target = new Coordinator();
        Source source = (Source) attach.getSource();
        getLink().setTermini(source, target);

        attachReceived(attach);
    }

    @Override
    protected void recoverLink(final Attach attach) throws AmqpErrorException
    {
        throw new AmqpErrorException(new Error(AmqpError.NOT_IMPLEMENTED, "Cannot recover a Coordinator Link."));
    }

    @Override
    protected void handle(final Binary deliveryTag, final DeliveryState state, final Boolean settled)
    {

    }

    @Override
    public void attachReceived(final Attach attach) throws AmqpErrorException
    {
        super.attachReceived(attach);
        setDeliveryCount(new SequenceNumber(attach.getInitialDeliveryCount().intValue()));
    }

    @Override
    public void initialiseUnsettled()
    {
    }
}
