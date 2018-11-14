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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.Session;
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
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class TxnCoordinatorReceivingLinkEndpoint extends AbstractReceivingLinkEndpoint<Coordinator>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TxnCoordinatorReceivingLinkEndpoint.class);
    private final Map<Integer, ServerTransaction> _createdTransactions = new ConcurrentHashMap<>();

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
    protected Error receiveDelivery(Delivery delivery)
    {
        // Only interested in the amqp-value section that holds the message to the coordinator
        try (QpidByteBuffer payload = delivery.getPayload())
        {
            List<EncodingRetainingSection<?>> sections = getSectionDecoder().parseAll(payload);
            boolean amqpValueSectionFound = false;
            for(EncodingRetainingSection section : sections)
            {
                try
                {
                    if (section instanceof AmqpValueSection)
                    {
                        if (amqpValueSectionFound)
                        {
                            throw new ConnectionScopedRuntimeException("Received more than one AmqpValue sections");
                        }
                        amqpValueSectionFound = true;
                        Object command = section.getValue();

                        Session_1_0 session = getSession();

                        AMQPConnection_1_0<?> connection = session.getConnection();
                        connection.receivedComplete();

                        if (command instanceof Declare)
                        {
                            final IdentifiedTransaction txn = connection.createIdentifiedTransaction();
                            _createdTransactions.put(txn.getId(), txn.getServerTransaction());
                            long notificationRepeatPeriod =
                                    getSession().getContextValue(Long.class, Session.TRANSACTION_TIMEOUT_NOTIFICATION_REPEAT_PERIOD);
                            connection.registerTransactionTickers(txn.getServerTransaction(),
                                                                  this::doTimeoutAction,
                                                                  notificationRepeatPeriod);

                            Declared state = new Declared();

                            state.setTxnId(Session_1_0.integerToTransactionId(txn.getId()));
                            updateDisposition(delivery.getDeliveryTag(), state, true);
                        }
                        else if (command instanceof Discharge)
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
                                updateDisposition(delivery.getDeliveryTag(), outcome, true);
                            }
                            return error;
                        }
                        else
                        {
                            throw new ConnectionScopedRuntimeException(String.format("Received unknown command '%s'",
                                                                                     command.getClass()
                                                                                            .getSimpleName()));
                        }
                    }
                }
                finally
                {
                    section.dispose();
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
        return null;
    }

    private Error discharge(Binary transactionIdAsBinary, boolean fail)
    {
        Error error = null;
        Integer transactionId = null;
        ServerTransaction txn = null;
        try
        {
            transactionId = Session_1_0.transactionIdToInteger(transactionIdAsBinary);
            txn = _createdTransactions.get(transactionId);
        }
        catch (UnknownTransactionException | IllegalArgumentException e)
        {
           // handle error below
        }

        if(txn != null)
        {
            AMQPConnection_1_0<?> connection = getSession().getConnection();
            if(fail)
            {
                txn.rollback();
                connection.incrementTransactionRollbackCounter();
            }
            else if(!(txn instanceof LocalTransaction && ((LocalTransaction)txn).isRollbackOnly()))
            {
                try
                {
                    txn.commit();
                }
                catch (ServerScopedRuntimeException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Transaction {} commit failed", transactionId, e);
                    }
                    else
                    {
                        LOGGER.info("Transaction {} commit failed: {}", transactionId, e.getMessage());
                    }
                    error = forceRollback(txn, connection);
                }
            }
            else
            {
                error = forceRollback(txn, connection);
            }
            _createdTransactions.remove(transactionId);
            connection.unregisterTransactionTickers(txn);
            connection.removeTransaction(transactionId);
            connection.decrementTransactionOpenCounter();
        }
        else
        {
            error = new Error();
            error.setCondition(TransactionError.UNKNOWN_ID);
            error.setDescription("Unknown transactionId " + transactionIdAsBinary.toString());
        }
        return error;
    }

    private Error forceRollback(final ServerTransaction txn, final AMQPConnection_1_0<?> connection)
    {
        txn.rollback();
        connection.incrementTransactionRollbackCounter();
        final Error error = new Error();
        error.setCondition(TransactionError.TRANSACTION_ROLLBACK);
        error.setDescription("The transaction was rolled back due to an earlier issue (e.g. a published message was sent settled but could not be enqueued)");
        return error;
    }

    @Override
    protected void remoteDetachedPerformDetach(Detach detach)
    {
        // force rollback of open transactions
        rollbackOpenTransactions();
        close();
    }

    @Override
    protected Map<Binary, DeliveryState> getLocalUnsettled()
    {
        return Collections.emptyMap();
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
    public void attachReceived(final Attach attach) throws AmqpErrorException
    {
        super.attachReceived(attach);
        setDeliveryCount(new SequenceNumber(attach.getInitialDeliveryCount().intValue()));
    }

    @Override
    public void receiveComplete()
    {

    }

    private void doTimeoutAction(final String message)
    {
        rollbackOpenTransactions();
        Error error = new Error(TransactionError.TRANSACTION_TIMEOUT, message);
        getSession().getConnection().close(error);
    }

    private void rollbackOpenTransactions()
    {
        for(Map.Entry<Integer, ServerTransaction> entry : _createdTransactions.entrySet())
        {
            entry.getValue().rollback();
            AMQPConnection_1_0<?> connection = getSession().getConnection();
            connection.decrementTransactionOpenCounter();
            connection.incrementTransactionRollbackCounter();
            connection.removeTransaction(entry.getKey());
        }
        _createdTransactions.clear();
    }
}
