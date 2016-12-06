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
package org.apache.qpid.server.protocol.v1_0;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AbstractSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declare;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declared;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;

public class TxnCoordinatorReceivingLink_1_0 implements ReceivingLink_1_0
{
    private static final Logger _logger = LoggerFactory.getLogger(TxnCoordinatorReceivingLink_1_0.class);
    private NamedAddressSpace _namedAddressSpace;
    private ReceivingLinkEndpoint _endpoint;

    private ArrayList<Transfer> _incompleteMessage;
    private SectionDecoder _sectionDecoder;
    private LinkedHashMap<Integer, ServerTransaction> _openTransactions;
    private Session_1_0 _session;


    public TxnCoordinatorReceivingLink_1_0(NamedAddressSpace namedAddressSpace,
                                           Session_1_0 session_1_0, ReceivingLinkEndpoint endpoint,
                                           LinkedHashMap<Integer, ServerTransaction> openTransactions)
    {
        _namedAddressSpace = namedAddressSpace;
        _session = session_1_0;
        _endpoint  = endpoint;
        _sectionDecoder = new SectionDecoderImpl(endpoint.getSession().getConnection().getDescribedTypeRegistry().getSectionDecoderRegistry());
        _openTransactions = openTransactions;
    }

    public void messageTransfer(Transfer xfr)
    {
        List<QpidByteBuffer> payload = new ArrayList<>();

        final Binary deliveryTag = xfr.getDeliveryTag();

        if(Boolean.TRUE.equals(xfr.getMore()) && _incompleteMessage == null)
        {
            _incompleteMessage = new ArrayList<Transfer>();
            _incompleteMessage.add(xfr);
            return;
        }
        else if(_incompleteMessage != null)
        {
            _incompleteMessage.add(xfr);
            if(Boolean.TRUE.equals(xfr.getMore()))
            {
                return;
            }

            int size = 0;
            for(Transfer t : _incompleteMessage)
            {
                final List<QpidByteBuffer> bufs = t.getPayload();
                if(bufs != null)
                {
                    size += QpidByteBufferUtils.remaining(bufs);
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
            List<AbstractSection<?>> sections = _sectionDecoder.parseAll(payload);
            for(AbstractSection section : sections)
            {
                if(section instanceof AmqpValueSection)
                {
                    Object command = section.getValue();


                    if(command instanceof Declare)
                    {
                        Integer txnId = Integer.valueOf(0);
                        Iterator<Integer> existingTxn  = _openTransactions.keySet().iterator();
                        while(existingTxn.hasNext())
                        {
                            txnId = existingTxn.next();
                        }
                        txnId = Integer.valueOf(txnId.intValue() + 1);

                        _openTransactions.put(txnId, new LocalTransaction(_namedAddressSpace.getMessageStore()));

                        Declared state = new Declared();

                        _session.incrementStartedTransactions();

                        state.setTxnId(_session.integerToBinary(txnId));
                        _endpoint.updateDisposition(deliveryTag, state, true);

                    }
                    else if(command instanceof Discharge)
                    {
                        Discharge discharge = (Discharge) command;

                        discharge(_session.binaryToInteger(discharge.getTxnId()), discharge.getFail());
                        _endpoint.updateDisposition(deliveryTag, new Accepted(), true);

                    }
                    else
                    {
                        // TODO error handling

                        // also should panic if we receive more than one AmqpValue, or no AmqpValue section
                    }
                }
            }

        }
        catch (AmqpErrorException e)
        {
            //TODO
            _logger.error("AMQP error", e);
        }
        finally
        {
            for(QpidByteBuffer buf : payload)
            {
                buf.dispose();
            }
        }

    }

    public void remoteDetached(LinkEndpoint endpoint, Detach detach)
    {
        endpoint.detach();
    }

    @Override
    public void handle(final Binary deliveryTag, final DeliveryState state, final Boolean settled)
    {

    }

    private Error discharge(Integer transactionId, boolean fail)
    {
        Error error = null;
        ServerTransaction txn = _openTransactions.get(transactionId);
        if(txn != null)
        {
            if(fail)
            {
                txn.rollback();
                _session.incrementRolledBackTransactions();
            }
            else
            {
                txn.commit();
                _session.incrementCommittedTransactions();
            }
            _openTransactions.remove(transactionId);
        }
        else
        {
            error = new Error();
            error.setCondition(AmqpError.NOT_FOUND);
            error.setDescription("Unknown transactionId" + transactionId);
        }
        return error;
    }



    public void start()
    {
        _endpoint.setLinkCredit(UnsignedInteger.ONE);
        _endpoint.setCreditWindow();
    }
}
