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

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.protocol.MessageFormatRegistry;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequenceSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationPropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.FooterSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class StandardReceivingLink_1_0 implements ReceivingLink_1_0
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardReceivingLink_1_0.class);
    private NamedAddressSpace _addressSpace;

    private ReceivingDestination _destination;
    private SectionDecoderImpl _sectionDecoder;
    private volatile ReceivingLinkAttachment _attachment;


    private ArrayList<Transfer> _incompleteMessage;
    private TerminusDurability _durability;

    private Map<Binary, Outcome> _unsettledMap = Collections.synchronizedMap(new HashMap<Binary, Outcome>());
    private boolean _resumedMessage;
    private Binary _messageDeliveryTag;
    private ReceiverSettleMode _receivingSettlementMode;


    public StandardReceivingLink_1_0(ReceivingLinkAttachment receivingLinkAttachment, NamedAddressSpace addressSpace,
                                     ReceivingDestination destination)
    {
        _addressSpace = addressSpace;
        _destination = destination;
        _attachment = receivingLinkAttachment;
        _receivingSettlementMode = receivingLinkAttachment.getEndpoint().getReceivingSettlementMode();
        _durability = ((Target)receivingLinkAttachment.getTarget()).getDurable();

        _sectionDecoder = new SectionDecoderImpl(receivingLinkAttachment.getEndpoint().getSession().getConnection().getSectionDecoderRegistry());


    }

    public Error messageTransfer(Transfer xfr)
    {
        List<QpidByteBuffer> fragments = null;

        org.apache.qpid.server.protocol.v1_0.type.DeliveryState xfrState = xfr.getState();
        final Binary deliveryTag = xfr.getDeliveryTag();
        UnsignedInteger messageFormat = null;
        if(Boolean.TRUE.equals(xfr.getMore()) && _incompleteMessage == null)
        {
            _incompleteMessage = new ArrayList<>();
            _incompleteMessage.add(xfr);
            _resumedMessage = Boolean.TRUE.equals(xfr.getResume());
            _messageDeliveryTag = deliveryTag;
            return null;
        }
        else if(_incompleteMessage != null)
        {
            _incompleteMessage.add(xfr);

            if(Boolean.TRUE.equals(xfr.getMore()))
            {
                return null;
            }

            fragments = new ArrayList<>(_incompleteMessage.size());

            for(Transfer t : _incompleteMessage)
            {
                if(t.getMessageFormat() != null && messageFormat == null)
                {
                    messageFormat = t.getMessageFormat();
                }
                fragments.addAll(t.getPayload());
                t.dispose();
            }
            _incompleteMessage=null;

        }
        else
        {
            _resumedMessage = Boolean.TRUE.equals(xfr.getResume());
            _messageDeliveryTag = deliveryTag;
            fragments = xfr.getPayload();
            messageFormat = xfr.getMessageFormat();

            xfr.dispose();
        }

        if(_resumedMessage)
        {
            if(_unsettledMap.containsKey(_messageDeliveryTag))
            {
                Outcome outcome = _unsettledMap.get(_messageDeliveryTag);
                boolean settled = ReceiverSettleMode.FIRST.equals(getReceivingSettlementMode());
                getEndpoint().updateDisposition(_messageDeliveryTag, (DeliveryState) outcome, settled);
                if(settled)
                {
                    _unsettledMap.remove(_messageDeliveryTag);
                }
            }
            else
            {
                throw new ConnectionScopedRuntimeException("Unexpected delivery Tag: " + _messageDeliveryTag + "_unsettledMap: " + _unsettledMap);
            }
        }
        else
        {
            ServerMessage<?> serverMessage;
            String routingAddress;

            if(messageFormat == null || UnsignedInteger.ZERO.equals(messageFormat))
            {
                List<EncodingRetainingSection<?>> dataSections = new ArrayList<>();

                MessageMetaData_1_0 mmd = createMessageMetaData(fragments, dataSections);
                MessageHandle<MessageMetaData_1_0> handle = _addressSpace.getMessageStore().addMessage(mmd);

                for (EncodingRetainingSection<?> dataSection : dataSections)
                {
                    for (QpidByteBuffer buf : dataSection.getEncodedForm())
                    {
                        handle.addContent(buf);
                        buf.dispose();
                    }
                }
                final StoredMessage<MessageMetaData_1_0> storedMessage = handle.allContentAdded();
                Message_1_0 message = new Message_1_0(storedMessage, getSession().getConnection().getReference());
                routingAddress = _destination.getRoutingAddress(message);
                serverMessage = message;
            }
            else
            {
                MessageFormat format = MessageFormatRegistry.getFormat(messageFormat.intValue());
                if(format != null)
                {
                    serverMessage = format.createMessage(fragments, _addressSpace.getMessageStore(), getSession().getConnection().getReference());
                    routingAddress = format.getRoutingAddress(serverMessage, _destination.getAddress());
                }
                else
                {
                    final Error err = new Error();
                    err.setCondition(AmqpError.NOT_IMPLEMENTED);
                    err.setDescription("Unknown message format: " + messageFormat);
                    return err;
                }
            }

            for(QpidByteBuffer fragment: fragments)
            {
                fragment.dispose();
            }
            fragments = null;

            MessageReference<?> reference = serverMessage.newReference();
            try
            {
                Binary transactionId = null;
                if (xfrState != null)
                {
                    if (xfrState instanceof TransactionalState)
                    {
                        transactionId = ((TransactionalState) xfrState).getTxnId();
                    }
                }

                ServerTransaction transaction = null;
                if (transactionId != null)
                {
                    transaction = getSession().getTransaction(transactionId);
                }
                else
                {
                    Session_1_0 session = getSession();
                    transaction = session != null
                            ? session.getTransaction(null)
                            : new AutoCommitTransaction(_addressSpace.getMessageStore());
                }

                try
                {
                    Session_1_0 session = getSession();

                    session.getAMQPConnection()
                            .checkAuthorizedMessagePrincipal(serverMessage.getMessageHeader().getUserId());
                    _destination.authorizePublish(session.getSecurityToken(), routingAddress);

                    Outcome outcome = _destination.send(serverMessage, routingAddress, transaction, session.getCapacityCheckAction());
                    Source source = (Source) getEndpoint().getSource();

                    DeliveryState resultantState;

                    if(source.getOutcomes() == null || Arrays.asList(source.getOutcomes()).contains(outcome.getSymbol()))
                    {
                        if (transactionId == null)
                        {
                            resultantState = (DeliveryState) outcome;
                        }
                        else
                        {
                            TransactionalState transactionalState = new TransactionalState();
                            transactionalState.setOutcome(outcome);
                            transactionalState.setTxnId(transactionId);
                            resultantState = transactionalState;
                        }
                    }
                    else if(transactionId != null)
                    {
                        // cause the txn to fail
                        if(transaction instanceof LocalTransaction)
                        {
                            ((LocalTransaction) transaction).setRollbackOnly();
                        }
                        resultantState = null;
                    }
                    else
                    {
                        // we should just use the default outcome
                        resultantState = null;
                    }


                    boolean settled = ReceiverSettleMode.FIRST.equals(getReceivingSettlementMode()                                                             );

                    if (!settled)
                    {
                        _unsettledMap.put(deliveryTag, outcome);
                    }

                    getEndpoint().updateDisposition(deliveryTag, resultantState, settled);

                    getSession().getAMQPConnection()
                            .registerMessageReceived(serverMessage.getSize(), serverMessage.getArrivalTime());

                    if (!(transaction instanceof AutoCommitTransaction))
                    {
                        ServerTransaction.Action a;
                        transaction.addPostTransactionAction(new ServerTransaction.Action()
                        {
                            public void postCommit()
                            {
                                getEndpoint().updateDisposition(deliveryTag, null, true);
                            }

                            public void onRollback()
                            {
                                getEndpoint().updateDisposition(deliveryTag, null, true);
                            }
                        });
                    }
                }
                catch (AccessControlException e)
                {
                    final Error err = new Error();
                    err.setCondition(AmqpError.NOT_ALLOWED);
                    err.setDescription(e.getMessage());
                    _attachment.getEndpoint().close(err);

                }
            }
            finally
            {
                reference.release();
            }
        }
        return null;
    }

    private MessageMetaData_1_0 createMessageMetaData(final List<QpidByteBuffer> fragments,
                                                      final List<EncodingRetainingSection<?>> dataSections)
    {

        List<EncodingRetainingSection<?>> sections;
        try
        {
             sections = _sectionDecoder.parseAll(fragments);
        }
        catch (AmqpErrorException e)
        {
            LOGGER.error("Decoding read section error", e);
            // TODO - fix error handling
            throw new IllegalArgumentException(e);
        }

        long contentSize = 0L;

        HeaderSection headerSection = null;
        PropertiesSection propertiesSection = null;
        DeliveryAnnotationsSection deliveryAnnotationsSection = null;
        MessageAnnotationsSection messageAnnotationsSection = null;
        ApplicationPropertiesSection applicationPropertiesSection = null;
        FooterSection footerSection = null;
        Iterator<EncodingRetainingSection<?>> iter = sections.iterator();
        EncodingRetainingSection<?> s = iter.hasNext() ? iter.next() : null;
        if (s instanceof HeaderSection)
        {
            headerSection = (HeaderSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof DeliveryAnnotationsSection)
        {
            deliveryAnnotationsSection = (DeliveryAnnotationsSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof MessageAnnotationsSection)
        {
            messageAnnotationsSection = (MessageAnnotationsSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof PropertiesSection)
        {
            propertiesSection = (PropertiesSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof ApplicationPropertiesSection)
        {
            applicationPropertiesSection = (ApplicationPropertiesSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }

        if (s instanceof AmqpValueSection)
        {
            contentSize = s.getEncodedSize();
            dataSections.add(s);
            s = iter.hasNext() ? iter.next() : null;
        }
        else if (s instanceof DataSection)
        {
            do
            {
                contentSize += s.getEncodedSize();
                dataSections.add(s);
                s = iter.hasNext() ? iter.next() : null;
            } while (s instanceof DataSection);
        }
        else if (s instanceof AmqpSequenceSection)
        {
            do
            {
                contentSize += s.getEncodedSize();
                dataSections.add(s);
                s = iter.hasNext() ? iter.next() : null;
            }
            while (s instanceof AmqpSequenceSection);
        }

        if (s instanceof FooterSection)
        {
            footerSection = (FooterSection) s;
            s = iter.hasNext() ? iter.next() : null;
        }
        if (s != null)
        {
            // TODO error
        }
        return new MessageMetaData_1_0(headerSection,
                                       deliveryAnnotationsSection,
                                       messageAnnotationsSection,
                                       propertiesSection,
                                       applicationPropertiesSection,
                                       footerSection,
                                       System.currentTimeMillis(),
                                       contentSize);
    }

    private ReceiverSettleMode getReceivingSettlementMode()
    {
        return _receivingSettlementMode;
    }

    public void remoteDetached(LinkEndpoint endpoint, Detach detach)
    {
        //TODO
        // if not durable or close
        if(!TerminusDurability.UNSETTLED_STATE.equals(_durability) ||
           (detach != null && Boolean.TRUE.equals(detach.getClosed())))
        {
            endpoint.close();
        }
        else if(detach == null || detach.getError() != null)
        {
            _attachment = null;
        }
    }

    public void start()
    {
        getEndpoint().setLinkCredit(UnsignedInteger.valueOf(_destination.getCredit()));
        getEndpoint().setCreditWindow();
    }

    public ReceivingLinkEndpoint getEndpoint()
    {
        return _attachment.getEndpoint();
    }


    public Session_1_0 getSession()
    {
        ReceivingLinkAttachment attachment = _attachment;
        return attachment == null ? null : attachment.getSession();
    }

    public void handle(Binary deliveryTag, DeliveryState state, Boolean settled)
    {
        if(Boolean.TRUE.equals(settled))
        {
            _unsettledMap.remove(deliveryTag);
        }
    }

    public void setLinkAttachment(ReceivingLinkAttachment linkAttachment)
    {
        _attachment = linkAttachment;
        _receivingSettlementMode = linkAttachment.getEndpoint().getReceivingSettlementMode();
        ReceivingLinkEndpoint endpoint = linkAttachment.getEndpoint();
        Map initialUnsettledMap = endpoint.getInitialUnsettledMap();

        Map<Binary, Outcome> unsettledCopy = new HashMap<Binary, Outcome>(_unsettledMap);
        for(Map.Entry<Binary, Outcome> entry : unsettledCopy.entrySet())
        {
            Binary deliveryTag = entry.getKey();
            if(!initialUnsettledMap.containsKey(deliveryTag))
            {
                _unsettledMap.remove(deliveryTag);
            }
        }

    }

    public Map getUnsettledOutcomeMap()
    {
        return _unsettledMap;
    }

    public ReceivingDestination getDestination()
    {
        return _destination;
    }
}
