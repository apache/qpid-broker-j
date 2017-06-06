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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.protocol.MessageFormatRegistry;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
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
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
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

public class StandardReceivingLinkEndpoint extends AbstractReceivingLinkEndpoint<Target>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardReceivingLinkEndpoint.class);

    private ArrayList<Transfer> _incompleteMessage;
    private boolean _resumedMessage;
    private Binary _messageDeliveryTag;
    private Map<Binary, Outcome> _unsettledMap = Collections.synchronizedMap(new HashMap<Binary, Outcome>());
    private ReceivingDestination _receivingDestination;

    public StandardReceivingLinkEndpoint(final Session_1_0 session,
                                         final Link_1_0<Source, Target> link)
    {
        super(session, link);
    }

    @Override
    public void start()
    {
        setLinkCredit(UnsignedInteger.valueOf(getReceivingDestination().getCredit()));
        setCreditWindow();
    }

    private TerminusDurability getDurability()
    {
        return getTarget().getDurable();
    }

    @Override
    protected Error messageTransfer(Transfer xfr)
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
                updateDisposition(_messageDeliveryTag, (DeliveryState) outcome, settled);
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
                MessageHandle<MessageMetaData_1_0> handle = getAddressSpace().getMessageStore().addMessage(mmd);

                for (EncodingRetainingSection<?> dataSection : dataSections)
                {
                    for (QpidByteBuffer buf : dataSection.getEncodedForm())
                    {
                        handle.addContent(buf);
                        buf.dispose();
                    }
                    dataSection.dispose();
                }
                final StoredMessage<MessageMetaData_1_0> storedMessage = handle.allContentAdded();
                Message_1_0 message = new Message_1_0(storedMessage, getSession().getConnection().getReference());

                routingAddress = getReceivingDestination().getRoutingAddress(message);
                serverMessage = message;
            }
            else
            {
                MessageFormat format = MessageFormatRegistry.getFormat(messageFormat.intValue());
                if(format != null)
                {
                    serverMessage = format.createMessage(fragments, getAddressSpace().getMessageStore(), getSession().getConnection().getReference());
                    routingAddress = format.getRoutingAddress(serverMessage, getReceivingDestination().getAddress());
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
                    transaction = new AutoCommitTransaction(getAddressSpace().getMessageStore());
                }

                try
                {
                    Session_1_0 session = getSession();
                    // locally cache arrival time to ensure that we don't reload metadata
                    final long arrivalTime = serverMessage.getArrivalTime();
                    session.getAMQPConnection()
                           .checkAuthorizedMessagePrincipal(serverMessage.getMessageHeader().getUserId());
                    getReceivingDestination().authorizePublish(session.getSecurityToken(), routingAddress);

                    Outcome outcome = getReceivingDestination().send(serverMessage, routingAddress, transaction, null);
                    Source source = getSource();

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

                    updateDisposition(deliveryTag, resultantState, settled);

                    getSession().getAMQPConnection()
                                .registerMessageReceived(serverMessage.getSize(), arrivalTime);

                    if (!(transaction instanceof AutoCommitTransaction))
                    {
                        ServerTransaction.Action a;
                        transaction.addPostTransactionAction(new ServerTransaction.Action()
                        {
                            public void postCommit()
                            {
                                updateDisposition(deliveryTag, null, true);
                            }

                            public void onRollback()
                            {
                                updateDisposition(deliveryTag, null, true);
                            }
                        });
                    }
                }
                catch (AccessControlException e)
                {
                    final Error err = new Error();
                    err.setCondition(AmqpError.NOT_ALLOWED);
                    err.setDescription(e.getMessage());
                    close(err);

                }
            }
            finally
            {
                reference.release();
            }
        }
        return null;
    }

    @Override
    protected void remoteDetachedPerformDetach(Detach detach)
    {
        if(!TerminusDurability.UNSETTLED_STATE.equals(getDurability()) ||
           (detach != null && Boolean.TRUE.equals(detach.getClosed())))
        {
            close();
        }
        else if(detach == null || detach.getError() != null)
        {
            detach();
            destroy();
        }
        else
        {
            detach();
        }
    }


    @Override
    protected void handle(Binary deliveryTag, DeliveryState state, Boolean settled)
    {
        if(Boolean.TRUE.equals(settled))
        {
            _unsettledMap.remove(deliveryTag);
        }
    }


    private MessageMetaData_1_0 createMessageMetaData(final List<QpidByteBuffer> fragments,
                                                      final List<EncodingRetainingSection<?>> dataSections)
    {

        List<EncodingRetainingSection<?>> sections;
        try
        {
            sections = getSectionDecoder().parseAll(fragments);
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
            }
            while (s instanceof DataSection);
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
            throw new ConnectionScopedRuntimeException(String.format("Encountered unexpected section '%s'", s.getClass().getSimpleName()));
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

    @Override
    public void attachReceived(final Attach attach) throws AmqpErrorException
    {
        super.attachReceived(attach);

        Source source = (Source) attach.getSource();
        Target target = new Target();
        Target attachTarget = (Target) attach.getTarget();

        setDeliveryCount(attach.getInitialDeliveryCount());

        target.setAddress(attachTarget.getAddress());
        target.setDynamic(attachTarget.getDynamic());
        if (Boolean.TRUE.equals(attachTarget.getDynamic()) && attachTarget.getDynamicNodeProperties() != null)
        {
            Map<Symbol, Object> dynamicNodeProperties = new HashMap<>();
            if (attachTarget.getDynamicNodeProperties().containsKey(Session_1_0.LIFETIME_POLICY))
            {
                dynamicNodeProperties.put(Session_1_0.LIFETIME_POLICY,
                                          attachTarget.getDynamicNodeProperties().get(Session_1_0.LIFETIME_POLICY));
            }
            target.setDynamicNodeProperties(dynamicNodeProperties);
        }
        target.setDurable(TerminusDurability.min(attachTarget.getDurable(),
                                                 getLink().getHighestSupportedTerminusDurability()));
        final List<Symbol> targetCapabilities = new ArrayList<>();
        if (attachTarget.getCapabilities() != null)
        {
            final List<Symbol> desiredCapabilities = Arrays.asList(attachTarget.getCapabilities());
            if (desiredCapabilities.contains(Symbol.valueOf("temporary-topic")))
            {
                targetCapabilities.add(Symbol.valueOf("temporary-topic"));
            }
            if (desiredCapabilities.contains(Symbol.valueOf("topic")))
            {
                targetCapabilities.add(Symbol.valueOf("topic"));
            }
            target.setCapabilities(targetCapabilities.toArray(new Symbol[targetCapabilities.size()]));
        }

        final ReceivingDestination destination = getSession().getReceivingDestination(getLink(), target);

        targetCapabilities.addAll(Arrays.asList(destination.getCapabilities()));
        target.setCapabilities(targetCapabilities.toArray(new Symbol[targetCapabilities.size()]));

        setCapabilities(targetCapabilities);
        setDestination(destination);

        Map initialUnsettledMap = getInitialUnsettledMap();
        Map<Binary, Outcome> unsettledCopy = new HashMap<Binary, Outcome>(_unsettledMap);
        for(Map.Entry<Binary, Outcome> entry : unsettledCopy.entrySet())
        {
            Binary deliveryTag = entry.getKey();
            if(!initialUnsettledMap.containsKey(deliveryTag))
            {
                _unsettledMap.remove(deliveryTag);
            }
        }

        getLink().setTermini(source, target);
    }

    @Override
    public void initialiseUnsettled()
    {
        _localUnsettled = new HashMap(_unsettledMap);
    }

    public ReceivingDestination getReceivingDestination()
    {
        return _receivingDestination;
    }

    public void setDestination(final ReceivingDestination receivingDestination)
    {
        _receivingDestination = receivingDestination;
    }

    @Override
    protected void recoverLink(final Attach attach) throws AmqpErrorException
    {
        if (getTarget() == null)
        {
            throw new AmqpErrorException(new Error(AmqpError.NOT_FOUND,
                                                   String.format("Link '%s' not found", getLinkName())));
        }

        Source source = (Source) attach.getSource();
        Target target = getTarget();

        final ReceivingDestination destination = getSession().getReceivingDestination(getLink(), getTarget());
        target.setCapabilities(destination.getCapabilities());
        setCapabilities(Arrays.asList(destination.getCapabilities()));
        setDestination(destination);
        attachReceived(attach);

        getLink().setTermini(source, target);
    }


    @Override
    protected void reattachLink(final Attach attach) throws AmqpErrorException
    {
        if (attach.getTarget() instanceof Coordinator)
        {
            throw new AmqpErrorException(new Error(AmqpError.PRECONDITION_FAILED, "Cannot reattach standard receiving Link as a transaction coordinator"));
        }

        attachReceived(attach);
    }

    @Override
    protected void resumeLink(final Attach attach) throws AmqpErrorException
    {
        if (getTarget() == null)
        {
            throw new IllegalStateException("Terminus should be set when resuming a Link.");
        }
        if (attach.getTarget() == null)
        {
            throw new IllegalStateException("Attach.getTarget should not be null when resuming a Link. That would be recovering the Link.");
        }
        if (attach.getTarget() instanceof Coordinator)
        {
            throw new AmqpErrorException(new Error(AmqpError.PRECONDITION_FAILED, "Cannot resume standard receiving Link as a transaction coordinator"));
        }

        attachReceived(attach);
        initialiseUnsettled();
    }

    @Override
    protected void establishLink(final Attach attach) throws AmqpErrorException
    {
        if (getSource() != null || getTarget() != null)
        {
            throw new IllegalStateException("Termini should be null when establishing a Link.");
        }

        attachReceived(attach);
    }
}
