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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.JMSSelectorFilter;
import org.apache.qpid.server.filter.SelectorParsingException;
import org.apache.qpid.server.filter.selector.ParseException;
import org.apache.qpid.server.filter.selector.TokenMgrError;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.NotFoundException;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Filter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Modified;
import org.apache.qpid.server.protocol.v1_0.type.messaging.NoLocalFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Released;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class SendingLinkEndpoint extends AbstractLinkEndpoint<Source, Target>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SendingLinkEndpoint.class);
    private static final Symbol PRIORITY = Symbol.valueOf("priority");

    private final List<Binary> _resumeAcceptedTransfers = new ArrayList<>();
    private final List<MessageInstance> _resumeFullTransfers = new ArrayList<>();
    private final Map<Binary, OutgoingDelivery> _unsettled = new ConcurrentHashMap<>();

    private volatile Binary _transactionId;
    private volatile Integer _priority;
    private volatile boolean _draining = false;
    private volatile SendingDestination _destination;
    private volatile EnumSet<ConsumerOption> _consumerOptions;
    private volatile FilterManager _consumerFilters;
    private volatile ConsumerTarget_1_0 _consumerTarget;
    private volatile MessageInstanceConsumer<ConsumerTarget_1_0> _consumer;

    public SendingLinkEndpoint(final Session_1_0 session, final LinkImpl<Source, Target> link)
    {
        super(session, link);
        setDeliveryCount(new SequenceNumber(0));
        setAvailable(UnsignedInteger.valueOf(0));
        setCapabilities(Arrays.asList(AMQPConnection_1_0.SHARED_SUBSCRIPTIONS));
    }

    @Override
    public void start()
    {
    }

    private void prepareConsumerOptionsAndFilters(final SendingDestination destination) throws AmqpErrorException
    {
        // TODO FIXME: this method might modify the source. this is not good encapsulation. furthermore if it does so then it should inform the link/linkregistry about it!
        _destination = destination;
        final Source source = getSource();

        EnumSet<ConsumerOption> options = EnumSet.noneOf(ConsumerOption.class);

        boolean noLocal = false;
        JMSSelectorFilter messageFilter = null;

        if(destination instanceof ExchangeDestination)
        {
            options.add(ConsumerOption.ACQUIRES);
            options.add(ConsumerOption.SEES_REQUEUES);
        }
        else if(destination instanceof MessageSourceDestination)
        {
            MessageSource messageSource = _destination.getMessageSource();

            if(messageSource instanceof Queue && ((Queue<?>)messageSource).getAvailableAttributes().contains("topic"))
            {
                source.setDistributionMode(StdDistMode.COPY);
            }

            Map<Symbol,Filter> filters = source.getFilter();

            Map<Symbol,Filter> actualFilters = new HashMap<Symbol,Filter>();

            if(filters != null)
            {
                for(Map.Entry<Symbol,Filter> entry : filters.entrySet())
                {
                    if(entry.getValue() instanceof NoLocalFilter)
                    {
                        actualFilters.put(entry.getKey(), entry.getValue());
                        noLocal = true;
                    }
                    else if(messageFilter == null && entry.getValue() instanceof org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter)
                    {

                        org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter selectorFilter = (org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter) entry.getValue();
                        try
                        {
                            messageFilter = new JMSSelectorFilter(selectorFilter.getValue());

                            actualFilters.put(entry.getKey(), entry.getValue());
                        }
                        catch (ParseException | SelectorParsingException | TokenMgrError e)
                        {
                            Error error = new Error();
                            error.setCondition(AmqpError.INVALID_FIELD);
                            error.setDescription("Invalid JMS Selector: " + selectorFilter.getValue());
                            error.setInfo(Collections.singletonMap(Symbol.valueOf("field"), Symbol.valueOf("filter")));
                            throw new AmqpErrorException(error);
                        }


                    }
                }
            }
            source.setFilter(actualFilters.isEmpty() ? null : actualFilters);

            if(source.getDistributionMode() != StdDistMode.COPY)
            {
                options.add(ConsumerOption.ACQUIRES);
                options.add(ConsumerOption.SEES_REQUEUES);
            }
        }
        else
        {
            throw new ConnectionScopedRuntimeException("Unknown destination type");
        }
        if(noLocal)
        {
            options.add(ConsumerOption.NO_LOCAL);
        }

        FilterManager filters = null;
        if(messageFilter != null)
        {
            filters = new FilterManager();
            filters.add(messageFilter.getName(), messageFilter);
        }
        _consumerOptions = options;
        _consumerFilters = filters;
    }

    private void createConsumerTarget() throws AmqpErrorException
    {
        final Source source = getSource();
        _consumerTarget = new ConsumerTarget_1_0(this,
                                         _destination instanceof ExchangeDestination ? true : source.getDistributionMode() != StdDistMode.COPY);
        try
        {
            final String name = getTarget().getAddress() == null ? getLinkName() : getTarget().getAddress();
            _consumer = _destination.getMessageSource()
                                    .addConsumer(_consumerTarget,
                                                 _consumerFilters,
                                                 Message_1_0.class,
                                                 name,
                                                 _consumerOptions,
                                                 getPriority());
            _consumerTarget.updateNotifyWorkDesired();
        }
        catch (MessageSource.ExistingExclusiveConsumer e)
        {
            String msg = "Cannot add a consumer to the destination as there is already an exclusive consumer";
            throw new AmqpErrorException(new Error(AmqpError.RESOURCE_LOCKED, msg), e);
        }
        catch (MessageSource.ExistingConsumerPreventsExclusive e)
        {
            String msg = "Cannot add an exclusive consumer to the destination as there is already a consumer";
            throw new AmqpErrorException(new Error(AmqpError.RESOURCE_LOCKED, msg), e);
        }
        catch (MessageSource.ConsumerAccessRefused e)
        {
            String msg = "Cannot add an exclusive consumer to the destination as there is an incompatible exclusivity policy";
            throw new AmqpErrorException(new Error(AmqpError.RESOURCE_LOCKED, msg), e);
        }
        catch (MessageSource.QueueDeleted e)
        {
            String msg = "Cannot add a consumer to the destination as the destination has been deleted";
            throw new AmqpErrorException(new Error(AmqpError.RESOURCE_DELETED, msg), e);
        }
    }


    @Override
    protected Map<Symbol, Object> initProperties(final Attach attach)
    {

        Map<Symbol, Object> peerProperties = attach.getProperties();
        if(peerProperties != null)
        {
            Map<Symbol, Object> actualProperties = new HashMap<>();
            if(peerProperties.containsKey(PRIORITY))
            {
                Object value = peerProperties.get(PRIORITY);
                if(value instanceof Number)
                {
                    _priority = ((Number)value).intValue();
                }
                else if(value instanceof String)
                {
                    try
                    {
                        _priority = Integer.parseInt(value.toString());
                    }
                    catch (NumberFormatException e)
                    {
                    }
                }
                if(_priority != null)
                {
                    actualProperties.put(PRIORITY, _priority);
                }
            }
            return actualProperties;
        }
        else
        {

            return Collections.emptyMap();
        }
    }

    @Override
    protected void reattachLink(final Attach attach) throws AmqpErrorException
    {
        if (getSource() == null)
        {
            throw new IllegalStateException("Terminus should be set when resuming a Link.");
        }
        if (attach.getSource() == null)
        {
            throw new IllegalStateException("Attach.getSource should not be null when resuming a Link. That would be recovering the Link.");
        }

        Source newSource = (Source) attach.getSource();
        Source oldSource = getSource();

        final SendingDestination destination = getSession().getSendingDestination(getLink(), oldSource);
        prepareConsumerOptionsAndFilters(destination);

        if (getDestination() instanceof ExchangeDestination && !Boolean.TRUE.equals(newSource.getDynamic()))
        {
            final SendingDestination newDestination =
                    getSession().getSendingDestination(getLink(), newSource);
            if (getSession().updateSourceForSubscription(this, newSource, newDestination))
            {
                setDestination(newDestination);
            }
        }

        attachReceived(attach);
    }

    @Override
    protected void resumeLink(final Attach attach) throws AmqpErrorException
    {
        if (getSource() == null)
        {
            throw new IllegalStateException("Terminus should be set when resuming a Link.");
        }
        if (attach.getSource() == null)
        {
            throw new IllegalStateException("Attach.getSource should not be null when resuming a Link. That would be recovering the Link.");
        }

        Source newSource = (Source) attach.getSource();
        Source oldSource = getSource();

        final SendingDestination destination = getSession().getSendingDestination(getLink(), oldSource);
        prepareConsumerOptionsAndFilters(destination);

        if (getDestination() instanceof ExchangeDestination && !Boolean.TRUE.equals(newSource.getDynamic()))
        {
            final SendingDestination newDestination =
                    getSession().getSendingDestination(getLink(), newSource);
            if (getSession().updateSourceForSubscription(this, newSource, newDestination))
            {
                setDestination(newDestination);
            }
        }

        attachReceived(attach);
    }

    @Override
    protected void establishLink(final Attach attach) throws AmqpErrorException
    {
        if (getSource() != null || getTarget() != null)
        {
            throw new IllegalStateException("LinkEndpoint and Termini should be null when establishing a Link.");
        }

        attachReceived(attach);
    }

    @Override
    protected void recoverLink(final Attach attach) throws AmqpErrorException
    {
        if (getSource() == null)
        {
            throw new AmqpErrorException(new Error(AmqpError.NOT_FOUND, ""));
        }

        final SendingDestination destination = getSession().getSendingDestination(getLink(), getSource());
        prepareConsumerOptionsAndFilters(destination);

        attachReceived(attach);
    }

    @Override
    public Role getRole()
    {
        return Role.SENDER;
    }

    private Integer getPriority()
    {
        return _priority;
    }

    void transfer(final Transfer xfr, final boolean decrementCredit)
    {
        Session_1_0 s = getSession();
        xfr.setMessageFormat(UnsignedInteger.ZERO);
        if(decrementCredit)
        {
            setLinkCredit(getLinkCredit().subtract(UnsignedInteger.ONE));
        }

        getDeliveryCount().incr();

        xfr.setHandle(getLocalHandle());

        s.sendTransfer(xfr, this, true);
    }


    boolean drained()
    {
        if (_draining)
        {
            getDeliveryCount().add(getLinkCredit().intValue());
            setLinkCredit(UnsignedInteger.ZERO);
            sendFlow();
            _draining = false;
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public void receiveFlow(final Flow flow)
    {
        UnsignedInteger receiverDeliveryCount = flow.getDeliveryCount();
        UnsignedInteger receiverLinkCredit = flow.getLinkCredit();
        setDrain(flow.getDrain());

        Map<Symbol, Object> properties = flow.getProperties();
        if (properties != null)
        {
             _transactionId = (Binary) properties.get(Symbol.valueOf("txn-id"));
        }

        if(receiverDeliveryCount == null)
        {
            setLinkCredit(receiverLinkCredit);
        }
        else
        {
            // 2.6.7 Flow Control : link_credit_snd := delivery_count_rcv + link_credit_rcv - delivery_count_snd
            UnsignedInteger limit = receiverDeliveryCount.add(receiverLinkCredit);
            if(limit.compareTo(getDeliveryCount().unsignedIntegerValue())<=0)
            {
                setLinkCredit(UnsignedInteger.valueOf(0));
            }
            else
            {
                setLinkCredit(limit.subtract(getDeliveryCount().unsignedIntegerValue()));
            }
        }
        flowStateChanged();

    }

    @Override
    public void flowStateChanged()
    {
        if(Boolean.TRUE.equals(getDrain()))
        {
            if(getLinkCredit().compareTo(UnsignedInteger.ZERO) > 0)
            {
                _draining = true;
                getSession().notifyWork(getConsumerTarget());
            }
        }
        else
        {
            _draining = false;
        }

        while(!_resumeAcceptedTransfers.isEmpty() && hasCreditToSend())
        {
            Accepted accepted = new Accepted();
            Transfer xfr = new Transfer();
            Binary dt = _resumeAcceptedTransfers.remove(0);
            xfr.setDeliveryTag(dt);
            xfr.setState(accepted);
            xfr.setResume(Boolean.TRUE);
            transfer(xfr, true);
            xfr.dispose();
        }
        if(_resumeAcceptedTransfers.isEmpty())
        {
            getConsumerTarget().flowStateChanged();
        }
    }


    @Override
    protected void remoteDetachedPerformDetach(final Detach detach)
    {
        getConsumerTarget().close();

        TerminusExpiryPolicy expiryPolicy = (getSource()).getExpiryPolicy();
        if (Boolean.TRUE.equals(detach.getClosed())
            || TerminusExpiryPolicy.LINK_DETACH.equals(expiryPolicy)
            || (TerminusExpiryPolicy.SESSION_END.equals(expiryPolicy) && getSession().isClosing())
            || (TerminusExpiryPolicy.CONNECTION_CLOSE.equals(expiryPolicy) && getSession().getConnection().isClosing()))
        {

            Modified state = new Modified();
            state.setDeliveryFailed(true);

            for (OutgoingDelivery delivery : _unsettled.values())
            {
                UnsettledAction action = delivery.getAction();
                if (action != null)
                {
                    action.process(state, Boolean.TRUE);
                    delivery.setAction(null);
                }
            }

            Error closingError = null;
            if (getDestination() instanceof ExchangeDestination
                && getSession().getConnection().getAddressSpace() instanceof QueueManagingVirtualHost)
            {
                try
                {
                    ((QueueManagingVirtualHost) getSession().getConnection().getAddressSpace()).removeSubscriptionQueue(
                            ((ExchangeDestination) getDestination()).getQueue().getName());
                }
                catch (AccessControlException e)
                {
                    LOGGER.error("Error unregistering subscription", e);
                    closingError = new Error(AmqpError.NOT_ALLOWED, "Error unregistering subscription");
                }
                catch (IllegalStateException e)
                {
                    closingError = new Error(AmqpError.RESOURCE_LOCKED, e.getMessage());
                }
                catch (NotFoundException e)
                {
                    closingError = new Error(AmqpError.NOT_FOUND, e.getMessage());
                }
            }
            close(closingError);
        }
        else if (detach.getError() != null)
        {
            detach();
            destroy();
            getConsumerTarget().updateNotifyWorkDesired();
        }
        else
        {
            detach();
            getConsumerTarget().updateNotifyWorkDesired();
        }
    }

    void addUnsettled(final Binary tag, final UnsettledAction unsettledAction, final MessageInstance messageInstance)
    {
        _unsettled.put(tag, new OutgoingDelivery(messageInstance, unsettledAction, null));
    }

    @Override
    protected void handleDeliveryState(final Binary deliveryTag, final DeliveryState state, final Boolean settled)
    {
        UnsettledAction action = _unsettled.get(deliveryTag).getAction();
        boolean localSettle = false;
        if(action != null)
        {
            localSettle = action.process(state, settled);
            if(localSettle && !Boolean.TRUE.equals(settled))
            {
                updateDisposition(deliveryTag, state, true);
            }
        }
        if(Boolean.TRUE.equals(settled) || localSettle)
        {
            _unsettled.remove(deliveryTag);
        }
    }

    public ServerTransaction getTransaction(Binary transactionId)
    {
        Session_1_0 session = getSession();
        return session == null ? null : session.getTransaction(transactionId);
    }

    public boolean hasCreditToSend()
    {
        UnsignedInteger linkCredit = getLinkCredit();
        return linkCredit != null && (linkCredit.compareTo(UnsignedInteger.valueOf(0)) > 0)
               && getSession().hasCreditToSend();
    }

    public void updateDisposition(final Binary deliveryTag, DeliveryState state, boolean settled)
    {
        if (settled && (_unsettled.remove(deliveryTag) != null))
        {
            getSession().updateDisposition(getRole(), deliveryTag, state, settled);
        }
    }

    public Binary getTransactionId()
    {
        return _transactionId;
    }

    @Override
    public void attachReceived(final Attach attach) throws AmqpErrorException
    {
        super.attachReceived(attach);

        Target target = (Target) attach.getTarget();
        Source source = getSource();
        if (source == null)
        {
            source = new Source();
            Source attachSource = (Source) attach.getSource();

            final Modified defaultOutcome = new Modified();
            defaultOutcome.setDeliveryFailed(true);
            source.setDefaultOutcome(defaultOutcome);
            source.setOutcomes(Accepted.ACCEPTED_SYMBOL, Released.RELEASED_SYMBOL, Rejected.REJECTED_SYMBOL);
            source.setAddress(attachSource.getAddress());
            source.setDynamic(attachSource.getDynamic());
            if (Boolean.TRUE.equals(attachSource.getDynamic()) && attachSource.getDynamicNodeProperties() != null)
            {
                Map<Symbol, Object> dynamicNodeProperties = new HashMap<>();
                if (attachSource.getDynamicNodeProperties().containsKey(Session_1_0.LIFETIME_POLICY))
                {
                    dynamicNodeProperties.put(Session_1_0.LIFETIME_POLICY,
                                              attachSource.getDynamicNodeProperties().get(Session_1_0.LIFETIME_POLICY));
                }
                source.setDynamicNodeProperties(dynamicNodeProperties);
            }
            source.setDurable(TerminusDurability.min(attachSource.getDurable(),
                                                     getLink().getHighestSupportedTerminusDurability()));
            source.setExpiryPolicy(attachSource.getExpiryPolicy());
            source.setDistributionMode(attachSource.getDistributionMode());
            source.setFilter(attachSource.getFilter());
            source.setCapabilities(attachSource.getCapabilities());
            final SendingDestination destination = getSession().getSendingDestination(getLink(), source);
            source.setCapabilities(destination.getCapabilities());
            if (destination instanceof ExchangeDestination)
            {
                ExchangeDestination exchangeDestination = (ExchangeDestination) destination;
                exchangeDestination.getQueue()
                                   .setAttributes(Collections.<String, Object>singletonMap(Queue.DESIRED_STATE,
                                                                                           org.apache.qpid.server.model.State.ACTIVE));
            }
            getLink().setSource(source);
            prepareConsumerOptionsAndFilters(destination);
        }

        getLink().setTarget(target);


        final MessageInstanceConsumer oldConsumer = getConsumer();
        createConsumerTarget();
        _resumeAcceptedTransfers.clear();
        _resumeFullTransfers.clear();
        final NamedAddressSpace addressSpace = getSession().getConnection().getAddressSpace();
        Map<Binary, OutgoingDelivery> unsettledCopy = new HashMap<>(_unsettled);
        Map<Binary, DeliveryState> remoteUnsettled =
                attach.getUnsettled() == null ? Collections.emptyMap() : new HashMap<>(attach.getUnsettled());

        for (Map.Entry<Binary, OutgoingDelivery> entry : unsettledCopy.entrySet())
        {
            Binary deliveryTag = entry.getKey();
            final MessageInstance queueEntry = entry.getValue().getMessageInstance();
            if (remoteUnsettled == null || !remoteUnsettled.containsKey(deliveryTag))
            {
                queueEntry.setRedelivered();
                queueEntry.release(oldConsumer);
                _unsettled.remove(deliveryTag);
            }
            else if (remoteUnsettled.get(deliveryTag) instanceof Outcome)
            {
                Outcome outcome = (Outcome) remoteUnsettled.get(deliveryTag);

                if (outcome instanceof Accepted)
                {
                    if (oldConsumer.acquires())
                    {
                        AutoCommitTransaction txn = new AutoCommitTransaction(addressSpace.getMessageStore());
                        if (queueEntry.acquire() || queueEntry.isAcquired())
                        {
                            txn.dequeue(Collections.singleton(queueEntry),
                                        new ServerTransaction.Action()
                                        {
                                            public void postCommit()
                                            {
                                                queueEntry.delete();
                                            }

                                            public void onRollback()
                                            {
                                            }
                                        });
                        }
                    }
                }
                else if (outcome instanceof Released)
                {
                    if (oldConsumer.acquires())
                    {
                        AutoCommitTransaction txn = new AutoCommitTransaction(addressSpace.getMessageStore());
                        txn.dequeue(Collections.singleton(queueEntry),
                                    new ServerTransaction.Action()
                                    {
                                        public void postCommit()
                                        {
                                            queueEntry.release(oldConsumer);
                                        }

                                        public void onRollback()
                                        {
                                        }
                                    });
                    }
                }

                // TODO: Handle rejected and modified outcome

                remoteUnsettled.remove(deliveryTag);
                _resumeAcceptedTransfers.add(deliveryTag);
            }
            else
            {
                _resumeFullTransfers.add(queueEntry);

                // TODO: exists in receivers map, but not yet got an outcome ... should resend with resume = true
            }
        }

        getConsumerTarget().updateNotifyWorkDesired();
    }

    @Override
    protected Map<Binary, DeliveryState> getLocalUnsettled()
    {
        Map<Binary, DeliveryState> unsettled = new HashMap<>();
        for (Map.Entry<Binary, OutgoingDelivery> entry : _unsettled.entrySet())
        {
            unsettled.put(entry.getKey(), entry.getValue().getLocalState());
        }
        return unsettled;
    }

    private MessageInstanceConsumer<ConsumerTarget_1_0> getConsumer()
    {
        return _consumer;
    }

    ConsumerTarget_1_0 getConsumerTarget()
    {
        return _consumerTarget;
    }

    public SendingDestination getDestination()
    {
        return _destination;
    }

    public void setDestination(final SendingDestination destination)
    {
        _destination = destination;
    }

    private static class OutgoingDelivery
    {
        private final MessageInstance _messageInstance;
        private volatile UnsettledAction _action;
        private volatile DeliveryState _localState;

        public OutgoingDelivery(final MessageInstance messageInstance,
                                final UnsettledAction action,
                                final DeliveryState localState)
        {
            _messageInstance = messageInstance;
            _action = action;
            _localState = localState;
        }

        public MessageInstance getMessageInstance()
        {
            return _messageInstance;
        }

        public UnsettledAction getAction()
        {
            return _action;
        }

        public DeliveryState getLocalState()
        {
            return _localState;
        }

        public void setLocalState(final DeliveryState localState)
        {
            _localState = localState;
        }

        public void setAction(final UnsettledAction action)
        {
            _action = action;
        }
    }
}
