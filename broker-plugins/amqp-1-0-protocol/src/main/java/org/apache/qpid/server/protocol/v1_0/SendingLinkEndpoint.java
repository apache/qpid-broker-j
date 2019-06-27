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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.txn.AsyncAutoCommitTransaction;
import org.apache.qpid.server.txn.AsyncCommand;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.LinkRegistryModel;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class SendingLinkEndpoint extends AbstractLinkEndpoint<Source, Target>
        implements AsyncAutoCommitTransaction.FutureRecorder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SendingLinkEndpoint.class);
    private static final Symbol PRIORITY = Symbol.valueOf("priority");
    private static final Pattern ANY_CONTAINER_ID = Pattern.compile(".*");

    private final List<Binary> _resumeAcceptedTransfers = new ArrayList<>();
    private final List<MessageInstance> _resumeFullTransfers = new ArrayList<>();
    private final Map<Binary, OutgoingDelivery> _unsettled = new ConcurrentHashMap<>();
    private final AsyncAutoCommitTransaction _asyncAutoCommitTransaction;
    private final java.util.Queue<AsyncCommand> _unfinishedCommandsQueue = new ConcurrentLinkedQueue<>();

    // TODO: QPID-7845 : remove after implementation of link resuming
    private final Action<Session_1_0> _cleanUpUnsettledDeliveryTask = object -> cleanUpUnsettledDeliveries();

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
        setCapabilities(Collections.singletonList(AMQPConnection_1_0.SHARED_SUBSCRIPTIONS));
        _asyncAutoCommitTransaction =
                new AsyncAutoCommitTransaction(getSession().getConnection().getAddressSpace().getMessageStore(), this);
    }

    @Override
    public void start()
    {
    }

    private void prepareConsumerOptionsAndFilters(final SendingDestination destination) throws AmqpErrorException
    {
        // TODO QPID-7952: this method might modify the source. this is not good encapsulation. furthermore if it does so then it should inform the link/linkregistry about it!
        _destination = destination;
        final Source source = getSource();

        EnumSet<ConsumerOption> options = EnumSet.noneOf(ConsumerOption.class);

        boolean noLocal = false;
        JMSSelectorFilter messageFilter = null;

        if(destination instanceof ExchangeSendingDestination)
        {
            options.add(ConsumerOption.ACQUIRES);
            options.add(ConsumerOption.SEES_REQUEUES);
        }
        else if(destination instanceof StandardSendingDestination)
        {
            MessageSource messageSource = _destination.getMessageSource();

            if(messageSource instanceof Queue && ((Queue<?>)messageSource).getAvailableAttributes().contains("topic"))
            {
                source.setDistributionMode(StdDistMode.COPY);
            }

            Map<Symbol,Filter> filters = source.getFilter();

            Map<Symbol,Filter> actualFilters = new HashMap<>();

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
                    else if (entry.getValue() instanceof Filter.InvalidFilter)
                    {
                        Error error = new Error();
                        error.setCondition(AmqpError.NOT_IMPLEMENTED);
                        error.setDescription("Unsupported filter type: " + ((Filter.InvalidFilter)entry.getValue()).getDescriptor());
                        error.setInfo(Collections.singletonMap(Symbol.valueOf("field"), Symbol.valueOf("filter")));
                        throw new AmqpErrorException(error);
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
                                                 _destination instanceof ExchangeSendingDestination
                                                 || source.getDistributionMode() != StdDistMode.COPY);
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
        catch (AccessControlException e)
        {
            throw new AmqpErrorException(new Error(AmqpError.UNAUTHORIZED_ACCESS, e.getMessage()));
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
                        // ignore
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

        if (getDestination() instanceof ExchangeSendingDestination && !Boolean.TRUE.equals(newSource.getDynamic()))
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

        if (getDestination() instanceof ExchangeSendingDestination && !Boolean.TRUE.equals(newSource.getDynamic()))
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
        Source source = getSource();
        if (source == null && attach.getDesiredCapabilities() != null)
        {
            List<Symbol> capabilities = Arrays.asList(attach.getDesiredCapabilities());
            if (capabilities.contains(Session_1_0.GLOBAL_CAPABILITY)
                && capabilities.contains(Session_1_0.SHARED_CAPABILITY)
                && getLinkName().endsWith("|global"))
            {
                final NamedAddressSpace namedAddressSpace = getSession().getConnection().getAddressSpace();
                final Pattern linkNamePattern = Pattern.compile("^" + Pattern.quote(getLinkName()) + "$");
                namedAddressSpace.visitSendingLinks((LinkRegistryModel.LinkVisitor<Link_1_0<Source, Target>>) link -> {
                    if (link.getSource() != null
                        && ANY_CONTAINER_ID.matcher(link.getRemoteContainerId()).matches()
                        && linkNamePattern.matcher(link.getName()).matches())
                    {
                        getLink().setSource(new Source(link.getSource()));
                        return true;
                    }
                    return false;
                });
            }
        }

        source = getSource();
        if (source == null)
        {
            throw new AmqpErrorException(new Error(AmqpError.NOT_FOUND, "Link not found"));
        }

        attach.setSource(source);
        receiveAttach(attach);
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

        s.sendTransfer(xfr, this);
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
            final Binary transactionId = (Binary) properties.get(Symbol.valueOf("txn-id"));
            if (transactionId != null)
            {
                try
                {
                    getSession().getTransaction(transactionId);
                }
                catch (UnknownTransactionException e)
                {
                    close(new Error(TransactionError.UNKNOWN_ID, e.getMessage()));
                    return;
                }
            }

            _transactionId = transactionId;
        }

        if(receiverDeliveryCount == null)
        {
            setLinkCredit(receiverLinkCredit);
        }
        else
        {
            // 2.6.7 Flow Control : link_credit_snd := delivery_count_rcv + link_credit_rcv - delivery_count_snd
            SequenceNumber limit =
                    new SequenceNumber(receiverDeliveryCount.intValue()).add(receiverLinkCredit.intValue());
            if (limit.compareTo(getDeliveryCount()) <= 0)
            {
                setLinkCredit(UnsignedInteger.valueOf(0));
            }
            else
            {
                setLinkCredit(limit.subtract(getDeliveryCount().intValue()).unsignedIntegerValue());
            }
        }

        // send flow when echo=true or drain=true but link credit is zero
        boolean sendFlow = Boolean.TRUE.equals(flow.getEcho()) ||
                ( Boolean.TRUE.equals(flow.getDrain()) && getLinkCredit().equals(UnsignedInteger.ZERO));

        flowStateChanged();

        if (sendFlow)
        {
            sendFlow();
        }
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
        TerminusExpiryPolicy expiryPolicy = getSource().getExpiryPolicy();
        if (Boolean.TRUE.equals(detach.getClosed())
            || TerminusExpiryPolicy.LINK_DETACH.equals(expiryPolicy)
            || ((expiryPolicy == null || TerminusExpiryPolicy.SESSION_END.equals(expiryPolicy)) && getSession().isClosing())
            || (TerminusExpiryPolicy.CONNECTION_CLOSE.equals(expiryPolicy) && getSession().getConnection().isClosing()))
        {
            cleanUpUnsettledDeliveries();

            close();
        }
        else if (detach.getError() != null)
        {
            cleanUpUnsettledDeliveries();
            detach();
            destroy();
            getConsumerTarget().updateNotifyWorkDesired();
        }
        else
        {
            detach();

            // TODO: QPID-7845 : Resuming links is unsupported at the moment. Destroying link unconditionally.
            destroy();
            getConsumerTarget().updateNotifyWorkDesired();
        }
    }

    private void cleanUpUnsettledDeliveries()
    {
        getSession().removeDeleteTask(_cleanUpUnsettledDeliveryTask);
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
        _unsettled.clear();
    }

    void addUnsettled(final Binary tag, final UnsettledAction unsettledAction, final MessageInstance messageInstance)
    {
        _unsettled.put(tag, new OutgoingDelivery(messageInstance, unsettledAction, null));
    }

    @Override
    protected void handleDeliveryState(final Binary deliveryTag, final DeliveryState state, final Boolean settled)
    {
        OutgoingDelivery outgoingDelivery = _unsettled.get(deliveryTag);
        boolean localSettle = false;
        if(outgoingDelivery != null && outgoingDelivery.getAction() != null)
        {
            UnsettledAction action = outgoingDelivery.getAction();
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

    AsyncAutoCommitTransaction getAsyncAutoCommitTransaction()
    {
        return _asyncAutoCommitTransaction;
    }

    public boolean hasCreditToSend()
    {
        UnsignedInteger linkCredit = getLinkCredit();
        return linkCredit != null && (linkCredit.compareTo(UnsignedInteger.valueOf(0)) > 0)
               && getSession().hasCreditToSend();
    }

    void updateDisposition(final Binary deliveryTag, DeliveryState state, boolean settled)
    {
        if (settled && (_unsettled.remove(deliveryTag) != null))
        {
            getSession().updateDisposition(this, deliveryTag, state, settled);
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
            getLink().setSource(source);
            prepareConsumerOptionsAndFilters(destination);
        }

        getLink().setTarget(target);


        final MessageInstanceConsumer oldConsumer = getConsumer();
        createConsumerTarget();
        _resumeAcceptedTransfers.clear();
        _resumeFullTransfers.clear();
        final NamedAddressSpace addressSpace = getSession().getConnection().getAddressSpace();

        // TODO: QPID-7845 : Resuming links is unsupported at the moment. Thus, cleaning up unsettled deliveries unconditionally.
        cleanUpUnsettledDeliveries();
        getSession().addDeleteTask(_cleanUpUnsettledDeliveryTask);

        Map<Binary, OutgoingDelivery> unsettledCopy = new HashMap<>(_unsettled);
        Map<Binary, DeliveryState> remoteUnsettled =
                attach.getUnsettled() == null ? Collections.emptyMap() : new HashMap<>(attach.getUnsettled());

        final boolean isUnsettledComplete = !Boolean.TRUE.equals(attach.getIncompleteUnsettled());
        for (Map.Entry<Binary, OutgoingDelivery> entry : unsettledCopy.entrySet())
        {
            Binary deliveryTag = entry.getKey();
            final MessageInstance queueEntry = entry.getValue().getMessageInstance();
            if (!remoteUnsettled.containsKey(deliveryTag) && isUnsettledComplete)
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
                                            @Override
                                            public void postCommit()
                                            {
                                                queueEntry.delete();
                                            }

                                            @Override
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
                                        @Override
                                        public void postCommit()
                                        {
                                            queueEntry.release(oldConsumer);
                                        }

                                        @Override
                                        public void onRollback()
                                        {
                                        }
                                    });
                    }
                }

                // TODO: QPID-7845: Handle rejected and modified outcome

                remoteUnsettled.remove(deliveryTag);
                _resumeAcceptedTransfers.add(deliveryTag);
            }
            else
            {
                _resumeFullTransfers.add(queueEntry);

                // TODO:QPID-7845: exists in receivers map, but not yet got an outcome ... should resend with resume = true
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

    @Override
    protected void detach(Error error, final boolean close)
    {
        if (_consumerTarget != null)
        {
            _consumerTarget.close();
        }

        Source source = getSource();
        TerminusExpiryPolicy expiryPolicy = source.getExpiryPolicy();
        NamedAddressSpace addressSpace = getSession().getConnection().getAddressSpace();
        List<Symbol> sourceCapabilities = source.getCapabilities() == null ? Collections.emptyList() : Arrays.asList(source.getCapabilities());

        if (close
            || TerminusExpiryPolicy.LINK_DETACH.equals(expiryPolicy)
            || ((expiryPolicy == null || TerminusExpiryPolicy.SESSION_END.equals(expiryPolicy)) && getSession().isClosing())
            || (TerminusExpiryPolicy.CONNECTION_CLOSE.equals(expiryPolicy) && getSession().getConnection().isClosing()))
        {
            cleanUpUnsettledDeliveries();
        }

        if (close)
        {
            Error closingError = null;
            if (getDestination() instanceof ExchangeSendingDestination
                && addressSpace instanceof QueueManagingVirtualHost && TerminusExpiryPolicy.NEVER.equals(expiryPolicy))
            {
                try
                {
                    ((QueueManagingVirtualHost) addressSpace).removeSubscriptionQueue(
                            ((ExchangeSendingDestination) getDestination()).getQueue().getName());

                    TerminusDurability sourceDurability = source.getDurable();
                    if (sourceDurability != null
                        && !TerminusDurability.NONE.equals(sourceDurability)
                        && sourceCapabilities.contains(Session_1_0.SHARED_CAPABILITY)
                        && sourceCapabilities.contains(ExchangeSendingDestination.TOPIC_CAPABILITY))
                    {
                        final Pattern containerIdPattern = sourceCapabilities.contains(Session_1_0.GLOBAL_CAPABILITY)
                                ? ANY_CONTAINER_ID
                                : Pattern.compile("^" + Pattern.quote(getSession().getConnection().getRemoteContainerId()) + "$");
                        final Pattern linkNamePattern = Pattern.compile("^" + Pattern.quote(getLinkName()) + "\\|?\\d*$");

                        addressSpace.visitSendingLinks((LinkRegistryModel.LinkVisitor<Link_1_0<Source, Target>>) link -> {
                            if (containerIdPattern.matcher(link.getRemoteContainerId()).matches()
                                && linkNamePattern.matcher(link.getName()).matches())
                            {
                                link.linkClosed();
                            }
                            return false;
                        });
                    }
                }
                catch (AccessControlException e)
                {
                    LOGGER.error("Error unregistering subscription", e);
                    closingError = new Error(AmqpError.NOT_ALLOWED, "Error unregistering subscription");
                }
                catch (IllegalStateException e)
                {
                    String message;
                    if(sourceCapabilities.contains(Session_1_0.SHARED_CAPABILITY)
                       && sourceCapabilities.contains(ExchangeSendingDestination.TOPIC_CAPABILITY))
                    {
                        String subscriptionName = getLinkName();
                        int separator = subscriptionName.indexOf("|");
                        if (separator > 0)
                        {
                            subscriptionName = subscriptionName.substring(0, separator);
                        }
                        message = "There are active consumers on the shared subscription '"+subscriptionName+"'";
                    }
                    else
                    {
                        message = e.getMessage();
                    }
                    closingError = new Error(AmqpError.RESOURCE_LOCKED, message);
                }
                catch (NotFoundException e)
                {
                    closingError = new Error(AmqpError.NOT_FOUND, e.getMessage());
                }
            }
            if (error == null)
            {
                error = closingError;
            }
            else
            {
                LOGGER.warn("Unexpected error on detaching endpoint {}: {}", getLinkName(), error);
            }
        }
        else if (addressSpace instanceof QueueManagingVirtualHost
                 && ((QueueManagingVirtualHost) addressSpace).isDiscardGlobalSharedSubscriptionLinksOnDetach()
                 && sourceCapabilities.contains(Session_1_0.SHARED_CAPABILITY)
                 && sourceCapabilities.contains(Session_1_0.GLOBAL_CAPABILITY)
                 && sourceCapabilities.contains(ExchangeSendingDestination.TOPIC_CAPABILITY)
                 && !getLinkName().endsWith("|global"))
        {
            // For JMS 2.0 global shared subscriptions we do not want to keep the links hanging around.
            // However, we keep one link (ending with "|global") to perform a null-source lookup upon un-subscription.
            getLink().linkClosed();
        }
        super.detach(error, close);
    }

    @Override
    public void receiveComplete()
    {
        AsyncCommand cmd;
        while((cmd = _unfinishedCommandsQueue.poll()) != null)
        {
            cmd.complete();
        }
    }

    @Override
    public void recordFuture(final ListenableFuture<Void> future, final ServerTransaction.Action action)
    {
        _unfinishedCommandsQueue.add(new AsyncCommand(future, action));
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
