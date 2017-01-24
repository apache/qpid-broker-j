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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.filter.SelectorParsingException;
import org.apache.qpid.filter.selector.ParseException;
import org.apache.qpid.filter.selector.TokenMgrError;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.JMSSelectorFilter;
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Released;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class SendingLink_1_0 implements Link_1_0
{
    private static final Logger _logger = LoggerFactory.getLogger(SendingLink_1_0.class);
    private final EnumSet<ConsumerOption> _consumerOptions;
    private final FilterManager _consumerFilters;

    private NamedAddressSpace _addressSpace;
    private volatile SendingDestination _destination;

    private MessageInstanceConsumer _consumer;
    private ConsumerTarget_1_0 _target;

    private boolean _draining;
    private final Map<Binary, MessageInstance> _unsettledMap =
            new HashMap<Binary, MessageInstance>();

    private final ConcurrentMap<Binary, UnsettledAction> _unsettledActionMap =
            new ConcurrentHashMap<Binary, UnsettledAction>();
    private volatile SendingLinkAttachment _linkAttachment;
    private TerminusDurability _durability;
    private List<MessageInstance> _resumeFullTransfers = new ArrayList<MessageInstance>();
    private List<Binary> _resumeAcceptedTransfers = new ArrayList<Binary>();
    private Runnable _closeAction;


    public SendingLink_1_0(final SendingLinkAttachment linkAttachment,
                           final NamedAddressSpace addressSpace,
                           final SendingDestination destination)
            throws AmqpErrorException
    {
        _addressSpace = addressSpace;
        _destination = destination;
        _linkAttachment = linkAttachment;
        final Source source = (Source) linkAttachment.getSource();
        _durability = source.getDurable();

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

    void createConsumerTarget() throws AmqpErrorException
    {
        final Source source = (Source) getEndpoint().getSource();
        _target = new ConsumerTarget_1_0(this, _destination instanceof ExchangeDestination ? true : source.getDistributionMode() != StdDistMode.COPY);
        try
        {
            final String name;
            if(getEndpoint().getTarget() instanceof Target)
            {
                Target target = (Target) getEndpoint().getTarget();
                name = target.getAddress() == null ? getEndpoint().getName() : target.getAddress();
            }
            else
            {
                name = getEndpoint().getName();
            }

            _consumer = _destination.getMessageSource()
                                    .addConsumer(_target,
                                                 _consumerFilters,
                                                 Message_1_0.class,
                                                 name,
                                                 _consumerOptions,
                                                 getEndpoint().getPriority());
            _target.updateNotifyWorkDesired();
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

    public void remoteDetached(final LinkEndpoint endpoint, final Detach detach)
    {
        _target.close();
        //TODO
        // if not durable or close
        if (Boolean.TRUE.equals(detach.getClosed())
            || !(TerminusDurability.UNSETTLED_STATE.equals(_durability) || TerminusDurability.CONFIGURATION.equals( _durability)))
        {

            Modified state = new Modified();
            state.setDeliveryFailed(true);

            for(UnsettledAction action : _unsettledActionMap.values())
            {
                action.process(state,Boolean.TRUE);
            }
            _unsettledActionMap.clear();

            endpoint.close();

            if(_destination instanceof ExchangeDestination
               && (_durability == TerminusDurability.CONFIGURATION
                    || _durability == TerminusDurability.UNSETTLED_STATE))
            {
                try
                {
                    if (getAddressSpace() instanceof QueueManagingVirtualHost)
                    {
                        ((QueueManagingVirtualHost) getAddressSpace()).removeSubscriptionQueue(((ExchangeDestination) _destination).getQueue().getName());
                    }
                }
                catch (AccessControlException e)
                {
                    _logger.error("Error unregistering subscription", e);
                    endpoint.detach(new Error(AmqpError.NOT_ALLOWED, "Error unregistering subscription"));
                }
                catch (IllegalStateException e)
                {
                    endpoint.detach(new Error(AmqpError.RESOURCE_LOCKED, e.getMessage()));
                }
                catch (NotFoundException e)
                {
                    endpoint.detach(new Error(AmqpError.NOT_FOUND, e.getMessage()));
                }
            }

            if(_closeAction != null)
            {
                _closeAction.run();
            }

        }
        else if (detach.getError() != null && !_linkAttachment.getEndpoint()
                                                              .getSession()
                                                              .isSyntheticError(detach.getError()))
        {
            _linkAttachment = null;
            _target.flowStateChanged();
        }
        else
        {
            endpoint.detach();
            _target.updateNotifyWorkDesired();
        }
    }

    public void start()
    {
        //TODO
    }

    public SendingLinkEndpoint getEndpoint()
    {
        return _linkAttachment == null ? null : _linkAttachment.getEndpoint() ;
    }

    public Session_1_0 getSession()
    {
        return _linkAttachment == null ? null : _linkAttachment.getSession();
    }

    public void flowStateChanged()
    {
        if(Boolean.TRUE.equals(getEndpoint().getDrain())
                && hasCredit())
        {
            _draining = true;
            _target.flush();
        }

        while(!_resumeAcceptedTransfers.isEmpty() && getEndpoint().hasCreditToSend())
        {
            Accepted accepted = new Accepted();
            Transfer xfr = new Transfer();
            Binary dt = _resumeAcceptedTransfers.remove(0);
            xfr.setDeliveryTag(dt);
            xfr.setState(accepted);
            xfr.setResume(Boolean.TRUE);
            getEndpoint().transfer(xfr, true);
            xfr.dispose();
        }
        if(_resumeAcceptedTransfers.isEmpty())
        {
            _target.flowStateChanged();
        }

    }

    boolean hasCredit()
    {
        return getEndpoint().getLinkCredit().compareTo(UnsignedInteger.ZERO) > 0;
    }

    public boolean isDraining()
    {
        return false;  //TODO
    }

    public boolean drained()
    {
        if(getEndpoint() != null)
        {
            if (_draining)
            {
                //TODO
                getEndpoint().drained();
                _draining = false;
                return true;
            }
            else
            {
                return false;
            }

        }
        else
        {
            return false;
        }
    }

    public void addUnsettled(Binary tag, UnsettledAction unsettledAction, MessageInstance queueEntry)
    {
        _unsettledActionMap.put(tag,unsettledAction);
        if(getTransactionId() == null)
        {
            _unsettledMap.put(tag, queueEntry);
        }
    }

    public void removeUnsettled(Binary tag)
    {
        _unsettledActionMap.remove(tag);
    }

    public void handle(Binary deliveryTag, DeliveryState state, Boolean settled)
    {
        UnsettledAction action = _unsettledActionMap.get(deliveryTag);
        boolean localSettle = false;
        if(action != null)
        {
            localSettle = action.process(state, settled);
            if(localSettle && !Boolean.TRUE.equals(settled))
            {
                _linkAttachment.updateDisposition(deliveryTag, state, true);
            }
        }
        if(Boolean.TRUE.equals(settled) || localSettle)
        {
            _unsettledActionMap.remove(deliveryTag);
            _unsettledMap.remove(deliveryTag);
        }
    }

    ServerTransaction getTransaction(Binary transactionId)
    {
        Session_1_0 session = _linkAttachment.getSession();
        return session == null ? null : session.getTransaction(transactionId);
    }

    public Binary getTransactionId()
    {
        SendingLinkEndpoint endpoint = getEndpoint();
        return endpoint == null ? null : endpoint.getTransactionId();
    }

    public boolean isDetached()
    {
        return _linkAttachment == null || getEndpoint().isDetached();
    }

    public boolean isAttached()
    {
        return _linkAttachment != null && getEndpoint().isAttached();
    }

    public synchronized void setLinkAttachment(SendingLinkAttachment linkAttachment) throws AmqpErrorException
    {
        _linkAttachment = linkAttachment;

        if (linkAttachment.getSession() != null)
        {
            SendingLinkEndpoint endpoint = linkAttachment.getEndpoint();
            Map initialUnsettledMap = endpoint.getInitialUnsettledMap();
            Map<Binary, MessageInstance> unsettledCopy = new HashMap<Binary, MessageInstance>(_unsettledMap);
            _resumeAcceptedTransfers.clear();
            _resumeFullTransfers.clear();

            createConsumerTarget();

            for (Map.Entry<Binary, MessageInstance> entry : unsettledCopy.entrySet())
            {
                Binary deliveryTag = entry.getKey();
                final MessageInstance queueEntry = entry.getValue();
                if (initialUnsettledMap == null || !initialUnsettledMap.containsKey(deliveryTag))
                {
                    queueEntry.setRedelivered();
                    queueEntry.release(_consumer);
                    _unsettledMap.remove(deliveryTag);
                }
                else if (initialUnsettledMap.get(deliveryTag) instanceof Outcome)
                {
                    Outcome outcome = (Outcome) initialUnsettledMap.get(deliveryTag);

                    if (outcome instanceof Accepted)
                    {
                        AutoCommitTransaction txn = new AutoCommitTransaction(_addressSpace.getMessageStore());
                        if (_consumer.acquires())
                        {
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
                        AutoCommitTransaction txn = new AutoCommitTransaction(_addressSpace.getMessageStore());
                        if (_consumer.acquires())
                        {
                            txn.dequeue(Collections.singleton(queueEntry),
                                        new ServerTransaction.Action()
                                        {
                                            public void postCommit()
                                            {
                                                queueEntry.release(_consumer);
                                            }

                                            public void onRollback()
                                            {
                                            }
                                        });
                        }
                    }
                    //_unsettledMap.remove(deliveryTag);
                    initialUnsettledMap.remove(deliveryTag);
                    _resumeAcceptedTransfers.add(deliveryTag);
                }
                else
                {
                    _resumeFullTransfers.add(queueEntry);
                    // exists in receivers map, but not yet got an outcome ... should resend with resume = true
                }
                // TODO - else
            }
        }

        _target.updateNotifyWorkDesired();
    }

    public Map getUnsettledOutcomeMap()
    {
        Map<Binary, MessageInstance> unsettled = new HashMap<Binary, MessageInstance>(_unsettledMap);

        for(Map.Entry<Binary, MessageInstance> entry : unsettled.entrySet())
        {
            entry.setValue(null);
        }

        return unsettled;
    }

    public void setCloseAction(Runnable action)
    {
        _closeAction = action;
    }

    public NamedAddressSpace getAddressSpace()
    {
        return _addressSpace;
    }

    public MessageInstanceConsumer getConsumer()
    {
        return _consumer;
    }

    public ConsumerTarget_1_0 getConsumerTarget()
    {
        return _target;
    }

    public SendingDestination getDestination()
    {
        return _destination;
    }

    public void setDestination(final SendingDestination destination)
    {
        _destination = destination;
    }
}
