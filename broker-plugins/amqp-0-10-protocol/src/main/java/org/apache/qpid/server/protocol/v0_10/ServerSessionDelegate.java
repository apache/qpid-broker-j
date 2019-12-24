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
package org.apache.qpid.server.protocol.v0_10;

import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.filter.ArrivalTimeFilter;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.FilterManagerFactory;
import org.apache.qpid.server.filter.MessageFilter;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.logging.messages.ExchangeMessages;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.RejectType;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.NoFactoryForTypeException;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_10.transport.*;
import org.apache.qpid.server.queue.QueueArgumentsConverter;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.txn.AlreadyKnownDtxException;
import org.apache.qpid.server.txn.DtxNotSelectedException;
import org.apache.qpid.server.txn.IncorrectDtxStateException;
import org.apache.qpid.server.txn.JoinAndResumeDtxException;
import org.apache.qpid.server.txn.NotAssociatedDtxException;
import org.apache.qpid.server.txn.RollbackOnlyDtxException;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.txn.SuspendAndFailDtxException;
import org.apache.qpid.server.txn.TimeoutDtxException;
import org.apache.qpid.server.txn.UnknownDtxBranchException;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.MessageDestinationIsAlternateException;
import org.apache.qpid.server.virtualhost.RequiredExchangeException;
import org.apache.qpid.server.virtualhost.ReservedExchangeNameException;
import org.apache.qpid.server.virtualhost.UnknownAlternateBindingException;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public class ServerSessionDelegate extends MethodDelegate<ServerSession> implements ProtocolDelegate<ServerSession>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerSessionDelegate.class);
    private final AtomicBoolean _closed = new AtomicBoolean();

    public ServerSessionDelegate()
    {

    }

    @Override
    public void command(ServerSession session, Method method)
    {
        try
        {
            if(!session.isClosing())
            {
                Object asyncCommandMark = session.getAsyncCommandMark();
                command(session, method, false);
                Object newOutstanding = session.getAsyncCommandMark();
                if(newOutstanding == null || newOutstanding == asyncCommandMark)
                {
                    session.processed(method);
                }

                if(newOutstanding != null)
                {
                    session.completeAsyncCommands();
                }

                if (method.isSync())
                {
                    session.awaitCommandCompletion();
                    session.flushProcessed();
                }
            }
        }
        catch(ServerScopedRuntimeException | ConnectionScopedRuntimeException e)
        {
            throw e;
        }
        catch(RuntimeException e)
        {
            LOGGER.error("Exception processing command", e);
            exception(session, method, ExecutionErrorCode.INTERNAL_ERROR, "Exception processing command: " + e);
        }
    }

    @Override
    public void messageAccept(ServerSession session, MessageAccept method)
    {
        final ServerSession serverSession = session;
        serverSession.accept(method.getTransfers());
        if(!serverSession.isTransactional())
        {
            serverSession.recordFuture(Futures.<Void>immediateFuture(null),
                                       new CommandProcessedAction(serverSession, method));
        }
    }

    @Override
    public void messageReject(ServerSession session, MessageReject method)
    {
        session.reject(method.getTransfers());
    }

    @Override
    public void messageRelease(ServerSession session, MessageRelease method)
    {
        session.release(method.getTransfers(), method.getSetRedelivered());
    }

    @Override
    public void messageAcquire(ServerSession session, MessageAcquire method)
    {
        RangeSet acquiredRanges = session.acquire(method.getTransfers());

        Acquired result = new Acquired(acquiredRanges);


        session.executionResult((int) method.getId(), result);


    }

    @Override
    public void messageResume(ServerSession session, MessageResume method)
    {
        super.messageResume(session, method);
    }

    @Override
    public void messageSubscribe(ServerSession session, MessageSubscribe method)
    {
        /*
          TODO - work around broken Python tests
          Correct code should read like
          if not hasAcceptMode() exception ILLEGAL_ARGUMENT "Accept-mode not supplied"
          else if not method.hasAcquireMode() exception ExecutionErrorCode.ILLEGAL_ARGUMENT, "Acquire-mode not supplied"
        */
        if(!method.hasAcceptMode())
        {
            method.setAcceptMode(MessageAcceptMode.EXPLICIT);
        }
        if(!method.hasAcquireMode())
        {
            method.setAcquireMode(MessageAcquireMode.PRE_ACQUIRED);

        }

        if(!method.hasQueue())
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, "queue not supplied");
        }
        else
        {
            String destination = method.getDestination();

            if (destination == null)
            {
                exception(session, method, ExecutionErrorCode.INVALID_ARGUMENT, "Subscriber must provide a destination. The protocol specification marking the destination argument as optional is considered a mistake.");
            }
            else if(session.getSubscription(destination) != null)
            {
                exception(session, method, ExecutionErrorCode.NOT_ALLOWED, "Subscription already exists with destination '"+destination+"'");
            }
            else
            {
                String queueName = method.getQueue();
                NamedAddressSpace addressSpace = getAddressSpace(session);

                final Collection<MessageSource> sources = new HashSet<>();
                final MessageSource queue = addressSpace.getAttainedMessageSource(queueName);

                if(method.getArguments() != null && method.getArguments().get("x-multiqueue") instanceof Collection)
                {
                    for(Object object : (Collection<Object>)method.getArguments().get("x-multiqueue"))
                    {
                        String sourceName = String.valueOf(object);
                        sourceName = sourceName.trim();
                        if(sourceName.length() != 0)
                        {
                            MessageSource source = addressSpace.getAttainedMessageSource(sourceName);
                            if(source == null)
                            {
                                sources.clear();
                                break;
                            }
                            else
                            {
                                sources.add(source);
                            }
                        }
                    }
                    queueName = method.getArguments().get("x-multiqueue").toString();
                }
                else if(queue != null)
                {
                    sources.add(queue);
                }

                if(sources.isEmpty())
                {
                    exception(session, method, ExecutionErrorCode.NOT_FOUND, "Queue: " + queueName + " not found");
                }
                else if(!verifySessionAccess(session, sources))
                {
                    exception(session,method,ExecutionErrorCode.RESOURCE_LOCKED, "Exclusive Queue: " + queueName + " owned exclusively by another session");
                }
                else
                {
                    ProtocolEngine protocolEngine = getServerConnection(session).getAmqpConnection();
                    FlowCreditManager_0_10 creditManager = new WindowCreditManager(0L,0L);

                    FilterManager filterManager = null;
                    try
                    {
                        filterManager = FilterManagerFactory.createManager(method.getArguments());
                    }
                    catch (AMQInvalidArgumentException amqe)
                    {
                        exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, "Exception Creating FilterManager");
                        return;
                    }


                    if(method.hasArguments() && method.getArguments().containsKey(AMQPFilterTypes.REPLAY_PERIOD.toString()))
                    {
                        Object value = method.getArguments().get(AMQPFilterTypes.REPLAY_PERIOD.toString());
                        final long period;
                        if(value instanceof Number)
                        {
                            period = ((Number)value).longValue();
                        }
                        else if(value instanceof String)
                        {
                            try
                            {
                                period = Long.parseLong(value.toString());
                            }
                            catch (NumberFormatException e)
                            {
                                exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, "Cannot parse value " + value + " as a number for filter " + AMQPFilterTypes.REPLAY_PERIOD.toString());
                                return;
                            }
                        }
                        else
                        {
                            exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, "Cannot parse value " + value + " as a number for filter " + AMQPFilterTypes.REPLAY_PERIOD.toString());
                            return;
                        }
                        final long startingFrom = System.currentTimeMillis() - (1000l * period);
                        if(filterManager == null)
                        {
                            filterManager = new FilterManager();
                        }
                        MessageFilter filter = new ArrivalTimeFilter(startingFrom, period == 0);
                        filterManager.add(filter.getName(), filter);

                    }

                    boolean multiQueue = sources.size()>1;
                    ConsumerTarget_0_10 target = new ConsumerTarget_0_10(session, destination,
                                                                         method.getAcceptMode(),
                                                                         method.getAcquireMode(),
                                                                         MessageFlowMode.WINDOW,
                                                                         creditManager,
                                                                         method.getArguments(),
                                                                         multiQueue
                    );

                    Integer priority = null;
                    if(method.hasArguments() && method.getArguments().containsKey("x-priority"))
                    {
                        Object value = method.getArguments().get("x-priority");
                        if(value instanceof Number)
                        {
                            priority = ((Number)value).intValue();
                        }
                        else if(value instanceof String)
                        {
                            try
                            {
                                priority = Integer.parseInt(value.toString());
                            }
                            catch (NumberFormatException e)
                            {
                            }
                        }
                    }

                    session.register(destination, target);
                    try
                    {
                        EnumSet<ConsumerOption> options = EnumSet.noneOf(ConsumerOption.class);
                        if(method.getAcquireMode() == MessageAcquireMode.PRE_ACQUIRED)
                        {
                            options.add(ConsumerOption.ACQUIRES);
                        }
                        if(method.getAcquireMode() != MessageAcquireMode.NOT_ACQUIRED || method.getAcceptMode() == MessageAcceptMode.EXPLICIT)
                        {
                            options.add(ConsumerOption.SEES_REQUEUES);
                        }
                        if(method.getExclusive())
                        {
                            options.add(ConsumerOption.EXCLUSIVE);
                        }
                        for(MessageSource source : sources)
                        {
                            source.addConsumer(target,
                                               filterManager,
                                               MessageTransferMessage.class,
                                               destination,
                                               options,
                                               priority);
                        }
                        target.updateNotifyWorkDesired();
                    }
                    catch (Queue.ExistingExclusiveConsumer existing)
                    {
                        exception(session, method, ExecutionErrorCode.RESOURCE_LOCKED, "Queue has an exclusive consumer");
                    }
                    catch (Queue.ExistingConsumerPreventsExclusive exclusive)
                    {
                        exception(session, method, ExecutionErrorCode.RESOURCE_LOCKED, "Queue has an existing consumer - can't subscribe exclusively");
                    }
                    catch (AccessControlException e)
                    {
                        exception(session, method, ExecutionErrorCode.UNAUTHORIZED_ACCESS, e.getMessage());
                    }
                    catch (MessageSource.ConsumerAccessRefused consumerAccessRefused)
                    {
                        exception(session, method, ExecutionErrorCode.RESOURCE_LOCKED, "Queue has an incompatible exclusivity policy");
                    }
                    catch (MessageSource.QueueDeleted queueDeleted)
                    {
                        exception(session, method, ExecutionErrorCode.NOT_FOUND, "Queue was deleted");
                    }
                }
            }
        }
    }

    protected boolean verifySessionAccess(final ServerSession session, final Collection<MessageSource> queues)
    {
        for(MessageSource source : queues)
        {
            if(!verifySessionAccess(session, source))
            {
                return false;
            }
        }
        return true;
    }

    protected boolean verifySessionAccess(final ServerSession session, final MessageSource queue)
    {
        return queue.verifySessionAccess(session.getModelObject());
    }

    private static String getMessageUserId(MessageTransfer xfr)
    {
        byte[] userIdBytes = xfr.getHeader() == null ? null : xfr.getHeader().getMessageProperties() == null ? null : xfr.getHeader().getMessageProperties().getUserId();
        return userIdBytes == null ? null : new String(userIdBytes, StandardCharsets.UTF_8);
    }

    @Override
    public void messageTransfer(ServerSession ssn, final MessageTransfer xfr)
    {
        try
        {
            if(ssn.blockingTimeoutExceeded())
            {
                getEventLogger(ssn).message(ChannelMessages.FLOW_CONTROL_IGNORED());

                ssn.close(ErrorCodes.MESSAGE_TOO_LARGE,
                          "Session flow control was requested, but not enforced by sender");
            }
            else if(xfr.getBodySize() > ssn.getConnection().getMaxMessageSize())
            {
                exception(ssn, xfr, ExecutionErrorCode.RESOURCE_LIMIT_EXCEEDED,
                          "Message size of " + xfr.getBodySize() + " greater than allowed maximum of " + ssn.getConnection().getMaxMessageSize());
            }
            else
            {
                final MessageDestination destination = getDestinationForMessage(ssn, xfr);

                final DeliveryProperties delvProps =
                        xfr.getHeader() == null ? null : xfr.getHeader().getDeliveryProperties();
                if (delvProps != null && delvProps.hasTtl() && !delvProps.hasExpiration())
                {
                    delvProps.setExpiration(System.currentTimeMillis() + delvProps.getTtl());
                }

                final MessageMetaData_0_10 messageMetaData = new MessageMetaData_0_10(xfr);

                final NamedAddressSpace virtualHost = getAddressSpace(ssn);
                try
                {
                    ssn.getAMQPConnection().checkAuthorizedMessagePrincipal(getMessageUserId(xfr));
                    ssn.authorisePublish(destination, messageMetaData.getRoutingKey(), messageMetaData.isImmediate(), ssn
                            .getAMQPConnection().getLastReadTime());

                }
                catch (AccessControlException e)
                {
                    ExecutionErrorCode errorCode = ExecutionErrorCode.UNAUTHORIZED_ACCESS;
                    exception(ssn, xfr, errorCode, e.getMessage());

                    return;
                }

                final MessageStore store = virtualHost.getMessageStore();
                final StoredMessage<MessageMetaData_0_10> storeMessage = createStoreMessage(xfr, messageMetaData, store);
                final MessageTransferMessage message =
                        new MessageTransferMessage(storeMessage, ssn.getReference());
                MessageReference<MessageTransferMessage> reference = message.newReference();

                try
                {
                    final InstanceProperties instanceProperties = new InstanceProperties()
                    {
                        @Override
                        public Object getProperty(final Property prop)
                        {
                            switch (prop)
                            {
                                case EXPIRATION:
                                    return message.getExpiration();
                                case IMMEDIATE:
                                    return message.isImmediate();
                                case MANDATORY:
                                    return (delvProps == null || !delvProps.getDiscardUnroutable())
                                           && xfr.getAcceptMode() == MessageAcceptMode.EXPLICIT;
                                case PERSISTENT:
                                    return message.isPersistent();
                                case REDELIVERED:
                                    return delvProps.getRedelivered();
                            }
                            return null;
                        }
                    };

                    RoutingResult<MessageTransferMessage> routingResult = ssn.enqueue(message, instanceProperties, destination);

                    boolean explictlyRejected = routingResult.containsReject(RejectType.LIMIT_EXCEEDED);
                    if (!routingResult.hasRoutes() || explictlyRejected)
                    {
                        boolean closeWhenNoRoute = ssn.getAMQPConnection().getPort().getCloseWhenNoRoute();
                        boolean discardUnroutable = delvProps != null && delvProps.getDiscardUnroutable();
                        if (!discardUnroutable && xfr.getAcceptMode() == MessageAcceptMode.EXPLICIT)
                        {
                            RangeSet rejects = RangeSetFactory.createRangeSet();
                            rejects.add(xfr.getId());
                            MessageReject reject = new MessageReject(rejects, MessageRejectCode.UNROUTABLE, "Unroutable");
                            ssn.invoke(reject);
                        }
                        else if (!discardUnroutable && closeWhenNoRoute && explictlyRejected)
                        {
                            ExecutionErrorCode code = ExecutionErrorCode.RESOURCE_LIMIT_EXCEEDED;
                            String errorMessage = String.format("No route for message with destination '%s' and routing key '%s' : %s",
                                                                xfr.getDestination(),
                                                                message.getInitialRoutingAddress(),
                                                                routingResult.getRejectReason());

                            ExecutionException ex = new ExecutionException();
                            ex.setErrorCode(code);
                            ex.setDescription(errorMessage);
                            ssn.invoke(ex);
                            ssn.close(ErrorCodes.RESOURCE_ERROR, errorMessage);
                            return;
                        }
                        else
                        {
                            getEventLogger(ssn).message(ExchangeMessages.DISCARDMSG(destination.getName(),
                                                                                    messageMetaData.getRoutingKey()));
                        }
                    }

                    // TODO: we currently do not send MessageAccept when AcceptMode is EXPLICIT
                    if (ssn.isTransactional())
                    {
                        ssn.processed(xfr);
                    }
                    else
                    {
                        ssn.recordFuture(Futures.immediateFuture(null),
                                         new CommandProcessedAction(ssn, xfr));
                    }
                }
                catch (VirtualHostUnavailableException e)
                {
                    getServerConnection(ssn).sendConnectionCloseAsync(ConnectionCloseCode.CONNECTION_FORCED, e.getMessage());
                }
                finally
                {
                    reference.release();
                }
            }
        }
        finally
        {
            xfr.dispose();
        }
    }

    private StoredMessage<MessageMetaData_0_10> createStoreMessage(final MessageTransfer xfr,
                                                                   final MessageMetaData_0_10 messageMetaData, final MessageStore store)
    {
        final MessageHandle<MessageMetaData_0_10> addedMessage = store.addMessage(messageMetaData);
        QpidByteBuffer body = xfr.getBody();
        if(body != null)
        {
            addedMessage.addContent(body);
        }
        final StoredMessage<MessageMetaData_0_10> storedMessage = addedMessage.allContentAdded();
        return storedMessage;
    }

    @Override
    public void messageCancel(ServerSession session, MessageCancel method)
    {
        String destination = method.getDestination();

        ConsumerTarget_0_10 sub = session.getSubscription(destination);

        if(sub == null)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, "not-found: destination '"+destination+"'");
        }
        else
        {
            session.unregister(sub);
        }
    }

    @Override
    public void messageFlush(ServerSession session, MessageFlush method)
    {
        String destination = method.getDestination();

        ConsumerTarget_0_10 sub = session.getSubscription(destination);

        if(sub == null)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, "not-found: destination '"+destination+"'");
        }
        else
        {
            sub.flush();
        }
    }

    @Override
    public void txSelect(ServerSession session, TxSelect method)
    {
        // TODO - check current tx mode
        session.selectTx();
    }

    @Override
    public void txCommit(ServerSession session, TxCommit method)
    {
        // TODO - check current tx mode
        session.commit();
    }

    @Override
    public void txRollback(ServerSession session, TxRollback method)
    {
        // TODO - check current tx mode
        session.rollback();
    }

    @Override
    public void dtxSelect(ServerSession session, DtxSelect method)
    {
        // TODO - check current tx mode
        session.selectDtx();
    }

    @Override
    public void dtxStart(ServerSession session, DtxStart method)
    {
        XaResult result = new XaResult();
        result.setStatus(DtxXaStatus.XA_OK);
        try
        {
            session.startDtx(method.getXid(), method.getJoin(), method.getResume());
            session.executionResult(method.getId(), result);
        }
        catch(JoinAndResumeDtxException e)
        {
            exception(session, method, ExecutionErrorCode.COMMAND_INVALID, e.getMessage());
        }
        catch(UnknownDtxBranchException e)
        {
            exception(session, method, ExecutionErrorCode.NOT_ALLOWED, "Unknown xid " + method.getXid());
        }
        catch(AlreadyKnownDtxException e)
        {
            exception(session, method, ExecutionErrorCode.NOT_ALLOWED, "Xid already started an neither join nor " +
                                                                       "resume set" + method.getXid());
        }
        catch(DtxNotSelectedException e)
        {
            exception(session, method, ExecutionErrorCode.COMMAND_INVALID, e.getMessage());
        }

    }

    @Override
    public void dtxEnd(ServerSession session, DtxEnd method)
    {
        XaResult result = new XaResult();
        result.setStatus(DtxXaStatus.XA_OK);
        try
        {
            try
            {
                session.endDtx(method.getXid(), method.getFail(), method.getSuspend());
            }
            catch (TimeoutDtxException e)
            {
                result.setStatus(DtxXaStatus.XA_RBTIMEOUT);
            }
            session.executionResult(method.getId(), result);
        }
        catch(UnknownDtxBranchException e)
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_STATE, e.getMessage());
        }
        catch(NotAssociatedDtxException e)
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_STATE, e.getMessage());
        }
        catch(DtxNotSelectedException e)
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_STATE, e.getMessage());
        }
        catch(SuspendAndFailDtxException e)
        {
            exception(session, method, ExecutionErrorCode.COMMAND_INVALID, e.getMessage());
        }

    }

    @Override
    public void dtxCommit(ServerSession session, DtxCommit method)
    {
        XaResult result = new XaResult();
        result.setStatus(DtxXaStatus.XA_OK);
        try
        {
            try
            {
                session.commitDtx(method.getXid(), method.getOnePhase());
            }
            catch (RollbackOnlyDtxException e)
            {
                result.setStatus(DtxXaStatus.XA_RBROLLBACK);
            }
            catch (TimeoutDtxException e)
            {
                result.setStatus(DtxXaStatus.XA_RBTIMEOUT);
            }
            session.executionResult(method.getId(), result);
        }
        catch(UnknownDtxBranchException e)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, e.getMessage());
        }
        catch(IncorrectDtxStateException e)
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_STATE, e.getMessage());
        }
        catch(StoreException e)
        {
            exception(session, method, ExecutionErrorCode.INTERNAL_ERROR, e.getMessage());
            throw e;
        }
    }

    @Override
    public void dtxForget(ServerSession session, DtxForget method)
    {
        try
        {
            session.forgetDtx(method.getXid());
        }
        catch(UnknownDtxBranchException e)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, e.getMessage());
        }
        catch(IncorrectDtxStateException e)
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_STATE, e.getMessage());
        }

    }

    @Override
    public void dtxGetTimeout(ServerSession session, DtxGetTimeout method)
    {
        GetTimeoutResult result = new GetTimeoutResult();
        try
        {
            result.setTimeout(session.getTimeoutDtx(method.getXid()));
            session.executionResult(method.getId(), result);
        }
        catch(UnknownDtxBranchException e)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, e.getMessage());
        }
    }

    @Override
    public void dtxPrepare(ServerSession session, DtxPrepare method)
    {
        XaResult result = new XaResult();
        result.setStatus(DtxXaStatus.XA_OK);
        try
        {
            try
            {
                session.prepareDtx(method.getXid());
            }
            catch (RollbackOnlyDtxException e)
            {
                result.setStatus(DtxXaStatus.XA_RBROLLBACK);
            }
            catch (TimeoutDtxException e)
            {
                result.setStatus(DtxXaStatus.XA_RBTIMEOUT);
            }
            session.executionResult((int) method.getId(), result);
        }
        catch(UnknownDtxBranchException e)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, e.getMessage());
        }
        catch(IncorrectDtxStateException e)
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_STATE, e.getMessage());
        }
        catch(StoreException e)
        {
            exception(session, method, ExecutionErrorCode.INTERNAL_ERROR, e.getMessage());
            throw e;
        }
    }

    @Override
    public void dtxRecover(ServerSession session, DtxRecover method)
    {
        RecoverResult result = new RecoverResult();
        List inDoubt = session.recoverDtx();
        result.setInDoubt(inDoubt);
        session.executionResult(method.getId(), result);
    }

    @Override
    public void dtxRollback(ServerSession session, DtxRollback method)
    {

        XaResult result = new XaResult();
        result.setStatus(DtxXaStatus.XA_OK);
        try
        {
            try
            {
                session.rollbackDtx(method.getXid());
            }
            catch (TimeoutDtxException e)
            {
                result.setStatus(DtxXaStatus.XA_RBTIMEOUT);
            }
            session.executionResult(method.getId(), result);
        }
        catch(UnknownDtxBranchException e)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, e.getMessage());
        }
        catch(IncorrectDtxStateException e)
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_STATE, e.getMessage());
        }
        catch(StoreException e)
        {
            exception(session, method, ExecutionErrorCode.INTERNAL_ERROR, e.getMessage());
            throw e;
        }
    }

    @Override
    public void dtxSetTimeout(ServerSession session, DtxSetTimeout method)
    {
        try
        {
            session.setTimeoutDtx(method.getXid(), method.getTimeout());
        }
        catch(UnknownDtxBranchException e)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, e.getMessage());
        }
    }

    @Override
    public void executionSync(final ServerSession ssn, final ExecutionSync sync)
    {
        ssn.awaitCommandCompletion();
        ssn.syncPoint();
    }


    @Override
    public void exchangeDeclare(ServerSession session, ExchangeDeclare method)
    {
        String exchangeName = method.getExchange();
        NamedAddressSpace addressSpace = getAddressSpace(session);

        String alternateExchangeName = method.getAlternateExchange();
        if(nameNullOrEmpty(method.getExchange()))
        {
            // special case handling to fake the existence of the default exchange for 0-10
            if(!ExchangeDefaults.DIRECT_EXCHANGE_CLASS.equals(method.getType()))
            {
                exception(session, method, ExecutionErrorCode.NOT_ALLOWED,
                          "Attempt to redeclare default exchange "
                          + " of type " + ExchangeDefaults.DIRECT_EXCHANGE_CLASS
                          + " to " + method.getType() +".");
            }
            if(!nameNullOrEmpty(alternateExchangeName))
            {
                exception(session, method, ExecutionErrorCode.NOT_ALLOWED,
                          "Attempt to set alternate exchange of the default exchange "
                          + " to " + alternateExchangeName + ".");
            }
        }
        else
        {
            if(method.getPassive())
            {

                Exchange<?> exchange = getExchange(session, exchangeName, false);

                if(exchange == null)
                {
                    exception(session, method, ExecutionErrorCode.NOT_FOUND, "not-found: exchange-name '" + exchangeName + "'");
                }
                else
                {
                    if (!exchange.getType().equals(method.getType())
                            && (method.getType() != null && method.getType().length() > 0))
                    {
                        exception(session, method, ExecutionErrorCode.NOT_ALLOWED, "Attempt to redeclare exchange: "
                                + exchangeName + " of type " + exchange.getType() + " to " + method.getType() + ".");
                    }
                }
            }
            else
            {

                try
                {
                    Map<String,Object> attributes = new HashMap<String, Object>();
                    if(method.hasArguments())
                    {
                        attributes.putAll(method.getArguments());
                    }
                    attributes.put(org.apache.qpid.server.model.Exchange.NAME, method.getExchange());
                    attributes.put(org.apache.qpid.server.model.Exchange.TYPE, method.getType());
                    attributes.put(org.apache.qpid.server.model.Exchange.DURABLE, method.getDurable());
                    attributes.put(org.apache.qpid.server.model.Exchange.LIFETIME_POLICY,
                                   method.getAutoDelete() ? LifetimePolicy.DELETE_ON_NO_LINKS : LifetimePolicy.PERMANENT);
                    if (method.hasAlternateExchange() && !nameNullOrEmpty(alternateExchangeName))
                    {
                        validateAlternateExchangeIsNotQueue(addressSpace, alternateExchangeName);

                        attributes.put(org.apache.qpid.server.model.Exchange.ALTERNATE_BINDING,
                                       Collections.singletonMap(AlternateBinding.DESTINATION, alternateExchangeName));
                    }
                    validateAndSanitizeExchangeDeclareArguments(attributes, session.getAMQPConnection());
                    addressSpace.createMessageDestination(Exchange.class, attributes);;
                }
                catch(ReservedExchangeNameException e)
                {
                    Exchange<?> existingExchange = getExchange(session, exchangeName, false);
                    if(existingExchange == null
                       || !existingExchange.getType().equals(method.getType())
                       || (method.hasAlternateExchange() && (existingExchange.getAlternateBinding() == null ||
                                                             !alternateExchangeName
                                                                    .equals(existingExchange.getAlternateBinding().getDestination()))) )
                    {
                        exception(session, method, ExecutionErrorCode.NOT_ALLOWED, "Attempt to declare exchange: "
                                                                                   + exchangeName + " which begins with reserved name or prefix.");
                    }
                }
                catch (UnknownAlternateBindingException e)
                {
                    exception(session,
                              method,
                              ExecutionErrorCode.NOT_FOUND,
                              String.format("Unknown alternate destination '%s'", e.getAlternateBindingName()));
                }
                catch(NoFactoryForTypeException e)
                {
                    exception(session, method, ExecutionErrorCode.NOT_FOUND, "Unknown Exchange Type: " + method.getType());
                }
                catch(AbstractConfiguredObject.DuplicateNameException e)
                {
                    Exchange<?> exchange = (Exchange<?>) e.getExisting();
                    if(!exchange.getType().equals(method.getType()))
                    {
                        exception(session, method, ExecutionErrorCode.NOT_ALLOWED,
                                "Attempt to redeclare exchange: " + exchangeName
                                        + " of type " + exchange.getType()
                                        + " to " + method.getType() +".");
                    }
                    else if(method.hasAlternateExchange()
                              && (exchange.getAlternateBinding() == null ||
                                  !alternateExchangeName.equals(exchange.getAlternateBinding().getDestination())))
                    {
                        exception(session, method, ExecutionErrorCode.NOT_ALLOWED,
                                  "Attempt to change alternate exchange of: " + exchangeName
                                  + " from " + exchange.getAlternateBinding()
                                  + " to " + alternateExchangeName + ".");
                    }
                }
                catch (AccessControlException e)
                {
                    exception(session, method, ExecutionErrorCode.UNAUTHORIZED_ACCESS, e.getMessage());
                }
                catch (IllegalArgumentException | IllegalConfigurationException e)
                {
                    exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, e.getMessage());
                }
            }
        }
    }

    private void validateAndSanitizeExchangeDeclareArguments(final Map<String, Object> attributes, final AMQPConnection_0_10 connection)
    {
        final Model model = connection.getModel();
        final ConfiguredObjectTypeRegistry typeRegistry = model.getTypeRegistry();
        final List<ConfiguredObjectAttribute<?, ?>> types = new ArrayList<>(typeRegistry.getAttributeTypes(Exchange.class).values());
        typeRegistry.getTypeSpecialisations(Exchange.class).forEach(type -> types.addAll(typeRegistry.getTypeSpecificAttributes(type)));
        final Set<String> unsupported = attributes.keySet()
                                                  .stream()
                                                  .filter(name -> types.stream().noneMatch(a -> Objects.equals(name, a.getName())
                                                                                                && !a.isDerived()))
                                                  .collect(Collectors.toSet());
        if (!unsupported.isEmpty())
        {
            Exchange.BehaviourOnUnknownDeclareArgument unknownArgumentBehaviour =
                    connection.getContextValue(Exchange.BehaviourOnUnknownDeclareArgument.class,
                                               Exchange.UNKNOWN_EXCHANGE_DECLARE_ARGUMENT_BEHAVIOUR_NAME);
            switch(unknownArgumentBehaviour)
            {
                case LOG:
                    LOGGER.warn("Unsupported exchange declare arguments : {}", String.join(",", unsupported));
                    // fall through
                case IGNORE:
                    attributes.keySet().removeAll(unsupported);
                    break;
                case FAIL:
                default:
                    throw new IllegalArgumentException(String.format(
                            "Unsupported exchange declare arguments : %s", String.join(",", unsupported)));
            }
        }
    }

    private void exception(ServerSession session, Method method, ExecutionErrorCode errorCode, String description)
    {
        ExecutionException ex = new ExecutionException();
        ex.setErrorCode(errorCode);
        ex.setCommandId(method.getId());
        ex.setDescription(description);

        session.invoke(ex);

        session.close(errorCode.getValue(), description);
    }

    private Exchange<?> getExchange(ServerSession session, String exchangeName, boolean mayCreate)
    {
        return getExchange(getAddressSpace(session),exchangeName, mayCreate);
    }

    private Exchange<?> getExchange(NamedAddressSpace addressSpace, String exchangeName, boolean mayCreate)
    {
        MessageDestination destination = addressSpace.getAttainedMessageDestination(exchangeName, mayCreate);
        return destination instanceof Exchange ? (Exchange<?>) destination : null;
    }


    private Queue<?> getQueue(NamedAddressSpace addressSpace, String name)
    {
        MessageSource source = addressSpace.getAttainedMessageSource(name);
        return source instanceof Queue ? (Queue<?>) source : null;
    }

    private MessageDestination getDestinationForMessage(ServerSession ssn, MessageTransfer xfr)
    {
        NamedAddressSpace addressSpace = getAddressSpace(ssn);

        MessageDestination destination;
        if(xfr.hasDestination())
        {
            destination = xfr.getDestination().trim().isEmpty() ? null : addressSpace.getAttainedMessageDestination(xfr.getDestination(), true);
            if(destination == null)
            {
                destination = addressSpace.getDefaultDestination();
            }
            else
            {
                Header header = xfr.getHeader();
                DeliveryProperties delvProps;
                if(header == null)
                {
                    delvProps = new DeliveryProperties();
                    header = new Header(delvProps, null, null);
                    xfr.setHeader(header);
                }
                else if(header.getDeliveryProperties() == null)
                {
                    delvProps = new DeliveryProperties();
                    header = new Header(delvProps, header.getMessageProperties(), header.getNonStandardProperties());
                    xfr.setHeader(header);
                }
                else
                {
                    delvProps = header.getDeliveryProperties();
                }
                if(delvProps.getExchange() == null && !xfr.getDestination().equals(delvProps.getRoutingKey()))
                {
                    delvProps.setExchange(xfr.getDestination());
                }
            }
        }
        else if(xfr.getHeader() != null
                && xfr.getHeader().getDeliveryProperties() != null
                && xfr.getHeader().getDeliveryProperties().getExchange() != null)
        {
            destination = addressSpace.getAttainedMessageDestination(xfr.getHeader().getDeliveryProperties().getExchange(), true);
        }
        else
        {
            destination = addressSpace.getDefaultDestination();
        }
        return destination;
    }

    private NamedAddressSpace getAddressSpace(ServerSession session)
    {
        ServerConnection conn = getServerConnection(session);
        return conn.getAddressSpace();
    }

    private ServerConnection getServerConnection(ServerSession session)
    {
        return (ServerConnection) session.getConnection();
    }

    private <T> T getContextValue(ServerSession session, Class<T> clazz, String name)
    {
        return getServerConnection(session).getAmqpConnection().getContextProvider().getContextValue(clazz, name);
    }

    private EventLogger getEventLogger(ServerSession session)
    {
        return getServerConnection(session).getAmqpConnection().getEventLogger();
    }

    @Override
    public void exchangeDelete(ServerSession session, ExchangeDelete method)
    {
        if (nameNullOrEmpty(method.getExchange()))
        {
            exception(session, method, ExecutionErrorCode.INVALID_ARGUMENT, "Delete not allowed for default exchange");
            return;
        }

        Exchange<?> exchange = getExchange(session, method.getExchange(), false);

        if(exchange == null)
        {
            exception(session, method, ExecutionErrorCode.NOT_FOUND, "No such exchange '" + method.getExchange() + "'");
        }
        else
        {
            if (method.getIfUnused() && exchange.hasBindings())
            {
                exception(session, method, ExecutionErrorCode.PRECONDITION_FAILED, "Exchange has bindings");
            }
            else
            {
                try
                {
                    exchange.delete();
                }
                catch (MessageDestinationIsAlternateException e)
                {
                    exception(session, method, ExecutionErrorCode.NOT_ALLOWED, "Exchange in use as an alternate binding destination");
                }
                catch (RequiredExchangeException e)
                {
                    exception(session, method, ExecutionErrorCode.NOT_ALLOWED, "Exchange '"+method.getExchange()+"' cannot be deleted");
                }
                catch (AccessControlException e)
                {
                    exception(session, method, ExecutionErrorCode.UNAUTHORIZED_ACCESS, e.getMessage());
                }
            }
        }
    }

    private boolean nameNullOrEmpty(String name)
    {
        if(name == null || name.length() == 0)
        {
            return true;
        }

        return false;
    }

    @Override
    public void exchangeQuery(ServerSession session, ExchangeQuery method)
    {

        ExchangeQueryResult result = new ExchangeQueryResult();


        final String exchangeName = method.getName();

        if(nameNullOrEmpty(exchangeName))
        {
            // Fake the existence of the "default" exchange for 0-10
            result.setDurable(true);
            result.setType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
            result.setNotFound(false);
        }
        else
        {
            Exchange<?> exchange = getExchange(session, exchangeName, true);

            if(exchange != null)
            {
                result.setDurable(exchange.isDurable());
                result.setType(exchange.getType());
                result.setNotFound(false);
            }
            else
            {
                result.setNotFound(true);
            }
        }
        session.executionResult((int) method.getId(), result);
    }

    @Override
    public void exchangeBind(ServerSession session, ExchangeBind method)
    {

        NamedAddressSpace addressSpace = getAddressSpace(session);

        if (!method.hasQueue())
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, "queue not set");
        }
        else
        {
            final String exchangeName = method.getExchange();
            if (nameNullOrEmpty(exchangeName))
            {
                exception(session, method, ExecutionErrorCode.INVALID_ARGUMENT, "Bind not allowed for default exchange");
            }
            else
            {
                //TODO - here because of non-compliant python tests
                // should raise exception ILLEGAL_ARGUMENT "binding-key not set"
                if (!method.hasBindingKey())
                {
                    method.setBindingKey(method.getQueue());
                }
                Queue<?> queue = getQueue(addressSpace, method.getQueue());
                Exchange<?> exchange = getExchange(addressSpace, exchangeName, false);
                if(queue == null)
                {
                    exception(session, method, ExecutionErrorCode.NOT_FOUND, "Queue: '" + method.getQueue() + "' not found");
                }
                else if(exchange == null)
                {
                    exception(session, method, ExecutionErrorCode.NOT_FOUND, "Exchange: '" + exchangeName + "' not found");
                }
                else if(exchange.getType().equals(ExchangeDefaults.HEADERS_EXCHANGE_CLASS) && (!method.hasArguments() || method.getArguments() == null || !method.getArguments().containsKey("x-match")))
                {
                    exception(session, method, ExecutionErrorCode.INTERNAL_ERROR, "Bindings to an exchange of type " + ExchangeDefaults.HEADERS_EXCHANGE_CLASS + " require an x-match header");
                }
                else
                {
                    if (!exchange.isBound(method.getBindingKey(), method.getArguments(), queue))
                    {
                        try
                        {
                            exchange.addBinding(method.getBindingKey(), queue, method.getArguments());
                        }
                        catch (AccessControlException e)
                        {
                            exception(session, method, ExecutionErrorCode.UNAUTHORIZED_ACCESS, e.getMessage());
                        }
                        catch (AMQInvalidArgumentException e)
                        {
                            exception(session,
                                      method,
                                      ExecutionErrorCode.INVALID_ARGUMENT,
                                      String.format(
                                              "Cannot bind queue '%s' to exchange '%s' due to invalid argument : %s",
                                              queue.getName(),
                                              exchangeName,
                                              e.getMessage()));
                        }
                    }
                    else
                    {
                        // todo
                    }
                }


            }
        }



    }

    @Override
    public void exchangeUnbind(ServerSession session, ExchangeUnbind method)
    {
        NamedAddressSpace addressSpace = getAddressSpace(session);

        if (!method.hasQueue())
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, "queue not set");
        }
        else if (nameNullOrEmpty(method.getExchange()))
        {
            exception(session, method, ExecutionErrorCode.INVALID_ARGUMENT, "Unbind not allowed for default exchange");
        }
        else if (!method.hasBindingKey())
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, "binding-key not set");
        }
        else
        {
            Queue<?> queue = getQueue(addressSpace, method.getQueue());
            Exchange<?> exchange = getExchange(addressSpace, method.getExchange(), false);
            if(queue == null)
            {
                exception(session, method, ExecutionErrorCode.NOT_FOUND, "Queue: '" + method.getQueue() + "' not found");
            }
            else if(exchange == null)
            {
                exception(session, method, ExecutionErrorCode.NOT_FOUND, "Exchange: '" + method.getExchange() + "' not found");
            }
            else
            {
                try
                {
                    if(exchange.hasBinding(method.getBindingKey(), queue))
                    {
                        exchange.deleteBinding(method.getBindingKey(), queue);
                    }
                }
                catch (AccessControlException e)
                {
                    exception(session, method, ExecutionErrorCode.UNAUTHORIZED_ACCESS, e.getMessage());
                }
            }
        }
    }

    @Override
    public void exchangeBound(ServerSession session, ExchangeBound method)
    {

        ExchangeBoundResult result = new ExchangeBoundResult();
        NamedAddressSpace addressSpace = getAddressSpace(session);
        Exchange<?> exchange;
        MessageSource source;
        Queue<?> queue;
        boolean isDefaultExchange;
        if(!nameNullOrEmpty(method.getExchange()))
        {
            isDefaultExchange = false;
            exchange = getExchange(addressSpace, method.getExchange(), false);

            if(exchange == null)
            {
                result.setExchangeNotFound(true);
            }
        }
        else
        {
            isDefaultExchange = true;
            exchange = null;
        }

        if(isDefaultExchange)
        {
            // fake the existence of the "default" exchange for 0-10
            if(method.hasQueue())
            {
                queue = getQueue(session, method.getQueue());

                if(queue == null)
                {
                    result.setQueueNotFound(true);
                }
                else
                {
                    if(method.hasBindingKey())
                    {
                        if(!method.getBindingKey().equals(method.getQueue()))
                        {
                            result.setKeyNotMatched(true);
                        }
                    }
                }
            }
            else if(method.hasBindingKey())
            {
                if(getQueue(session, method.getBindingKey()) == null)
                {
                    result.setKeyNotMatched(true);
                }
            }

            if(method.hasArguments() && !method.getArguments().isEmpty())
            {
                result.setArgsNotMatched(true);
            }


        }
        else if(method.hasQueue())
        {
            source = getMessageSource(session, method.getQueue());

            if(source == null)
            {
                result.setQueueNotFound(true);
            }
            if(source == null || source instanceof Queue)
            {
                queue = (Queue<?>) source;

                if (exchange != null && queue != null)
                {

                    boolean queueMatched = exchange.isBound(queue);

                    result.setQueueNotMatched(!queueMatched);


                    if (method.hasBindingKey())
                    {

                        if (queueMatched)
                        {
                            final boolean keyMatched = exchange.isBound(method.getBindingKey(), queue);
                            result.setKeyNotMatched(!keyMatched);
                            if (method.hasArguments())
                            {
                                if (keyMatched)
                                {
                                    result.setArgsNotMatched(!exchange.isBound(method.getBindingKey(),
                                                                               method.getArguments(),
                                                                               queue));
                                }
                                else
                                {
                                    result.setArgsNotMatched(!exchange.isBound(method.getArguments(), queue));
                                }
                            }
                        }
                        else
                        {
                            boolean keyMatched = exchange.isBound(method.getBindingKey());
                            result.setKeyNotMatched(!keyMatched);
                            if (method.hasArguments())
                            {
                                if (keyMatched)
                                {
                                    result.setArgsNotMatched(!exchange.isBound(method.getBindingKey(),
                                                                               method.getArguments()));
                                }
                                else
                                {
                                    result.setArgsNotMatched(!exchange.isBound(method.getArguments()));
                                }
                            }
                        }

                    }
                    else if (method.hasArguments())
                    {
                        if (queueMatched)
                        {
                            result.setArgsNotMatched(!exchange.isBound(method.getArguments(), queue));
                        }
                        else
                        {
                            result.setArgsNotMatched(!exchange.isBound(method.getArguments()));
                        }
                    }

                }
                else if (exchange != null && method.hasBindingKey())
                {
                    final boolean keyMatched = exchange.isBound(method.getBindingKey());
                    result.setKeyNotMatched(!keyMatched);

                    if (method.hasArguments())
                    {
                        if (keyMatched)
                        {
                            result.setArgsNotMatched(!exchange.isBound(method.getBindingKey(), method.getArguments()));
                        }
                        else
                        {
                            result.setArgsNotMatched(!exchange.isBound(method.getArguments()));
                        }
                    }


                }
            }

        }
        else if(exchange != null && method.hasBindingKey())
        {
            final boolean keyMatched = exchange.isBound(method.getBindingKey());
            result.setKeyNotMatched(!keyMatched);

            if(method.hasArguments())
            {
                if(keyMatched)
                {
                    result.setArgsNotMatched(!exchange.isBound(method.getBindingKey(), method.getArguments()));
                }
                else
                {
                    result.setArgsNotMatched(!exchange.isBound(method.getArguments()));
                }
            }

        }
        else if(exchange != null && method.hasArguments())
        {
            result.setArgsNotMatched(!exchange.isBound(method.getArguments()));
        }


        session.executionResult((int) method.getId(), result);


    }

    private MessageSource getMessageSource(ServerSession session, String queue)
    {
        return getAddressSpace(session).getAttainedMessageSource(queue);
    }

    private Queue<?> getQueue(ServerSession session, String queue)
    {
        return getQueue(getAddressSpace(session), queue);
    }

    @Override
    public void queueDeclare(ServerSession session, final QueueDeclare method)
    {

        final NamedAddressSpace addressSpace = getAddressSpace(session);

        String queueName = method.getQueue();
        Queue<?> queue;
        //TODO: do we need to check that the queue already exists with exactly the same "configuration"?

        final boolean exclusive = method.getExclusive();
        final boolean autoDelete = method.getAutoDelete();

        if(method.getPassive())
        {
            queue = getQueue(addressSpace, queueName);

            if (queue == null)
            {
                String description = "Queue: " + queueName + " not found on VirtualHost(" + addressSpace + ").";
                ExecutionErrorCode errorCode = ExecutionErrorCode.NOT_FOUND;

                exception(session, method, errorCode, description);

            }
            else if (exclusive)
            {
                if (queue.getExclusive() == ExclusivityPolicy.NONE)
                {
                    String description = "Cannot passively declare queue ('" + queueName + "')"
                                         + " as exclusive as queue with same name is" +
                                         " already declared as non-exclusive";
                    ExecutionErrorCode errorCode = ExecutionErrorCode.RESOURCE_LOCKED;
                    exception(session, method, errorCode, description);

                }
                else if (!verifySessionAccess(session, queue))
                {
                    String description = "Cannot passively declare queue('" + queueName + "'),"
                                         + " as exclusive queue with same name "
                                         + "declared on another session";
                    ExecutionErrorCode errorCode = ExecutionErrorCode.RESOURCE_LOCKED;
                    exception(session, method, errorCode, description);
                }
            }
        }
        else
        {
            try
            {

                Queue.BehaviourOnUnknownDeclareArgument unknownArgumentBehaviour =
                        session.getAMQPConnection().getContextValue(Queue.BehaviourOnUnknownDeclareArgument.class,
                                                   Queue.UNKNOWN_QUEUE_DECLARE_ARGUMENT_BEHAVIOUR_NAME);

                final Map<String, Object> arguments = QueueArgumentsConverter.convertWireArgsToModel(queueName,
                                                                                                     method.getArguments(),
                                                                                                     session.getAMQPConnection()
                                                                                                            .getModel(),
                                                                                                     unknownArgumentBehaviour);
                final String alternateExchangeName = method.getAlternateExchange();
                if (method.hasAlternateExchange() && !nameNullOrEmpty(alternateExchangeName))
                {
                    validateAlternateExchangeIsNotQueue(addressSpace, alternateExchangeName);
                    arguments.put(Queue.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, alternateExchangeName));
                }

                arguments.put(Queue.NAME, queueName);

                if(!arguments.containsKey(Queue.LIFETIME_POLICY))
                {
                    LifetimePolicy lifetime;
                    if(autoDelete)
                    {
                        lifetime = exclusive ? LifetimePolicy.DELETE_ON_SESSION_END
                                : LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS;
                    }
                    else
                    {
                        lifetime = LifetimePolicy.PERMANENT;
                    }
                    arguments.put(Queue.LIFETIME_POLICY, lifetime);
                }

                if(!arguments.containsKey(Queue.EXCLUSIVE))
                {
                    ExclusivityPolicy exclusivityPolicy =
                            exclusive ? ExclusivityPolicy.SESSION : ExclusivityPolicy.NONE;

                    arguments.put(Queue.EXCLUSIVE, exclusivityPolicy);
                }

                arguments.put(Queue.DURABLE, method.getDurable());


                queue = addressSpace.createMessageSource(Queue.class, arguments);

            }
            catch(AbstractConfiguredObject.DuplicateNameException qe)
            {
                queue = (Queue<?>) qe.getExisting();
                if (!verifySessionAccess(session, queue))
                {
                    String description = "Cannot declare queue('" + queueName + "'),"
                                                                           + " as exclusive queue with same name "
                                                                           + "declared on another session";
                    ExecutionErrorCode errorCode = ExecutionErrorCode.RESOURCE_LOCKED;

                    exception(session, method, errorCode, description);
                }
            }
            catch (AccessControlException e)
            {
                exception(session, method, ExecutionErrorCode.UNAUTHORIZED_ACCESS, e.getMessage());
            }
            catch (UnknownAlternateBindingException e)
            {
                exception(session,
                          method,
                          ExecutionErrorCode.NOT_FOUND,
                          String.format("Unknown alternate destination '%s'", e.getAlternateBindingName()));
            }
            catch (IllegalArgumentException | IllegalConfigurationException e)
            {
                exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, e.getMessage());
            }
        }
    }

    private void validateAlternateExchangeIsNotQueue(final NamedAddressSpace addressSpace, final String alternateExchangeName)
    {
        MessageDestination alternateMessageDestination = addressSpace.getAttainedMessageDestination(alternateExchangeName, false);
        if (alternateMessageDestination != null && !(alternateMessageDestination instanceof Exchange))
        {
            throw new IllegalConfigurationException(String.format(
                    "Alternate exchange '%s' is not a destination of type 'exchange'.", alternateExchangeName));
        }
    }

    @Override
    public void queueDelete(ServerSession session, QueueDelete method)
    {
        String queueName = method.getQueue();
        if(queueName == null || queueName.length()==0)
        {
            exception(session, method, ExecutionErrorCode.INVALID_ARGUMENT, "No queue name supplied");

        }
        else
        {
            Queue<?> queue = getQueue(session, queueName);


            if (queue == null)
            {
                exception(session, method, ExecutionErrorCode.NOT_FOUND, "No queue " + queueName + " found");
            }
            else
            {
                if(!verifySessionAccess(session, queue))
                {
                    exception(session,method,ExecutionErrorCode.RESOURCE_LOCKED, "Exclusive Queue: " + queueName + " owned exclusively by another session");
                }
                else if (method.getIfEmpty() && !queue.isEmpty())
                {
                    exception(session, method, ExecutionErrorCode.PRECONDITION_FAILED, "Queue " + queueName + " not empty");
                }
                else if (method.getIfUnused() && !queue.isUnused())
                {
                    // TODO - Error code
                    exception(session, method, ExecutionErrorCode.PRECONDITION_FAILED, "Queue " + queueName + " in use");

                }
                else
                {
                    try
                    {
                        queue.delete();
                    }
                    catch (AccessControlException e)
                    {
                        exception(session, method, ExecutionErrorCode.UNAUTHORIZED_ACCESS, e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public void queuePurge(ServerSession session, QueuePurge method)
    {
        String queueName = method.getQueue();
        if(queueName == null || queueName.length()==0)
        {
            exception(session, method, ExecutionErrorCode.ILLEGAL_ARGUMENT, "No queue name supplied");
        }
        else
        {
            Queue<?> queue = getQueue(session, queueName);

            if (queue == null)
            {
                exception(session, method, ExecutionErrorCode.NOT_FOUND, "No queue " + queueName + " found");
            }
            else
            {
                try
                {
                    queue.clearQueue();
                }
                catch (AccessControlException e)
                {
                    exception(session, method, ExecutionErrorCode.UNAUTHORIZED_ACCESS, e.getMessage());
                }
            }
        }
    }

    @Override
    public void queueQuery(ServerSession session, QueueQuery method)
    {
        QueueQueryResult result = new QueueQueryResult();

        MessageSource source = getMessageSource(session, method.getQueue());

        if(source != null)
        {
            result.setQueue(source.getName());

            if (source instanceof Queue)
            {
                final Queue<?> queue = (Queue<?>) source;
                result.setDurable(queue.isDurable());
                result.setExclusive(queue.isExclusive());
                result.setAutoDelete(queue.getLifetimePolicy() != LifetimePolicy.PERMANENT);
                Map<String, Object> arguments = new LinkedHashMap<>();
                Collection<String> availableAttrs = queue.getAvailableAttributes();
                for(String attrName : availableAttrs)
                {
                    arguments.put(attrName,  queue.getAttribute(attrName));
                }
                result.setArguments(QueueArgumentsConverter.convertModelArgsToWire(arguments));
                result.setMessageCount(queue.getQueueDepthMessages());
                result.setSubscriberCount(queue.getConsumerCount());
            }
            else
            {
                result.setDurable(true);
                result.setExclusive(false);
                result.setAutoDelete(false);
                result.setMessageCount(Integer.MAX_VALUE);
                result.setSubscriberCount(0);
            }
        }

        session.executionResult((int) method.getId(), result);

    }

    @Override
    public void messageSetFlowMode(ServerSession session, MessageSetFlowMode sfm)
    {
        String destination = sfm.getDestination();

        ConsumerTarget_0_10 sub = session.getSubscription(destination);

        if(sub == null)
        {
            exception(session, sfm, ExecutionErrorCode.NOT_FOUND, "not-found: destination '" + destination + "'");
        }
        else if(sub.isFlowModeChangeAllowed())
        {
            sub.setFlowMode(sfm.getFlowMode());
        }
        else
        {
            exception(session, sfm, ExecutionErrorCode.PRECONDITION_FAILED, "destination '" + destination + "' has credit");
        }
    }

    @Override
    public void messageStop(ServerSession session, MessageStop stop)
    {
        String destination = stop.getDestination();

        ConsumerTarget_0_10 sub = session.getSubscription(destination);

        if(sub == null)
        {
            exception(session, stop, ExecutionErrorCode.NOT_FOUND, "not-found: destination '"+destination+"'");
        }
        else
        {
            sub.stop();
        }

    }

    @Override
    public void messageFlow(ServerSession session, MessageFlow flow)
    {
        String destination = flow.getDestination();

        ConsumerTarget_0_10 sub = session.getSubscription(destination);

        if(sub == null)
        {
            exception(session, flow, ExecutionErrorCode.NOT_FOUND, "not-found: destination '"+destination+"'");
        }
        else
        {
            sub.addCredit(flow.getUnit(), flow.getValue());
        }

    }

    public void closed(ServerSession session)
    {
        if (_closed.compareAndSet(false, true))
        {
            session.stopSubscriptions();
            session.onClose();
            session.unregisterSubscriptions();
        }
    }

    public void detached(ServerSession session)
    {
        closed(session);
    }

    @Override
    public void init(ServerSession ssn, ProtocolHeader hdr)
    {
        LOGGER.warn("INIT: [{}] {}", ssn, hdr);
    }

    @Override
    public void control(ServerSession ssn, Method method)
    {
        method.dispatch(ssn, this);
    }

    public void command(ServerSession ssn, Method method, boolean processed)
    {
        ssn.identify(method);
        method.dispatch(ssn, this);
        if (processed)
        {
            ssn.processed(method);
        }
    }

    @Override
    public void error(ServerSession ssn, ProtocolError error)
    {
        LOGGER.warn("ERROR: [{}] {}", ssn, error);
    }

    @Override
    public void handle(ServerSession ssn, Method method)
    {
        LOGGER.warn("UNHANDLED: [{}] {}", ssn, method);
    }

    @Override public void sessionRequestTimeout(ServerSession ssn, SessionRequestTimeout t)
    {
        if (t.getTimeout() == 0)
        {
            ssn.setClose(true);
        }
        ssn.sessionTimeout(0); // Always report back an expiry of 0 until it is implemented
    }

    @Override public void sessionAttached(ServerSession ssn, SessionAttached atc)
    {
        ssn.setState(ServerSession.State.OPEN);
        synchronized (ssn.getStateLock())
        {
            ssn.getStateLock().notifyAll();
        }
    }

    @Override public void sessionTimeout(ServerSession ssn, SessionTimeout t)
    {
        // Setting of expiry is not implemented
    }

    @Override public void sessionCompleted(ServerSession ssn, SessionCompleted cmp)
    {
        RangeSet ranges = cmp.getCommands();
        RangeSet known = null;

        if (ranges != null)
        {
            if(ranges.size() == 1)
            {
                Range range = ranges.getFirst();
                boolean advanced = ssn.complete(range.getLower(), range.getUpper());

                if(advanced && cmp.getTimelyReply())
                {
                    known = range;
                }
            }
            else
            {
                if (cmp.getTimelyReply())
                {
                    known = RangeSetFactory.createRangeSet();
                }
                for (Range range : ranges)
                {
                    boolean advanced = ssn.complete(range.getLower(), range.getUpper());
                    if (advanced && known != null)
                    {
                        known.add(range);
                    }
                }
            }
        }
        else if (cmp.getTimelyReply())
        {
            known = RangeSetFactory.createRangeSet();
        }

        if (known != null)
        {
            ssn.sessionKnownCompleted(known);
        }
    }

    @Override public void sessionKnownCompleted(ServerSession ssn, SessionKnownCompleted kcmp)
    {
        RangeSet kc = kcmp.getCommands();
        if (kc != null)
        {
            ssn.knownComplete(kc);
        }
    }

    @Override public void sessionFlush(ServerSession ssn, SessionFlush flush)
    {
        if (flush.getCompleted())
        {
            ssn.flushProcessed();
        }
        if (flush.getConfirmed())
        {
           ssn.flushProcessed();
        }
        if (flush.getExpected())
        {
            ssn.flushExpected();
        }
    }

    @Override public void sessionCommandPoint(ServerSession ssn, SessionCommandPoint scp)
    {
        ssn.commandPoint(scp.getCommandId());
    }

    @Override public void executionResult(ServerSession ssn, ExecutionResult result)
    {
        ssn.result(result.getCommandId(), result.getValue());
    }

    @Override public void executionException(ServerSession ssn, ExecutionException exc)
    {
        ssn.setException(exc);
        LOGGER.error("session exception", exc);
        ssn.closed();
    }

    private static class CommandProcessedAction implements ServerTransaction.Action
    {
        private final ServerSession _serverSession;
        private final Method _method;

        public CommandProcessedAction(final ServerSession serverSession, final Method xfr)
        {
            _serverSession = serverSession;
            _method = xfr;
        }

        @Override
        public void postCommit()
        {
            _serverSession.processed(_method);
        }

        @Override
        public void onRollback()
        {
        }
    }

}
