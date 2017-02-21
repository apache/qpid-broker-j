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

import static org.apache.qpid.server.logging.subjects.LogSubjectFormat.CHANNEL_FORMAT;
import static org.apache.qpid.server.protocol.v0_10.ServerSession.State.CLOSED;
import static org.apache.qpid.server.protocol.v0_10.ServerSession.State.CLOSING;
import static org.apache.qpid.server.protocol.v0_10.ServerSession.State.DETACHED;
import static org.apache.qpid.server.protocol.v0_10.ServerSession.State.NEW;
import static org.apache.qpid.server.protocol.v0_10.ServerSession.State.OPEN;
import static org.apache.qpid.server.protocol.v0_10.ServerSession.State.RESUMING;
import static org.apache.qpid.server.transport.Option.COMPLETED;
import static org.apache.qpid.server.transport.Option.TIMELY_REPLY;
import static org.apache.qpid.server.util.Serial.ge;
import static org.apache.qpid.server.util.Serial.gt;
import static org.apache.qpid.server.util.Serial.le;
import static org.apache.qpid.server.util.Serial.lt;
import static org.apache.qpid.server.util.Serial.max;
import static org.apache.qpid.server.util.Strings.toUTF8;

import java.nio.ByteBuffer;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.CapacityChecker;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.network.Frame;
import org.apache.qpid.server.txn.AlreadyKnownDtxException;
import org.apache.qpid.server.txn.AsyncAutoCommitTransaction;
import org.apache.qpid.server.txn.DistributedTransaction;
import org.apache.qpid.server.txn.DtxNotSelectedException;
import org.apache.qpid.server.txn.IncorrectDtxStateException;
import org.apache.qpid.server.txn.JoinAndResumeDtxException;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.NotAssociatedDtxException;
import org.apache.qpid.server.txn.RollbackOnlyDtxException;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.txn.SuspendAndFailDtxException;
import org.apache.qpid.server.txn.TimeoutDtxException;
import org.apache.qpid.server.txn.UnknownDtxBranchException;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.transport.*;

public class ServerSession extends SessionInvoker
        implements LogSubject, AsyncAutoCommitTransaction.FutureRecorder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerSession.class);
    public static final int UNLIMITED_CREDIT = 0xFFFFFFFF;

    private static final String NULL_DESTINATION = UUID.randomUUID().toString();
    private static final int PRODUCER_CREDIT_TOPUP_THRESHOLD = 1 << 30;
    private static final int UNFINISHED_COMMAND_QUEUE_THRESHOLD = 500;

    private final Set<Object> _blockingEntities = Collections.synchronizedSet(new HashSet<Object>());

    private final AtomicBoolean _blocking = new AtomicBoolean(false);
    private final AtomicInteger _outstandingCredit = new AtomicInteger(UNLIMITED_CREDIT);
    private final CheckCapacityAction _checkCapacityAction = new CheckCapacityAction();
    private final long timeout = 60000;  // TODO server side close does not require this
    // completed incoming commands
    private final Object processedLock = new Object();
    private final int commandLimit = Integer.getInteger("qpid.session.command_limit", 64 * 1024);
    private final Object commandsLock = new Object();
    private final Object stateLock = new Object();
    private final AtomicBoolean _failoverRequired = new AtomicBoolean(false);
    private Session_0_10 _modelObject;
    private long _blockTime;
    private long _blockingTimeout;
    private boolean _wireBlockingState;
    private ServerConnection connection;
    private Binary name;
    private boolean closing;
    private int channel;
    private ServerSessionDelegate delegate;
    private SessionListener listener = new DefaultSessionListener();
    private boolean incomingInit;
    // incoming command count
    private int commandsIn;
    private RangeSet processed;
    private int maxProcessed;
    private int syncPoint;
    // outgoing command count
    private int commandsOut = 0;
    private Map<Integer,Method> commands = new HashMap<Integer, Method>();
    private int commandBytes = 0;
    private int byteLimit = Integer.getInteger("qpid.session.byte_limit", 1024 * 1024);
    private int maxComplete = commandsOut - 1;
    private State state = NEW;
    private Semaphore credit = new Semaphore(0);
    private Thread resumer = null;
    private boolean transacted = false;
    private SessionDetachCode detachCode;
    private boolean _isNoReplay = false;
    private Map<Integer,ResultFuture<?>> results = new HashMap<Integer,ResultFuture<?>>();
    private org.apache.qpid.server.transport.ExecutionException exception = null;

    public Binary getName()
    {
        return name;
    }

    protected void setClose(boolean close)
    {
        this.closing = close;
    }

    public int getChannel()
    {
        return channel;
    }

    void setChannel(int channel)
    {
        this.channel = channel;
    }

    public SessionListener getSessionListener()
    {
        return listener;
    }

    protected State getState()
    {
        return this.state;
    }

    void addCredit(int value)
    {
        credit.release(value);
    }

    void drainCredit()
    {
        credit.drainPermits();
    }

    private void initReceiver()
    {
        synchronized (processedLock)
        {
            incomingInit = false;
            processed = RangeSetFactory.createRangeSet();
        }
    }

    void attach()
    {
        initReceiver();
        sessionAttach(name.getBytes());
        sessionRequestTimeout(0);//use expiry here only if/when session resume is supported
    }

    void resume()
    {
        _failoverRequired.set(false);

        synchronized (commandsLock)
        {
            attach();

            for (int i = maxComplete + 1; lt(i, commandsOut); i++)
            {
                Method m = getCommand(i);
                if (m == null)
                {
                    m = new ExecutionSync();
                    m.setId(i);
                }
                else if (m instanceof MessageTransfer)
                {
                    MessageTransfer xfr = (MessageTransfer)m;

                    Header header = xfr.getHeader();

                    if (header != null)
                    {
                        if (header.getDeliveryProperties() != null)
                        {
                           header.getDeliveryProperties().setRedelivered(true);
                        }
                        else
                        {
                            DeliveryProperties deliveryProps = new DeliveryProperties();
                            deliveryProps.setRedelivered(true);

                            xfr.setHeader(new Header(deliveryProps, header.getMessageProperties(),
                                                     header.getNonStandardProperties()));
                        }

                    }
                    else
                    {
                        DeliveryProperties deliveryProps = new DeliveryProperties();
                        deliveryProps.setRedelivered(true);
                        xfr.setHeader(new Header(deliveryProps, null, null));
                    }
                }
                sessionCommandPoint(m.getId(), 0);
                send(m);
            }

            sessionCommandPoint(commandsOut, 0);

            sessionFlush(COMPLETED);
            resumer = Thread.currentThread();
            state = RESUMING;

            if(isTransacted())
            {
                txSelect();
            }

            listener.resumed(this);
            resumer = null;
        }
    }

    private Method getCommand(int i)
    {
        return commands.get(i);
    }

    private void setCommand(int commandId, Method command)
    {
        commands.put(commandId, command);
    }

    private Method removeCommand(int id)
    {
        return commands.remove(id);
    }

    final void commandPoint(int id)
    {
        synchronized (processedLock)
        {
            this.commandsIn = id;
            if (!incomingInit)
            {
                incomingInit = true;
                maxProcessed = commandsIn - 1;
                syncPoint = maxProcessed;
            }
        }
    }

    public int getCommandsOut()
    {
        return commandsOut;
    }

    public int getCommandsIn()
    {
        return commandsIn;
    }

    public int nextCommandId()
    {
        return commandsIn++;
    }

    final void identify(Method cmd)
    {
        if (!incomingInit)
        {
            throw new IllegalStateException();
        }

        int id = nextCommandId();
        cmd.setId(id);

        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("identify: ch={}, commandId={}", this.channel, id);
        }

        if ((id & 0xff) == 0)
        {
            flushProcessed(TIMELY_REPLY);
        }
    }

    public void processed(Method command)
    {
        processed(command.getId());
    }

    public void processed(int command)
    {
        processed(command, command);
    }

    public void processed(Range range)
    {

        processed(range.getLower(), range.getUpper());
    }

    public void processed(int lower, int upper)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("{} ch={} processed([{},{}]) {} {}", this, channel, lower, upper, syncPoint, maxProcessed);
        }

        boolean flush;
        synchronized (processedLock)
        {
            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("{} processed: {}", this, processed);
            }

            if (ge(upper, commandsIn))
            {
                throw new IllegalArgumentException
                    ("range exceeds max received command-id: " + Range.newInstance(lower, upper));
            }

            processed.add(lower, upper);

            Range first = processed.getFirst();

            int flower = first.getLower();
            int fupper = first.getUpper();
            int old = maxProcessed;
            if (le(flower, maxProcessed + 1))
            {
                maxProcessed = max(maxProcessed, fupper);
            }
            boolean synced = ge(maxProcessed, syncPoint);
            flush = lt(old, syncPoint) && synced;
            if (synced)
            {
                syncPoint = maxProcessed;
            }
        }
        if (flush)
        {
            flushProcessed();
        }
    }

    void flushExpected()
    {
        RangeSet rs = RangeSetFactory.createRangeSet();
        synchronized (processedLock)
        {
            if (incomingInit)
            {
                rs.add(commandsIn);
            }
        }
        sessionExpected(rs, null);
    }

    public void flushProcessed(Option... options)
    {
        RangeSet copy;
        synchronized (processedLock)
        {
            copy = processed.copy();
        }

        synchronized (commandsLock)
        {
            if (state == DETACHED || state == CLOSING || state == CLOSED)
            {
                return;
            }
            if (copy.size() > 0)
            {
                sessionCompleted(copy, options);
            }
        }
    }

    void knownComplete(RangeSet kc)
    {
        if (kc.size() > 0)
        {
            synchronized (processedLock)
            {
                processed.subtract(kc) ;
            }
        }
    }

    void syncPoint()
    {
        int id = getCommandsIn() - 1;
        LOGGER.debug("{} synced to {}", this, id);
        boolean flush;
        synchronized (processedLock)
        {
            syncPoint = id;
            flush = ge(maxProcessed, syncPoint);
        }
        if (flush)
        {
            flushProcessed();
        }
    }

    protected boolean complete(int lower, int upper)
    {
        //avoid autoboxing
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("{} complete({}, {})", this, lower, upper);
        }
        synchronized (commandsLock)
        {
            int old = maxComplete;
            for (int id = max(maxComplete, lower); le(id, upper); id++)
            {
                Method m = removeCommand(id);
                if (m != null)
                {
                    commandBytes -= m.getBodySize();
                    m.complete();
                }
            }
            if (le(lower, maxComplete + 1))
            {
                maxComplete = max(maxComplete, upper);
            }

            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("{}   commands remaining: {}", this, commandsOut - maxComplete);
            }

            commandsLock.notifyAll();
            return gt(maxComplete, old);
        }
    }

    void received(Method m)
    {
        m.delegate(this, delegate);
    }

    private void send(Method m)
    {
        m.setChannel(channel);
        connection.send(m);

        if (!m.isBatch())
        {
            connection.flush();
        }
    }

    protected boolean isBytesFull()
    {
        return commandBytes >= byteLimit;
    }

    protected boolean isCommandsFull(int id)
    {
        return id - maxComplete >= commandLimit;
    }

    @Override
    public void invoke(Method m)
    {
        invoke(m,(Runnable)null);
    }

    public void invoke(Method m, Runnable postIdSettingAction)
    {
        if (m.getEncodedTrack() == Frame.L4)
        {
            synchronized (commandsLock)
            {
                if (state == DETACHED && m.isUnreliable())
                {
                    Thread current = Thread.currentThread();
                    if (!current.equals(resumer))
                    {
                        return;
                    }
                }

                if (state != OPEN && state != CLOSED && state != CLOSING)
                {
                    Thread current = Thread.currentThread();
                    if (!current.equals(resumer) )
                    {
                        // Should not happen
                        throw new SessionException(String.format("Unexpected state %s", state));
                    }
                }

                switch (state)
                {
                case OPEN:
                    break;
                case RESUMING:
                    Thread current = Thread.currentThread();
                    if (!current.equals(resumer))
                    {
                        throw new SessionException
                            ("timed out waiting for resume to finish");
                    }
                    break;
                case CLOSING:
                case CLOSED:
                    org.apache.qpid.server.transport.ExecutionException exc = getException();
                    if (exc != null)
                    {
                        throw new SessionException(exc);
                    }
                    else
                    {
                        throw new SessionClosedException();
                    }
                default:
                    throw new SessionException
                        (String.format
                         ("timed out waiting for session to become open " +
                          "(state=%s)", state));
                }

                int next;
                next = commandsOut++;
                m.setId(next);
                if(postIdSettingAction != null)
                {
                    postIdSettingAction.run();
                }

                if (isFull(next))
                {
                    // Should not happen
                    throw new SessionException(String.format("Command buffer full next: %d", next));
                }

                if (state == CLOSED)
                {
                    org.apache.qpid.server.transport.ExecutionException exc = getException();
                    if (exc != null)
                    {
                        throw new SessionException(exc);
                    }
                    else
                    {
                        throw new SessionClosedException();
                    }
                }

                if (isFull(next))
                {
                    throw new SessionException("timed out waiting for completion");
                }

                if (next == 0)
                {
                    sessionCommandPoint(0, 0);
                }

                boolean replayTransfer = !_isNoReplay && !closing && !transacted &&
                                         m instanceof MessageTransfer &&
                                         ! m.isUnreliable();

                if ((replayTransfer) || m.hasCompletionListener())
                {
                    setCommand(next, m);
                    commandBytes += m.getBodySize();
                }

                try
                {
                    send(m);
                }
                catch (SenderException e)
                {
                    if (!closing)
                    {
                        // if we are not closing then this will happen
                        // again on resume
                        LOGGER.error("error sending command", e);
                    }
                    else
                    {
                        e.rethrow();
                    }
                }

                // flush every 64K commands to avoid ambiguity on
                // wraparound
                if (shouldIssueFlush(next))
                {
                    try
                    {
                        sessionFlush(COMPLETED);
                    }
                    catch (SenderException e)
                    {
                        if (!closing)
                        {
                            // if expiry is > 0 then this will happen
                            // again on resume
                            LOGGER.error("error sending flush (periodic)", e);
                        }
                        else
                        {
                            e.rethrow();
                        }
                    }
                }
            }
        }
        else
        {
            send(m);
        }
    }

    private void checkFailoverRequired(String message)
    {
        if (_failoverRequired.get())
        {
            throw new SessionException(message);
        }
    }

    protected boolean shouldIssueFlush(int next)
    {
        return (next % 65536) == 0;
    }

    void result(int command, Struct result)
    {
        ResultFuture<?> future;
        synchronized (results)
        {
            future = results.remove(command);
        }

        if (future != null)
        {
            future.set(result);
        }
        else
        {
            LOGGER.warn("Received a response to a command" +
                     " that's no longer valid on the client side." +
                     " [ command id : {} , result : {} ]", command, result);
        }
    }

    void setException(org.apache.qpid.server.transport.ExecutionException exc)
    {
        synchronized (results)
        {
            if (exception != null)
            {
                throw new IllegalStateException(
                        String.format("too many exceptions: %s, %s", exception, exc));
            }
            exception = exc;
        }
    }

    org.apache.qpid.server.transport.ExecutionException getException()
    {
        synchronized (results)
        {
            return exception;
        }
    }

    @Override
    protected <T> Future<T> invoke(Method m, Class<T> klass)
    {
        synchronized (commandsLock)
        {
            int command = commandsOut;
            ResultFuture<T> future = new ResultFuture<T>(klass);
            synchronized (results)
            {
                results.put(command, future);
            }
            invoke(m);
            return future;
        }
    }

    public final void messageTransfer(String destination,
                                      MessageAcceptMode acceptMode,
                                      MessageAcquireMode acquireMode,
                                      Header header,
                                      byte[] body,
                                      Option ... _options) {
        messageTransfer(destination, acceptMode, acquireMode, header,
                        ByteBuffer.wrap(body), _options);
    }

    public final void messageTransfer(String destination,
                                      MessageAcceptMode acceptMode,
                                      MessageAcquireMode acquireMode,
                                      Header header,
                                      String body,
                                      Option ... _options) {
        messageTransfer(destination, acceptMode, acquireMode, header,
                        toUTF8(body), _options);
    }

    public void exception(Throwable t)
    {
        LOGGER.error("caught exception", t);
    }

    public void closed()
    {
        synchronized (commandsLock)
        {
            if (closing || getException() != null)
            {
                state = CLOSED;
            }
            else
            {
                state = DETACHED;
            }

            commandsLock.notifyAll();

            synchronized (results)
            {
                for (ResultFuture<?> result : results.values())
                {
                    synchronized(result)
                    {
                        result.notifyAll();
                    }
                }
            }
            if(state == CLOSED)
            {
                delegate.closed(this);
            }
            else
            {
                delegate.detached(this);
            }
        }

        if(state == CLOSED)
        {
            connection.removeSession(this);
            listener.closed(this);
        }
    }

    public boolean isClosing()
    {
        return state == CLOSED || state == CLOSING;
    }

    @Override
    public String toString()
    {
        return String.format("ssn:%s", name);
    }

    public void setTransacted(boolean b) {
        this.transacted = b;
    }

    public boolean isTransacted(){
        return transacted;
    }

    public void setDetachCode(SessionDetachCode dtc)
    {
        this.detachCode = dtc;
    }

    public SessionDetachCode getDetachCode()
    {
        return this.detachCode;
    }

    public Object getStateLock()
    {
        return stateLock;
    }

    protected void sendSessionAttached(final byte[] name, final Option... options)
    {
        super.sessionAttached(name, options);
    }

    public enum State { NEW, DETACHED, RESUMING, OPEN, CLOSING, CLOSED }

    public interface MessageDispositionChangeListener
    {
        void onAccept();

        void onRelease(boolean setRedelivered);

        void onReject();

        boolean acquire();
    }

    private interface MessageDispositionAction
    {
        void performAction(MessageDispositionChangeListener  listener);
    }

    private final SortedMap<Integer, MessageDispositionChangeListener> _messageDispositionListenerMap =
            new ConcurrentSkipListMap<Integer, MessageDispositionChangeListener>();

    private ServerTransaction _transaction;
    private final AtomicLong _txnStarts = new AtomicLong(0);
    private final AtomicLong _txnCommits = new AtomicLong(0);
    private final AtomicLong _txnRejects = new AtomicLong(0);

    private final AtomicLong _txnCount = new AtomicLong(0);
    private Map<String, ConsumerTarget_0_10> _subscriptions = new ConcurrentHashMap<String, ConsumerTarget_0_10>();

    private final CopyOnWriteArrayList<Consumer<?, ConsumerTarget_0_10>> _consumers = new CopyOnWriteArrayList<>();

    private AtomicReference<LogMessage> _forcedCloseLogMessage = new AtomicReference<LogMessage>();
    private volatile long _uncommittedMessageSize;

    private final List<StoredMessage<MessageMetaData_0_10>> _uncommittedMessages = new ArrayList<>();

    public ServerSession(ServerConnection connection, ServerSessionDelegate delegate, Binary name, long expiry)
    {
        this.connection = connection;
        this.delegate = delegate;
        this.name = name;
        this.closing = false;
        this._isNoReplay = false;
        initReceiver();
        _transaction = new AsyncAutoCommitTransaction(this.getMessageStore(),this);

        ServerConnection serverConnection = (ServerConnection) connection;

        _blockingTimeout = serverConnection.getBroker().getContextValue(Long.class, Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT);
    }

    public Subject getSubject()
    {
        return _modelObject.getSubject();
    }

    public AccessControlContext getAccessControllerContext()
    {
        return _modelObject.getAccessControllerContext();
    }

    protected void setState(final State state)
    {
        if(runningAsSubject())
        {
            setStateSuper(state);

            if (state == State.OPEN)
            {
                getAMQPConnection().getEventLogger().message(ChannelMessages.CREATE());
            }
        }
        else
        {
            runAsSubject(new PrivilegedAction<Void>() {

                @Override
                public Void run()
                {
                    setState(state);
                    return null;
                }
            });

        }
    }

    private void setStateSuper(State state)
    {
        synchronized (commandsLock)
        {
            this.state = state;
            commandsLock.notifyAll();
        }
    }


    private <T> T runAsSubject(final PrivilegedAction<T> privilegedAction)
    {
        return AccessController.doPrivileged(privilegedAction, getAccessControllerContext());
    }

    private boolean runningAsSubject()
    {
        return getAuthorizedSubject().equals(Subject.getSubject(AccessController.getContext()));
    }

    private void invokeBlock()
    {
        invoke(new MessageSetFlowMode("", MessageFlowMode.CREDIT));
        invoke(new MessageStop(""));
    }

    private void invokeUnblock()
    {
        MessageFlow mf = new MessageFlow();
        mf.setUnit(MessageCreditUnit.MESSAGE);
        mf.setDestination("");
        _outstandingCredit.set(Integer.MAX_VALUE);
        mf.setValue(Integer.MAX_VALUE);
        invoke(mf);
    }

    void authorisePublish(final MessageDestination destination,
                          final String routingKey,
                          final boolean immediate,
                          final long currentTime)
    {
        _modelObject.getPublishAuthCache().authorisePublish(destination, routingKey, immediate, currentTime);
    }

    protected boolean isFull(int id)
    {
        return isCommandsFull(id);
    }

    public int enqueue(final MessageTransferMessage message,
                       final InstanceProperties instanceProperties,
                       final MessageDestination exchange)
    {
        if(_outstandingCredit.get() != UNLIMITED_CREDIT
                && _outstandingCredit.decrementAndGet() == (Integer.MAX_VALUE - PRODUCER_CREDIT_TOPUP_THRESHOLD))
        {
            _outstandingCredit.addAndGet(PRODUCER_CREDIT_TOPUP_THRESHOLD);
            invoke(new MessageFlow("",MessageCreditUnit.MESSAGE, PRODUCER_CREDIT_TOPUP_THRESHOLD));
        }
        final RoutingResult<MessageTransferMessage> result =
                exchange.route(message, message.getInitialRoutingAddress(), instanceProperties);
        int enqueues = result.send(_transaction, _checkCapacityAction);
        getAMQPConnection().registerMessageReceived(message.getSize(), message.getArrivalTime());
        incrementOutstandingTxnsIfNecessary();
        incrementUncommittedMessageSize(message.getStoredMessage());
        return enqueues;
    }

    private void resetUncommittedMessages()
    {
        _uncommittedMessageSize = 0l;
        _uncommittedMessages.clear();
    }


    private void incrementUncommittedMessageSize(final StoredMessage<MessageMetaData_0_10> handle)
    {
        if (isTransactional() && !(_transaction instanceof DistributedTransaction))
        {
            _uncommittedMessageSize += handle.getContentSize();
            if (_uncommittedMessageSize > getMaxUncommittedInMemorySize())
            {
                handle.flowToDisk();
                if(!_uncommittedMessages.isEmpty() || _uncommittedMessageSize == handle.getContentSize())
                {
                    getAMQPConnection().getEventLogger()
                                       .message(getLogSubject(), ChannelMessages.LARGE_TRANSACTION_WARN(_uncommittedMessageSize));
                }

                if(!_uncommittedMessages.isEmpty())
                {
                    for (StoredMessage<MessageMetaData_0_10> uncommittedHandle : _uncommittedMessages)
                    {
                        uncommittedHandle.flowToDisk();
                    }
                    _uncommittedMessages.clear();
                }
            }
            else
            {
                _uncommittedMessages.add(handle);
            }
        }
    }

    public void sendMessage(MessageTransfer xfr,
                            Runnable postIdSettingAction)
    {
        getAMQPConnection().registerMessageDelivered(xfr.getBodySize());
        invoke(xfr, postIdSettingAction);
    }


    public void onMessageDispositionChange(MessageTransfer xfr, MessageDispositionChangeListener acceptListener)
    {
        _messageDispositionListenerMap.put(xfr.getId(), acceptListener);
    }

    public void accept(RangeSet ranges)
    {
        dispositionChange(ranges, new MessageDispositionAction()
        {
            @Override
            public void performAction(MessageDispositionChangeListener listener)
            {
                listener.onAccept();
            }
        });
    }


    public void release(RangeSet ranges, final boolean setRedelivered)
    {
        dispositionChange(ranges, new MessageDispositionAction()
                                      {
                                          @Override
                                          public void performAction(MessageDispositionChangeListener listener)
                                          {
                                              listener.onRelease(setRedelivered);
                                          }
                                      });
    }

    public void reject(RangeSet ranges)
    {
        dispositionChange(ranges, new MessageDispositionAction()
                                      {
                                          @Override
                                          public void performAction(MessageDispositionChangeListener listener)
                                          {
                                              listener.onReject();
                                          }
                                      });
    }

    public RangeSet acquire(RangeSet transfers)
    {
        RangeSet acquired = RangeSetFactory.createRangeSet();

        if(!_messageDispositionListenerMap.isEmpty())
        {
            Iterator<Integer> unacceptedMessages = _messageDispositionListenerMap.keySet().iterator();
            Iterator<Range> rangeIter = transfers.iterator();

            if(rangeIter.hasNext())
            {
                Range range = rangeIter.next();

                while(range != null && unacceptedMessages.hasNext())
                {
                    int next = unacceptedMessages.next();
                    while(gt(next, range.getUpper()))
                    {
                        if(rangeIter.hasNext())
                        {
                            range = rangeIter.next();
                        }
                        else
                        {
                            range = null;
                            break;
                        }
                    }
                    if(range != null && range.includes(next))
                    {
                        MessageDispositionChangeListener changeListener = _messageDispositionListenerMap.get(next);
                        if(changeListener != null && changeListener.acquire())
                        {
                            acquired.add(next);
                        }
                    }


                }

            }


        }

        return acquired;
    }

    public void dispositionChange(RangeSet ranges, MessageDispositionAction action)
    {
        if(ranges != null)
        {

            if(ranges.size() == 1)
            {
                Range r = ranges.getFirst();
                for(int i = r.getLower(); i <= r.getUpper(); i++)
                {
                    MessageDispositionChangeListener changeListener = _messageDispositionListenerMap.remove(i);
                    if(changeListener != null)
                    {
                        action.performAction(changeListener);
                    }
                }
            }
            else if(!_messageDispositionListenerMap.isEmpty())
            {
                Iterator<Integer> unacceptedMessages = _messageDispositionListenerMap.keySet().iterator();
                Iterator<Range> rangeIter = ranges.iterator();

                if(rangeIter.hasNext())
                {
                    Range range = rangeIter.next();

                    while(range != null && unacceptedMessages.hasNext())
                    {
                        int next = unacceptedMessages.next();
                        while(gt(next, range.getUpper()))
                        {
                            if(rangeIter.hasNext())
                            {
                                range = rangeIter.next();
                            }
                            else
                            {
                                range = null;
                                break;
                            }
                        }
                        if(range != null && range.includes(next))
                        {
                            MessageDispositionChangeListener changeListener = _messageDispositionListenerMap.remove(next);
                            action.performAction(changeListener);
                        }


                    }

                }
            }
        }
    }

    public void removeDispositionListener(Method method)
    {
        _messageDispositionListenerMap.remove(method.getId());
    }

    public void onClose()
    {
        if(_transaction instanceof LocalTransaction)
        {
            _transaction.rollback();
        }
        else if(_transaction instanceof DistributedTransaction)
        {
            getAddressSpace().getDtxRegistry().endAssociations(_modelObject);
        }

        for(MessageDispositionChangeListener listener : _messageDispositionListenerMap.values())
        {
            listener.onRelease(true);
        }
        _messageDispositionListenerMap.clear();

        for (Action<? super Session_0_10> task : _modelObject.getTaskList())
        {
            task.performAction(_modelObject);
        }

        LogMessage operationalLoggingMessage = _forcedCloseLogMessage.get();
        if (operationalLoggingMessage == null)
        {
            operationalLoggingMessage = ChannelMessages.CLOSE();
        }
        getAMQPConnection().getEventLogger().message(getLogSubject(), operationalLoggingMessage);
    }

    protected void awaitClose()
    {
        // Broker shouldn't block awaiting close - thus do override this method to do nothing
    }

    public void acknowledge(final MessageInstanceConsumer consumer,
                            final ConsumerTarget_0_10 target,
                            final MessageInstance entry)
    {
        if (entry.makeAcquisitionUnstealable(consumer))
        {
            _transaction.dequeue(entry.getEnqueueRecord(),
                                 new ServerTransaction.Action()
                                 {

                                     @Override
                                     public void postCommit()
                                     {
                                         entry.delete();
                                     }

                                     @Override
                                     public void onRollback()
                                     {
                                         // The client has acknowledge the message and therefore have seen it.
                                         // In the event of rollback, the message must be marked as redelivered.
                                         entry.setRedelivered();
                                         entry.release(consumer);
                                     }
                                 });
        }
    }

    Collection<ConsumerTarget_0_10> getSubscriptions()
    {
        return _subscriptions.values();
    }

    public void register(String destination, ConsumerTarget_0_10 sub)
    {
        _subscriptions.put(destination == null ? NULL_DESTINATION : destination, sub);
    }


    public void register(final MessageInstanceConsumer<ConsumerTarget_0_10> messageInstanceConsumer)
    {
        if(messageInstanceConsumer instanceof Consumer<?,?>)
        {
            final Consumer<?,ConsumerTarget_0_10> consumer = (Consumer<?,ConsumerTarget_0_10>) messageInstanceConsumer;
            _consumers.add(consumer);
        }
    }

    public ConsumerTarget_0_10 getSubscription(String destination)
    {
        return _subscriptions.get(destination == null ? NULL_DESTINATION : destination);
    }

    public void unregister(ConsumerTarget_0_10 sub)
    {
        _subscriptions.remove(sub.getName());
        sub.close();

    }

    public boolean isTransactional()
    {
        return _transaction.isTransactional();
    }

    public void selectTx()
    {
        _transaction = new LocalTransaction(this.getMessageStore());
        _txnStarts.incrementAndGet();
    }

    public void selectDtx()
    {
        _transaction = new DistributedTransaction(_modelObject, getAddressSpace().getDtxRegistry());

    }


    public void startDtx(Xid xid, boolean join, boolean resume)
            throws JoinAndResumeDtxException,
                   UnknownDtxBranchException,
                   AlreadyKnownDtxException,
                   DtxNotSelectedException
    {
        DistributedTransaction distributedTransaction = assertDtxTransaction();
        distributedTransaction.start(xid, join, resume);
    }


    public void endDtx(Xid xid, boolean fail, boolean suspend)
            throws NotAssociatedDtxException,
            UnknownDtxBranchException,
            DtxNotSelectedException,
            SuspendAndFailDtxException, TimeoutDtxException
    {
        DistributedTransaction distributedTransaction = assertDtxTransaction();
        distributedTransaction.end(xid, fail, suspend);
    }


    public long getTimeoutDtx(Xid xid)
            throws UnknownDtxBranchException
    {
        return getAddressSpace().getDtxRegistry().getTimeout(xid);
    }


    public void setTimeoutDtx(Xid xid, long timeout)
            throws UnknownDtxBranchException
    {
        getAddressSpace().getDtxRegistry().setTimeout(xid, timeout);
    }


    public void prepareDtx(Xid xid)
            throws UnknownDtxBranchException,
            IncorrectDtxStateException, StoreException, RollbackOnlyDtxException, TimeoutDtxException
    {
        getAddressSpace().getDtxRegistry().prepare(xid);
    }

    public void commitDtx(Xid xid, boolean onePhase)
            throws UnknownDtxBranchException,
            IncorrectDtxStateException, StoreException, RollbackOnlyDtxException, TimeoutDtxException
    {
        getAddressSpace().getDtxRegistry().commit(xid, onePhase);
    }


    public void rollbackDtx(Xid xid)
            throws UnknownDtxBranchException,
            IncorrectDtxStateException, StoreException, TimeoutDtxException
    {
        getAddressSpace().getDtxRegistry().rollback(xid);
    }


    public void forgetDtx(Xid xid) throws UnknownDtxBranchException, IncorrectDtxStateException
    {
        getAddressSpace().getDtxRegistry().forget(xid);
    }

    public List<Xid> recoverDtx()
    {
        return getAddressSpace().getDtxRegistry().recover();
    }

    private DistributedTransaction assertDtxTransaction() throws DtxNotSelectedException
    {
        if(_transaction instanceof DistributedTransaction)
        {
            return (DistributedTransaction) _transaction;
        }
        else
        {
            throw new DtxNotSelectedException();
        }
    }


    public void commit()
    {
        _transaction.commit();

        _txnCommits.incrementAndGet();
        _txnStarts.incrementAndGet();
        decrementOutstandingTxnsIfNecessary();
        resetUncommittedMessages();
    }

    public void rollback()
    {
        _transaction.rollback();

        _txnRejects.incrementAndGet();
        _txnStarts.incrementAndGet();
        decrementOutstandingTxnsIfNecessary();
        resetUncommittedMessages();
    }


    private void incrementOutstandingTxnsIfNecessary()
    {
        if(isTransactional())
        {
            //There can currently only be at most one outstanding transaction
            //due to only having LocalTransaction support. Set value to 1 if 0.
            _txnCount.compareAndSet(0,1);
        }
    }

    private void decrementOutstandingTxnsIfNecessary()
    {
        if(isTransactional())
        {
            //There can currently only be at most one outstanding transaction
            //due to only having LocalTransaction support. Set value to 0 if 1.
            _txnCount.compareAndSet(1,0);
        }
    }

    public long getTxnCommits()
    {
        return _txnCommits.get();
    }

    public long getTxnRejects()
    {
        return _txnRejects.get();
    }

    public int getChannelId()
    {
        return getChannel();
    }

    public long getTxnStart()
    {
        return _txnStarts.get();
    }

    public Principal getAuthorizedPrincipal()
    {
        return getConnection().getAuthorizedPrincipal();
    }

    public Subject getAuthorizedSubject()
    {
        return getSubject();
    }

    public Object getReference()
    {
        return getConnection().getReference();
    }

    public MessageStore getMessageStore()
    {
        return getAddressSpace().getMessageStore();
    }

    public NamedAddressSpace getAddressSpace()
    {
        return getConnection().getAddressSpace();
    }

    public boolean isDurable()
    {
        return false;
    }


    public UUID getId()
    {
        return _modelObject.getId();
    }

    public AMQPConnection_0_10 getAMQPConnection()
    {
        return getConnection().getAmqpConnection();
    }

    public ServerConnection getConnection()
    {
        return (ServerConnection) getConnectionSuper();
    }

    private ServerConnection getConnectionSuper()
    {
        return connection;
    }

    public LogSubject getLogSubject()
    {
        return _modelObject.getLogSubject();
    }

    public void block(Queue<?> queue)
    {
        block(queue, queue.getName());
    }

    public void block()
    {
        block(this, "** All Queues **");
    }


    private void block(final Object queue, final String name)
    {
        synchronized (_blockingEntities)
        {
            if(_blockingEntities.add(queue))
            {

                if(_blocking.compareAndSet(false,true))
                {
                    getAMQPConnection().getEventLogger().message(getLogSubject(), ChannelMessages.FLOW_ENFORCED(name));
                    if(getState() == State.OPEN)
                    {
                        getAMQPConnection().notifyWork(_modelObject);
                    }
                }


            }
        }
    }

    public void unblock(Queue<?> queue)
    {
        unblock((Object)queue);
    }

    public void unblock()
    {
        unblock(this);
    }

    private void unblock(final Object queue)
    {
        if(_blockingEntities.remove(queue) && _blockingEntities.isEmpty())
        {
            if(_blocking.compareAndSet(true,false) && !isClosing())
            {
                getAMQPConnection().getEventLogger().message(getLogSubject(), ChannelMessages.FLOW_REMOVED());
                getAMQPConnection().notifyWork(_modelObject);
            }
        }
    }


    boolean blockingTimeoutExceeded()
    {
        long blockTime = _blockTime;
        boolean b = _wireBlockingState && blockTime != 0 && (System.currentTimeMillis() - blockTime) > _blockingTimeout;
        return b;
    }

    public void updateBlockedStateIfNecesssary()
    {
        boolean desiredBlockingState = _blocking.get();
        if (desiredBlockingState != _wireBlockingState)
        {
            _wireBlockingState = desiredBlockingState;

            if (desiredBlockingState)
            {
                invokeBlock();
            }
            else
            {
                invokeUnblock();
            }
            _blockTime = desiredBlockingState ? System.currentTimeMillis() : 0;
        }

    }

    public Object getConnectionReference()
    {
        return getConnection().getReference();
    }

    @Override
    public String toLogString()
    {
        long connectionId = getConnection() instanceof ServerConnection
                            ? getConnection().getConnectionId()
                            : -1;
        String authorizedPrincipal = (getAuthorizedPrincipal() == null) ? "?" : getAuthorizedPrincipal().getName();

        String remoteAddress = String.valueOf(getConnection().getRemoteSocketAddress());
        return "[" +
               MessageFormat.format(CHANNEL_FORMAT,
                                    connectionId,
                                    authorizedPrincipal,
                                    remoteAddress,
                                    getAddressSpace().getName(),
                                    getChannel())
            + "] ";
    }

    public void close(int cause, String message)
    {
        _forcedCloseLogMessage.compareAndSet(null, ChannelMessages.CLOSE_FORCED(cause, message));
        close();
    }

    public void close()
    {
        // unregister subscriptions in order to prevent sending of new messages
        // to subscriptions with closing session
        unregisterSubscriptions();
        if(_modelObject != null)
        {
            _modelObject.delete();
        }
        closeSuper();
    }

    private void closeSuper()
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Closing [{}] in state [{}]", this, state);
        }
        synchronized (commandsLock)
        {
            switch(state)
            {
                case DETACHED:
                    state = CLOSED;
                    delegate.closed(this);
                    connection.removeSession(this);
                    listener.closed(this);
                    break;
                case CLOSED:
                    break;
                default:
                    state = CLOSING;
                    setClose(true);
                    sessionRequestTimeout(0);
                    sessionDetach(name.getBytes());
                    awaitClose();
            }
        }
    }

    void unregisterSubscriptions()
    {
        final Collection<ConsumerTarget_0_10> subscriptions = getSubscriptions();
        for (ConsumerTarget_0_10 subscription_0_10 : subscriptions)
        {
            unregister(subscription_0_10);
        }
    }

    void stopSubscriptions()
    {
        final Collection<ConsumerTarget_0_10> subscriptions = getSubscriptions();
        for (ConsumerTarget_0_10 subscription_0_10 : subscriptions)
        {
            subscription_0_10.stop();
        }
    }


    public void receivedComplete()
    {
        final Collection<ConsumerTarget_0_10> subscriptions = getSubscriptions();
        for (ConsumerTarget_0_10 subscription_0_10 : subscriptions)
        {
            subscription_0_10.flushCreditState(false);
        }
        awaitCommandCompletion();
    }

    public int getUnacknowledgedMessageCount()
    {
        return _messageDispositionListenerMap.size();
    }

    public boolean getBlocking()
    {
        return _blocking.get();
    }

    private final LinkedList<AsyncCommand> _unfinishedCommandsQueue = new LinkedList<AsyncCommand>();

    public void completeAsyncCommands()
    {
        AsyncCommand cmd;
        while((cmd = _unfinishedCommandsQueue.peek()) != null && cmd.isReadyForCompletion())
        {
            cmd.complete();
            _unfinishedCommandsQueue.poll();
        }
        while(_unfinishedCommandsQueue.size() > UNFINISHED_COMMAND_QUEUE_THRESHOLD)
        {
            cmd = _unfinishedCommandsQueue.poll();
            cmd.complete();
        }
    }


    public void awaitCommandCompletion()
    {
        AsyncCommand cmd;
        while((cmd = _unfinishedCommandsQueue.poll()) != null)
        {
            cmd.complete();
        }
    }


    public Object getAsyncCommandMark()
    {
        return _unfinishedCommandsQueue.isEmpty() ? null : _unfinishedCommandsQueue.getLast();
    }

    @Override
    public void recordFuture(final ListenableFuture<Void> future, final ServerTransaction.Action action)
    {
        _unfinishedCommandsQueue.add(new AsyncCommand(future, action));
    }

    private static class AsyncCommand
    {
        private final ListenableFuture<Void> _future;
        private ServerTransaction.Action _action;

        public AsyncCommand(final ListenableFuture<Void> future, final ServerTransaction.Action action)
        {
            _future = future;
            _action = action;
        }

        void complete()
        {
            boolean interrupted = false;
            try
            {
                while (true)
                {
                    try
                    {
                        _future.get();
                        break;
                    }
                    catch (InterruptedException e)
                    {
                        interrupted = true;
                    }

                }
            }
            catch(ExecutionException e)
            {
                if(e.getCause() instanceof RuntimeException)
                {
                    throw (RuntimeException)e.getCause();
                }
                else if(e.getCause() instanceof Error)
                {
                    throw (Error) e.getCause();
                }
                else
                {
                    throw new ServerScopedRuntimeException(e.getCause());
                }
            }
            if(interrupted)
            {
                Thread.currentThread().interrupt();
            }
            _action.postCommit();
            _action = null;
        }

        boolean isReadyForCompletion()
        {
            return _future.isDone();
        }
    }

    public long getConsumerCount()
    {
        return _subscriptions.values().size();
    }

    public Collection<Consumer<?, ConsumerTarget_0_10>> getConsumers()
    {

        return Collections.unmodifiableCollection(_consumers);
    }

    public void setModelObject(final Session_0_10 session)
    {
        _modelObject = session;
    }

    public Session_0_10 getModelObject()
    {
        return _modelObject;
    }

    public long getTransactionStartTimeLong()
    {
        ServerTransaction serverTransaction = _transaction;
        if (serverTransaction.isTransactional())
        {
            return serverTransaction.getTransactionStartTime();
        }
        else
        {
            return 0L;
        }
    }

    public long getTransactionUpdateTimeLong()
    {
        ServerTransaction serverTransaction = _transaction;
        if (serverTransaction.isTransactional())
        {
            return serverTransaction.getTransactionUpdateTime();
        }
        else
        {
            return 0L;
        }
    }

    public void doTimeoutAction(final String reason)
    {
        getAMQPConnection().closeSessionAsync(_modelObject,
                                              AMQPConnection.CloseReason.TRANSACTION_TIMEOUT, reason);
    }

    public final long getMaxUncommittedInMemorySize()
    {
        return _modelObject.getMaxUncommittedInMemorySize();
    }

    static class DefaultSessionListener implements SessionListener
    {

        @Override
        public void opened(ServerSession ssn) {}

        @Override
        public void resumed(ServerSession ssn) {}

        @Override
        public void message(ServerSession ssn, MessageTransfer xfr)
        {
            LOGGER.info("message: {}", xfr);
        }

        @Override
        public void exception(ServerSession ssn, SessionException exc)
        {
            LOGGER.error("session exception", exc);
        }

        @Override
        public void closed(ServerSession ssn) {}
    }

    private class CheckCapacityAction implements Action<MessageInstance>
    {
        @Override
        public void performAction(final MessageInstance entry)
        {
            TransactionLogResource queue = entry.getOwningResource();
            if(queue instanceof CapacityChecker)
            {
                ((CapacityChecker)queue).checkCapacity(_modelObject);
            }
        }
    }

    private class ResultFuture<T> implements Future<T>
    {

        private final Class<T> klass;
        private T result;

        private ResultFuture(Class<T> klass)
        {
            this.klass = klass;
        }

        private void set(Struct result)
        {
            synchronized (this)
            {
                this.result = klass.cast(result);
                notifyAll();
            }
        }

        public T get(long timeout)
        {
            synchronized (this)
            {
                Waiter w = new Waiter(this, timeout);
                while (w.hasTime() && state != CLOSED && !isDone())
                {
                    checkFailoverRequired("Operation was interrupted by failover.");
                    LOGGER.debug("{} waiting for result: {}", ServerSession.this, this);
                    w.await();
                }
            }

            if (isDone())
            {
                return result;
            }
            else if (state == CLOSED)
            {
                org.apache.qpid.server.transport.ExecutionException ex = getException();
                if(ex == null)
                {
                    throw new SessionClosedException();
                }
                throw new SessionException(ex);
            }
            else
            {
                throw new SessionException(
                        String.format("%s timed out waiting for result: %s",
                                      ServerSession.this, this));
            }
        }

        public boolean isDone()
        {
            return result != null;
        }

        @Override
        public String toString()
        {
            return String.format("Future(%s)", isDone() ? result : klass);
        }

    }
}
