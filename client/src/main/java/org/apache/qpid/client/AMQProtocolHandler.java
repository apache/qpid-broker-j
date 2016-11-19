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
package org.apache.qpid.client;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.AMQConnectionClosedException;
import org.apache.qpid.AMQDisconnectedException;
import org.apache.qpid.AMQException;
import org.apache.qpid.AMQTimeoutException;
import org.apache.qpid.QpidException;
import org.apache.qpid.client.failover.FailoverException;
import org.apache.qpid.client.failover.FailoverState;
import org.apache.qpid.client.protocol.AMQProtocolSession;
import org.apache.qpid.client.protocol.BlockingMethodFrameListener;
import org.apache.qpid.client.state.AMQState;
import org.apache.qpid.client.state.AMQStateManager;
import org.apache.qpid.client.state.StateWaiter;
import org.apache.qpid.client.state.listener.SpecificMethodFrameListener;
import org.apache.qpid.codec.ClientDecoder;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.framing.AMQBody;
import org.apache.qpid.framing.AMQDataBlock;
import org.apache.qpid.framing.AMQFrame;
import org.apache.qpid.framing.AMQMethodBody;
import org.apache.qpid.framing.AMQProtocolHeaderException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.ConnectionCloseBody;
import org.apache.qpid.framing.ConnectionCloseOkBody;
import org.apache.qpid.framing.HeartbeatBody;
import org.apache.qpid.framing.MethodRegistry;
import org.apache.qpid.framing.ProtocolInitiation;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.protocol.ErrorCodes;
import org.apache.qpid.protocol.AMQMethodEvent;
import org.apache.qpid.protocol.AMQMethodListener;
import org.apache.qpid.thread.Threading;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.ConnectionSettings;
import org.apache.qpid.transport.ExceptionHandlingByteBufferReceiver;
import org.apache.qpid.transport.TransportException;
import org.apache.qpid.transport.network.NetworkConnection;
import org.apache.qpid.transport.network.TransportActivity;

public class AMQProtocolHandler implements ExceptionHandlingByteBufferReceiver, TransportActivity
{
    /** Used for debugging. */
    private static final Logger _logger = LoggerFactory.getLogger(AMQProtocolHandler.class);

    private static final long MAXIMUM_STATE_WAIT_TIME = Long.parseLong(System.getProperty("amqj.MaximumStateWait", "30000"));
    private static final String AMQJ_DEFAULT_SYNCWRITE_TIMEOUT = "amqj.default_syncwrite_timeout";

    /**
     * The connection that this protocol handler is associated with. There is a 1-1 mapping between connection
     * instances and protocol handler instances.
     */
    private final AMQConnection _connection;

    /** Our wrapper for a protocol session that provides access to session values in a typesafe manner. */
    private final AMQProtocolSession _protocolSession;

    /** Holds the state of the protocol session. */
    private AMQStateManager _stateManager;

    /** Holds the method listeners, */
    private final CopyOnWriteArraySet<AMQMethodListener> _frameListeners = new CopyOnWriteArraySet<AMQMethodListener>();

    /**
     * We create the failover handler when the session is created since it needs a reference to the IoSession in order
     * to be able to send errors during failover back to the client application. The session won't be available in the
     * case where we failing over due to a Connection.Redirect message from the broker.
     */
    private final FailoverHandler _failoverHandler;

    /**
     * This flag is used to track whether failover is being attempted. It is used to prevent the application constantly
     * attempting failover where it is failing.
     */
    private FailoverState _failoverState = FailoverState.NOT_STARTED;

    /** Used to provide a condition to wait upon for operations that are required to wait for failover to complete. */
    private CountDownLatch _failoverLatch;

    /** Object to lock on when changing the _failoverLatch  */
    private final Object _failoverLatchChange = new Object();

    /** The last failover exception that occurred */
    private FailoverException _lastFailoverException;

    /** Defines the default timeout to use for synchronous protocol commands. */
    private final long DEFAULT_SYNC_TIMEOUT = Long.getLong(ClientProperties.QPID_SYNC_OP_TIMEOUT,
                                                           Long.getLong(AMQJ_DEFAULT_SYNCWRITE_TIMEOUT,
                                                                        ClientProperties.DEFAULT_SYNC_OPERATION_TIMEOUT));
    private ClientDecoder _decoder;

    private ProtocolVersion _suggestedProtocolVersion;

    private long _writtenBytes;
    private long _readBytes;

    private int _messageReceivedCount;
    private int _messagesOut;


    private NetworkConnection _network;
    private ByteBufferSender _sender;
    private long _lastReadTime = System.currentTimeMillis();
    private long _lastWriteTime = System.currentTimeMillis();
    private HeartbeatListener _heartbeatListener = HeartbeatListener.DEFAULT;
    private Throwable _initialConnectionException;

    private static final int REUSABLE_BYTE_BUFFER_CAPACITY = 65 * 1024;
    private final byte[] _reusableBytes = new byte[REUSABLE_BYTE_BUFFER_CAPACITY];

    private int _queueId = 1;
    private final Object _queueIdLock = new Object();

    /**
     * Creates a new protocol handler, associated with the specified client connection instance.
     *
     * @param con The client connection that this is the event handler for.
     */
    public AMQProtocolHandler(AMQConnection con)
    {
        _connection = con;
        _protocolSession = new AMQProtocolSession(this, _connection);
        _stateManager = new AMQStateManager(_protocolSession);
        _failoverHandler = new FailoverHandler(this);
    }

    /**
     * Called when the network connection is closed. This can happen, either because the client explicitly requested
     * that the connection be closed, in which case nothing is done, or because the connection died. In the case
     * where the connection died, an attempt to failover automatically to a new connection may be started. The failover
     * process will be started, provided that it is the clients policy to allow failover, and provided that a failover
     * has not already been started or failed.
     * <p>
     * TODO  Clarify: presumably exceptionCaught is called when the client is sending during a connection failure and
     * not otherwise? The above comment doesn't make that clear.
     */
    public void closed()
    {
        if (_connection.isClosed())
        {
            _logger.debug("Session closed called by client");
        }
        else
        {
            // Use local variable to keep flag whether fail-over allowed or not,
            // in order to execute AMQConnection#exceptionRecievedout out of synchronization block,
            // otherwise it might deadlock with failover mutex
            boolean failoverNotAllowed = false;
            boolean failedWithoutConnecting = false;
            Throwable initialConnectionException = null;
            synchronized (this)
            {
                if (_logger.isDebugEnabled())
                {
                    _logger.debug("Session closed called with failover state " + _failoverState);
                }

                // reconnetablility was introduced here so as not to disturb the client as they have made their intentions
                // known through the policy settings.
                if (_failoverState == FailoverState.NOT_STARTED)
                {
                    // close the sender
                    try
                    {
                        _sender.close();
                    }
                    catch (Exception e)
                    {
                        _logger.warn("Exception occurred on closing the sender", e);
                    }
                    if (_connection.failoverAllowed())
                    {
                        _failoverState = FailoverState.IN_PROGRESS;

                        _logger.debug("FAILOVER STARTING");
                        startFailoverThread();
                    }
                    else if (_connection.isConnected())
                    {
                        failoverNotAllowed = true;
                        if (_logger.isDebugEnabled())
                        {
                            _logger.debug("Failover not allowed by policy:" + _connection.getFailoverPolicy());
                        }
                    }
                    else
                    {
                        failedWithoutConnecting = true;
                        initialConnectionException = _initialConnectionException;
                        _logger.debug("We are in process of establishing the initial connection");
                    }
                    _initialConnectionException = null;
                }
                else
                {
                    _logger.debug("Not starting the failover thread as state currently " + _failoverState);
                }
            }

            if (failoverNotAllowed)
            {
                _connection.closed(new AMQDisconnectedException(
                        "Server closed connection and reconnection not permitted.", _stateManager.getLastException()));
            }
            else if(failedWithoutConnecting)
            {
                if(initialConnectionException == null)
                {
                    initialConnectionException = _stateManager.getLastException();
                }
                String message = initialConnectionException == null ? "" : initialConnectionException.getMessage();
                _connection.exceptionReceived(new QpidException(
                        "Connection could not be established: " + message, initialConnectionException));
            }
        }

        if (_logger.isDebugEnabled())
        {
            _logger.debug("Protocol Session [" + this + "] closed");
        }
    }

    /** See {@link FailoverHandler} to see rationale for separate thread. */
    private void startFailoverThread()
    {
        if(!_connection.isClosed())
        {
            final Thread failoverThread;
            try
            {
                failoverThread = Threading.getThreadFactory().createThread(
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {

                                if (Thread.currentThread().isDaemon())
                                {
                                    throw new IllegalStateException("FailoverHandler must run on a non-daemon thread.");
                                }

                                // Create a latch, upon which tasks that must not run in parallel with a failover can wait for completion of
                                // the fail over.
                                setFailoverLatch(new CountDownLatch(1));

                                // We wake up listeners. If they can handle failover, they will extend the
                                // FailoverRetrySupport class and will in turn block on the latch until failover
                                // has completed before retrying the operation.
                                notifyFailoverStarting();

                                getConnection().doWithAllLocks(_failoverHandler);

                                getFailoverLatch().countDown();
                            }
                        });
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to create thread", e);
            }
            failoverThread.setName("Failover");
            // Do not inherit daemon-ness from current thread as this can be a daemon
            // thread such as a AnonymousIoService thread.
            failoverThread.setDaemon(false);
            failoverThread.start();
        }
    }

    public void readerIdle()
    {
        _logger.debug("Protocol Session [" + this + "] idle: reader");
        //  failover:
        _logger.warn("Timed out while waiting for heartbeat from peer.");
        _network.close();
    }

    public void writerIdle()
    {
        _logger.debug("Protocol Session [" + this + "] idle: writer");
        writeFrame(HeartbeatBody.FRAME);
        _heartbeatListener.heartbeatSent();
    }

    /**
     * Invoked when any exception is thrown by the NetworkDriver
     */
    public void exception(Throwable cause)
    {
        boolean causeIsAConnectionProblem =
                cause instanceof AMQConnectionClosedException ||
                cause instanceof IOException ||
                cause instanceof TransportException;

        if (causeIsAConnectionProblem)
        {
            //ensure the IoSender and IoReceiver are closed
            try
            {
                _network.close();
            }
            catch (Exception e)
            {
                //ignore
            }
        }
        FailoverState state = getFailoverState();
        if (state == FailoverState.NOT_STARTED)
        {
            if (causeIsAConnectionProblem)
            {
                _logger.info("Connection exception caught therefore going to attempt failover: " + cause, cause);
                _initialConnectionException = cause;
            }
            else
            {
                _connection.exceptionReceived(cause);
            }

            // FIXME Need to correctly handle other exceptions. Things like ...
            // AMQChannelClosedException
            // which will cause the JMSSession to end due to a channel close and so that Session needs
            // to be removed from the map so we can correctly still call close without an exception when trying to close
            // the server closed session.  See also CloseChannelMethodHandler as the sessionClose is never called on exception
        }
        // we reach this point if failover was attempted and failed therefore we need to let the calling app
        // know since we cannot recover the situation
        else if (state == FailoverState.FAILED)
        {
            _logger.error("Exception caught by protocol handler: " + cause, cause);

            // we notify the state manager of the error in case we have any clients waiting on a state
            // change. Those "waiters" will be interrupted and can handle the exception
            AMQDisconnectedException amqe = new AMQDisconnectedException("Failover could not re-establish connectivity: " + cause, cause);
            propagateExceptionToAllWaiters(amqe);
            _connection.closed(amqe);
        }
        else
        {
            _logger.warn("Exception caught by protocol handler: " + cause, cause);
        }
    }

    /**
     * There are two cases where we have other threads potentially blocking for events to be handled by this class.
     * These are for the state manager (waiting for a state change) or a frame listener (waiting for a particular type
     * of frame to arrive). When an error occurs we need to notify these waiters so that they can react appropriately.
     *
     * This should be called only when the exception is fatal for the connection.
     *
     * @param e the exception to propagate
     *
     * @see #propagateExceptionToFrameListeners
     */
    public void propagateExceptionToAllWaiters(Exception e)
    {
        getStateManager().error(e);

        propagateExceptionToFrameListeners(e);
    }

    /**
     * This caters for the case where we only need to propagate an exception to the the frame listeners to interupt any
     * protocol level waits.
     *
     * This will would normally be used to notify all Frame Listeners that Failover is about to occur and they should
     * stop waiting and relinquish the Failover lock. See {@link FailoverHandler}.
     *
     * Once the {@link FailoverHandler} has re-established the connection then the listeners will be able to re-attempt
     * their protocol request and so listen again for the correct frame.
     *
     * @param e the exception to propagate
     */
    public void propagateExceptionToFrameListeners(Exception e)
    {
        synchronized (_frameListeners)
        {
            if (!_frameListeners.isEmpty())
            {
                final Iterator it = _frameListeners.iterator();
                while (it.hasNext())
                {
                    final AMQMethodListener ml = (AMQMethodListener) it.next();
                    ml.error(e);
                }
            }
        }
    }

    public void notifyFailoverStarting()
    {
        // Set the last exception in the sync block to ensure the ordering with add.
        // either this gets done and the add does the ml.error
        // or the add completes first and the iterator below will do ml.error
        synchronized (_frameListeners)
        {
            _lastFailoverException = new FailoverException("Failing over about to start");
        }

        //Only notify the Frame listeners that failover is going to occur as the State listeners shouldn't be
        // interrupted unless failover cannot restore the state.
        propagateExceptionToFrameListeners(_lastFailoverException);
    }

    public void failoverInProgress()
    {
        _lastFailoverException = null;
    }

    public void received(ByteBuffer msg)
    {
        _readBytes += msg.remaining();
        _lastReadTime = System.currentTimeMillis();
        final List<AMQDataBlock> dataBlocks = _protocolSession.getMethodProcessor().getProcessedMethods();
        try
        {
            _decoder.decodeBuffer(msg);

            // Decode buffer
            int size = dataBlocks.size();
            for (int i = 0; i < size; i++)
            {
                AMQDataBlock message = dataBlocks.get(i);
                _logger.debug("RECV: {}", message);

                if(message instanceof AMQFrame)
                {

                    final long msgNumber = ++_messageReceivedCount;

                    if (((msgNumber % 1000) == 0) && _logger.isDebugEnabled())
                    {
                        _logger.debug("Received {} protocol messages", _messageReceivedCount);
                    }

                    AMQFrame frame = (AMQFrame) message;

                    final AMQBody bodyFrame = frame.getBodyFrame();

                    bodyFrame.handle(frame.getChannel(), _protocolSession);

                    _connection.bytesReceived(_readBytes);
                }
                else if (message instanceof ProtocolInitiation)
                {
                    // We get here if the server sends a response to our initial protocol header
                    // suggesting an alternate ProtocolVersion; the server will then close the
                    // connection.
                    try
                    {
                        ProtocolInitiation protocolInit = (ProtocolInitiation) message;
                        _suggestedProtocolVersion = protocolInit.checkVersion();
                        _logger.debug("Broker suggested using protocol version: {} ", _suggestedProtocolVersion);

                        // get round a bug in old versions of qpid whereby the connection is not closed
                        _stateManager.changeState(AMQState.CONNECTION_CLOSED);

                    }
                    catch (AMQProtocolHeaderException e)
                    {
                        _stateManager.error(e);
                        throw e;
                    }
                }
            }
        }
        catch (Exception e)
        {
            _logger.error("Exception processing frame", e);
            propagateExceptionToFrameListeners(e);
            exception(e);
        }
        finally
        {
            dataBlocks.clear();
        }


    }

    public void methodBodyReceived(final int channelId, final AMQBody bodyFrame)
            throws QpidException
    {
        final AMQMethodEvent<AMQMethodBody> evt =
                new AMQMethodEvent<AMQMethodBody>(channelId, (AMQMethodBody) bodyFrame);

        try
        {

            boolean wasAnyoneInterested = getStateManager().methodReceived(evt);
            synchronized (_frameListeners)
            {
                if (!_frameListeners.isEmpty())
                {
                    //This iterator is safe from the error state as the frame listeners always add before they send so their
                    // will be ready and waiting for this response.
                    Iterator it = _frameListeners.iterator();
                    while (it.hasNext())
                    {
                        final AMQMethodListener listener = (AMQMethodListener) it.next();
                        wasAnyoneInterested = listener.methodReceived(evt) || wasAnyoneInterested;
                    }
                }
            }
            if (!wasAnyoneInterested)
            {
                throw new QpidException("AMQMethodEvent " + evt + " was not processed by any listener.  Listeners:"
                                             + _frameListeners, null);
            }
        }
        catch (QpidException e)
        {
            propagateExceptionToFrameListeners(e);

            exception(e);
        }

    }

    public StateWaiter createWaiter(Set<AMQState> states) throws QpidException
    {
        return getStateManager().createWaiter(states);
    }

    public void writeFrame(AMQDataBlock frame)
    {
        writeFrame(frame, true);
    }

    public  synchronized void writeFrame(AMQDataBlock frame, boolean flush)
    {
        _lastWriteTime = System.currentTimeMillis();
        _writtenBytes += frame.getSize();
        frame.writePayload(_sender);
        if(flush)
        {
            _sender.flush();
        }

        _logger.debug("SEND: {}", frame);

        final long sentMessages = _messagesOut++;

        final boolean debug = _logger.isDebugEnabled();

        if (debug && ((sentMessages % 1000) == 0))
        {
            _logger.debug("Sent {} protocol messages", _messagesOut);
        }

        _connection.bytesSent(_writtenBytes);

    }


    /**
     * Convenience method that writes a frame to the protocol session and waits for a particular response. Equivalent to
     * calling getProtocolSession().write() then waiting for the response.
     *
     * @param frame
     * @param listener the blocking listener. Note the calling thread will block.
     */
    public AMQMethodEvent writeCommandFrameAndWaitForReply(AMQDataBlock frame, BlockingMethodFrameListener listener)
            throws QpidException, FailoverException
    {
        return writeCommandFrameAndWaitForReply(frame, listener, DEFAULT_SYNC_TIMEOUT);
    }

    /**
     * Convenience method that writes a frame to the protocol session and waits for a particular response. Equivalent to
     * calling getProtocolSession().write() then waiting for the response.
     *
     * @param frame
     * @param listener the blocking listener. Note the calling thread will block.
     */
    public AMQMethodEvent writeCommandFrameAndWaitForReply(AMQDataBlock frame, BlockingMethodFrameListener listener,
                                                           long timeout) throws QpidException, FailoverException
    {
        try
        {
            synchronized (_frameListeners)
            {
                if (_lastFailoverException != null)
                {
                    throw _lastFailoverException;
                }

                if(_stateManager.getCurrentState() == AMQState.CONNECTION_CLOSED ||
                        _stateManager.getCurrentState() == AMQState.CONNECTION_CLOSING)
                {
                    Exception e = _stateManager.getLastException();
                    if (e != null)
                    {
                        if (e instanceof QpidException)
                        {
                            QpidException amqe = (QpidException) e;
                            throw amqe.cloneForCurrentThread();
                        }
                        else
                        {
                            throw new AMQException(ErrorCodes.INTERNAL_ERROR, e.getMessage(), e);
                        }
                    }
                }

                _frameListeners.add(listener);
                //FIXME: At this point here we should check or before add we should check _stateManager is in an open
                // state so as we don't check we are likely just to time out here as I believe is being seen in QPID-1255
            }
            writeFrame(frame);

            long actualTimeout = timeout == -1 ? DEFAULT_SYNC_TIMEOUT : timeout;
            return listener.blockForFrame(actualTimeout);
            // When control resumes before this line, a reply will have been received
            // that matches the criteria defined in the blocking listener
        }
        finally
        {
            // If we don't removeKey the listener then no-one will
            _frameListeners.remove(listener);
        }

    }

    /** More convenient method to write a frame and wait for it's response. */
    public AMQMethodEvent syncWrite(AMQFrame frame, Class responseClass) throws QpidException, FailoverException
    {
        return syncWrite(frame, responseClass, DEFAULT_SYNC_TIMEOUT);
    }

    /** More convenient method to write a frame and wait for it's response. */
    public AMQMethodEvent syncWrite(AMQFrame frame, Class responseClass, long timeout) throws QpidException, FailoverException
    {
        return writeCommandFrameAndWaitForReply(frame, new SpecificMethodFrameListener(frame.getChannel(), responseClass),
                                                timeout);
    }

    public void closeSession(AMQSession session) throws QpidException
    {
        _protocolSession.closeSession(session);
    }

    /**
     * Closes the connection.
     * <p>
     * If a failover exception occurs whilst closing the connection it is ignored, as the connection is closed
     * anyway.
     *
     * @param timeout The timeout to wait for an acknowledgment to the close request.
     *
     * @throws QpidException If the close fails for any reason.
     */
    public void closeConnection(long timeout) throws QpidException
    {
        if (getStateManager().getCurrentState().equals(AMQState.CONNECTION_OPEN))
        {
            // Connection is already closed then don't do a syncWrite
            try
            {
                final ConnectionCloseBody body = _protocolSession.getMethodRegistry().createConnectionCloseBody(
                        ErrorCodes.REPLY_SUCCESS,
                        // replyCode
                        new AMQShortString("JMS client is closing the connection."),
                        0,
                        0);
                final AMQFrame frame = body.generateFrame(0);

                syncWrite(frame, ConnectionCloseOkBody.class, timeout);
                _network.close();
                closed();
            }
            catch (AMQTimeoutException e)
            {
                closed();
            }
            catch (FailoverException e)
            {
                _logger.debug("FailoverException interrupted connection close, ignoring as connection closed anyway.");
            }
        }
    }

    /** @return the number of bytes read from this protocol session */
    public long getReadBytes()
    {
        return _readBytes;
    }

    /** @return the number of bytes written to this protocol session */
    public long getWrittenBytes()
    {
        return _writtenBytes;
    }

    public void blockUntilNotFailingOver() throws InterruptedException
    {
        synchronized(_failoverLatchChange)
        {
            if (_failoverLatch != null)
            {
                if(!_failoverLatch.await(MAXIMUM_STATE_WAIT_TIME, TimeUnit.MILLISECONDS))
                {

                }
            }
        }
    }

    public String generateQueueName()
    {
        int id;
        synchronized (_queueIdLock)
        {
            id = _queueId++;
        }
        // convert '.', '/', ':' and ';' to single '_', for spec compliance and readability
        String localAddress = getLocalAddress().toString().replaceAll("[./:;]", "_");
        String queueName = "tmp_" + localAddress + "_" + id;
        return queueName.replaceAll("_+", "_");
    }

    public CountDownLatch getFailoverLatch()
    {
        synchronized (_failoverLatchChange)
        {
            return _failoverLatch;
        }
    }

    public void setFailoverLatch(CountDownLatch failoverLatch)
    {
        synchronized (_failoverLatchChange)
        {
            _failoverLatch = failoverLatch;
        }
    }

    public AMQConnection getConnection()
    {
        return _connection;
    }

    public AMQStateManager getStateManager()
    {
        return _stateManager;
    }

    public void setStateManager(AMQStateManager stateManager)
    {
        _stateManager = stateManager;
        _stateManager.setProtocolSession(_protocolSession);
    }

    public AMQProtocolSession getProtocolSession()
    {
        return _protocolSession;
    }

    synchronized FailoverState getFailoverState()
    {
        return _failoverState;
    }

    public synchronized void setFailoverState(FailoverState failoverState)
    {
        _failoverState= failoverState;
    }

    public MethodRegistry getMethodRegistry()
    {
        return _protocolSession.getMethodRegistry();
    }

    public ProtocolVersion getProtocolVersion()
    {
        return _protocolSession.getProtocolVersion();
    }

    public SocketAddress getRemoteAddress()
    {
        return _network.getRemoteAddress();
    }

    public SocketAddress getLocalAddress()
    {
        return _network.getLocalAddress();
    }

    public void setNetworkConnection(NetworkConnection network)
    {
        setNetworkConnection(network, network.getSender());
    }

    public void setNetworkConnection(NetworkConnection network, ByteBufferSender sender)
    {
        _network = network;
        _sender = sender;
        _protocolSession.setSender(sender);
    }

    @Override
    public long getLastReadTime()
    {
        return _lastReadTime;
    }

    @Override
    public long getLastWriteTime()
    {
        return _lastWriteTime;
    }

    protected ByteBufferSender getSender()
    {
        return _sender;
    }

    public NetworkConnection getNetworkConnection()
    {
        return _network;
    }

    public ProtocolVersion getSuggestedProtocolVersion()
    {
        return _suggestedProtocolVersion;
    }


    public void setHeartbeatListener(HeartbeatListener listener)
    {
        _heartbeatListener = listener == null ? HeartbeatListener.DEFAULT : listener;
    }

    public void heartbeatBodyReceived()
    {
        _heartbeatListener.heartbeatReceived();
    }

    public void setMaxFrameSize(final long frameMax)
    {
        _decoder.setMaxFrameSize(frameMax == 0l || frameMax > (long) Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) frameMax);
    }

    public void init(final ConnectionSettings settings)
    {
        _decoder = new ClientDecoder(_protocolSession.getMethodProcessor());
        _protocolSession.init(settings);
    }

    public long getDefaultTimeout()
    {
        return DEFAULT_SYNC_TIMEOUT;
    }
}
