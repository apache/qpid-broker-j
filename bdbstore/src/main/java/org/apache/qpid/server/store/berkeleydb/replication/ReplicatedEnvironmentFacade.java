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
package org.apache.qpid.server.store.berkeleydb.replication;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.sleepycat.je.*;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.rep.*;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.util.DbPing;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.VLSN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.berkeleydb.BDBUtils;
import org.apache.qpid.server.store.berkeleydb.CoalescingCommiter;
import org.apache.qpid.server.store.berkeleydb.EnvHomeRegistry;
import org.apache.qpid.server.store.berkeleydb.EnvironmentFacade;
import org.apache.qpid.server.store.berkeleydb.EnvironmentUtils;
import org.apache.qpid.server.store.berkeleydb.logging.Slf4jLoggingHandler;
import org.apache.qpid.server.store.berkeleydb.upgrade.Upgrader;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.DaemonThreadFactory;
import org.apache.qpid.server.util.ExternalServiceException;
import org.apache.qpid.server.util.ExternalServiceTimeoutException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ReplicatedEnvironmentFacade implements EnvironmentFacade, StateChangeListener
{
    public static final String MASTER_TRANSFER_TIMEOUT_PROPERTY_NAME = "qpid.bdb.ha.master_transfer_interval";
    public static final String DB_PING_SOCKET_TIMEOUT_PROPERTY_NAME = "qpid.bdb.ha.db_ping_socket_timeout";
    public static final String REMOTE_NODE_MONITOR_INTERVAL_PROPERTY_NAME = "qpid.bdb.ha.remote_node_monitor_interval";
    public static final String REMOTE_NODE_MONITOR_TIMEOUT_PROPERTY_NAME = "qpid.bdb.ha.remote_node_monitor_timeout";
    public static final String ENVIRONMENT_RESTART_RETRY_LIMIT_PROPERTY_NAME = "qpid.bdb.ha.environment_restart_retry_limit";
    public static final String EXECUTOR_SHUTDOWN_TIMEOUT_PROPERTY_NAME = "qpid.bdb.ha.executor_shutdown_timeout";

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicatedEnvironmentFacade.class);

    private static final int DEFAULT_MASTER_TRANSFER_TIMEOUT = 1000 * 60;
    private static final int DEFAULT_DB_PING_SOCKET_TIMEOUT = 1000;
    private static final int DEFAULT_REMOTE_NODE_MONITOR_INTERVAL = 1000;
    private static final int DEFAULT_REMOTE_NODE_MONITOR_TIMEOUT = 1000;
    private static final int DEFAULT_ENVIRONMENT_RESTART_RETRY_LIMIT = 3;
    private static final int DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT = 5000;

    /** Length of time allowed for a master transfer to complete before the operation will timeout */
    private final int _masterTransferTimeout;

    /**
     * Qpid monitors the health of the other nodes in the group.  This controls the length of time
     * between each scan of the all the other nodes.  For a master, this health check allows the
     * node is discover it has become partitioned without awaiting a persistent business transaction.
     */
    private final int _remoteNodeMonitorInterval;

    /**
     * Qpid uses DbPing to establish the health of other nodes in the group.  This controls the length
     * of time the check will await the response from DbPing before assuming the node is unavailable
     */
    private final int _remoteNodeMonitorTimeout;

    /**
     * Qpid uses DbPing to establish the health of other nodes in the group.  This controls the socket timeout
     * (so_timeout) used on the socket underlying DbPing.
     */
    private final int _dbPingSocketTimeout;

    /**
     * If the environment creation fails, Qpid will automatically retry.  This controls the number
     * of times recreation will be attempted.
     */
    private final int _environmentRestartRetryLimit;

    /**
     * Length of time for executors used by facade to shutdown gracefully
     */
    private final int _executorShutdownTimeout;

    private final int _logHandlerCleanerProtectedFilesLimit;

    static final SyncPolicy LOCAL_TRANSACTION_SYNCHRONIZATION_POLICY = SyncPolicy.SYNC;
    static final SyncPolicy REMOTE_TRANSACTION_SYNCHRONIZATION_POLICY = SyncPolicy.NO_SYNC;
    public static final ReplicaAckPolicy REPLICA_REPLICA_ACKNOWLEDGMENT_POLICY = ReplicaAckPolicy.SIMPLE_MAJORITY;

    @SuppressWarnings("serial")
    private static final Map<String, String> REPCONFIG_DEFAULTS = Collections.unmodifiableMap(new HashMap<String, String>()
    {{
        /**
         * Parameter decreased as the 24h default may lead very large log files for most users.
         */
        put(ReplicationConfig.REP_STREAM_TIMEOUT, "1 h");
        /**
         * Parameter increased as the 5 s default may lead to spurious timeouts.
         */
        put(ReplicationConfig.REPLICA_ACK_TIMEOUT, "15 s");
        /**
         * Parameter increased as the 10 s default may lead to spurious timeouts.
         */
        put(ReplicationConfig.INSUFFICIENT_REPLICAS_TIMEOUT, "20 s");
        /**
         * Parameter decreased as the 10 h default may cause user confusion.
         */
        put(ReplicationConfig.ENV_SETUP_TIMEOUT, "180 s");
        /**
         * Parameter changed from default (off) to allow the Environment to start in the
         * UNKNOWN state when the majority is not available.
         */
        put(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "5 s");
        /**
         * Parameter changed from default true so we adopt immediately adopt the new behaviour early. False
         * is scheduled to become default after JE 5.1.
         */
        put(ReplicationConfig.PROTOCOL_OLD_STRING_ENCODING, Boolean.FALSE.toString());
         /**
          * Allow Replica to proceed with transactions regardless of the state of a Replica
          * At the moment we do not read or write databases on Replicas.
          * Setting consistency policy to NoConsistencyRequiredPolicy
          * would allow to create transaction on Replica immediately.
          * Any followed write operation would fail with ReplicaWriteException.
          */
        put(ReplicationConfig.CONSISTENCY_POLICY, NoConsistencyRequiredPolicy.NAME);
    }});

    private static final Set<String> PARAMS_SET_BY_DEFAULT;

    static
    {
        Set<String> excludes = new HashSet<>(ENVCONFIG_DEFAULTS.keySet());

        excludes.addAll(REPCONFIG_DEFAULTS.keySet());

        excludes.addAll(Arrays.asList(EnvironmentConfig.MAX_MEMORY,
                                      EnvironmentConfig.MAX_MEMORY_PERCENT
                                     ));
        PARAMS_SET_BY_DEFAULT = Collections.unmodifiableSet(excludes);
    }


    public static final String PERMITTED_NODE_LIST = "permittedNodes";

    private final ReplicatedEnvironmentConfiguration _configuration;
    private final String _prettyGroupNodeName;
    private final File _environmentDirectory;

    private final ExecutorService _environmentJobExecutor;
    private final ListeningExecutorService _stateChangeExecutor;

    /**
     * Executor used to learn about changes in the group.  Number of threads in the pool is maintained dynamically
     * to be number of nodes in the group + 1.   This gives us sufficient threads to 'ping' all the remote nodes in the
     * group (in parallel), and a thread for the coordination of the pings.  We also use the executor for the
     * transfer master operation.
     */
    private final ScheduledThreadPoolExecutor _groupChangeExecutor;
    private final AtomicReference<State> _state = new AtomicReference<State>(State.OPENING);
    private final ConcurrentMap<String, ReplicationNode> _remoteReplicationNodes = new ConcurrentHashMap<String, ReplicationNode>();
    private final AtomicReference<ReplicationGroupListener> _replicationGroupListener = new AtomicReference<ReplicationGroupListener>();
    private final AtomicReference<StateChangeListener> _stateChangeListener = new AtomicReference<StateChangeListener>();
    private final Durability _defaultDurability;
    private final ConcurrentMap<String, Database> _cachedDatabases = new ConcurrentHashMap<>();
    private final ConcurrentMap<DatabaseEntry, Sequence> _cachedSequences = new ConcurrentHashMap<>();
    private final AtomicReference<ReplicatedEnvironment> _environment = new AtomicReference<>();

    private final Set<String> _permittedNodes = new CopyOnWriteArraySet<String>();
    private volatile Durability _realMessageStoreDurability = null;
    private volatile Durability _messageStoreDurability;
    private volatile CoalescingCommiter _coalescingCommiter = null;
    private volatile long _joinTime;
    private volatile ReplicatedEnvironment.State _lastKnownEnvironmentState;
    private volatile long _envSetupTimeoutMillis;
    /** Flag set true when JE need to discard transactions in order to rejoin the group */
    private volatile boolean _nodeRolledback;

    public ReplicatedEnvironmentFacade(ReplicatedEnvironmentConfiguration configuration)
    {
        _environmentDirectory = new File(configuration.getStorePath());
        if (!_environmentDirectory.exists())
        {
            if (!_environmentDirectory.mkdirs())
            {
                throw new IllegalArgumentException("Environment path " + _environmentDirectory + " could not be read or created. "
                                                   + "Ensure the path is correct and that the permissions are correct.");
            }
        }
        else if(_environmentDirectory.isFile())
        {
            throw new IllegalArgumentException("Environment path " + _environmentDirectory + " exists as a file - not a directory. "
                                               + "Ensure the path is correct.");

        }
        else
        {
            LOGGER.debug("Environment at path " + _environmentDirectory + " already exists.");
        }

        _configuration = configuration;

        _masterTransferTimeout = configuration.getFacadeParameter(Integer.class,
                                                                  MASTER_TRANSFER_TIMEOUT_PROPERTY_NAME,
                                                                  DEFAULT_MASTER_TRANSFER_TIMEOUT);
        _dbPingSocketTimeout = configuration.getFacadeParameter(Integer.class,
                                                                DB_PING_SOCKET_TIMEOUT_PROPERTY_NAME, DEFAULT_DB_PING_SOCKET_TIMEOUT);
        _remoteNodeMonitorInterval = configuration.getFacadeParameter(Integer.class,
                                                                      REMOTE_NODE_MONITOR_INTERVAL_PROPERTY_NAME, DEFAULT_REMOTE_NODE_MONITOR_INTERVAL);
        _remoteNodeMonitorTimeout = configuration.getFacadeParameter(Integer.class,
                                                                     REMOTE_NODE_MONITOR_TIMEOUT_PROPERTY_NAME,
                                                                     DEFAULT_REMOTE_NODE_MONITOR_TIMEOUT);
        _environmentRestartRetryLimit = configuration.getFacadeParameter(Integer.class,
                                                                         ENVIRONMENT_RESTART_RETRY_LIMIT_PROPERTY_NAME, DEFAULT_ENVIRONMENT_RESTART_RETRY_LIMIT);
        _executorShutdownTimeout = configuration.getFacadeParameter(Integer.class,
                                                                    EXECUTOR_SHUTDOWN_TIMEOUT_PROPERTY_NAME, DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT);
        _logHandlerCleanerProtectedFilesLimit = _configuration.getFacadeParameter(Integer.class,
                                                                                  LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME,
                                                                                  DEFAULT_LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT);

        _defaultDurability = new Durability(LOCAL_TRANSACTION_SYNCHRONIZATION_POLICY, REMOTE_TRANSACTION_SYNCHRONIZATION_POLICY, REPLICA_REPLICA_ACKNOWLEDGMENT_POLICY);
        _prettyGroupNodeName = _configuration.getGroupName() + ":" + _configuration.getName();

        // we rely on this executor being single-threaded as we need to restart and mutate the environment from one thread only
        _environmentJobExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("Environment-" + _prettyGroupNodeName));
        _stateChangeExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(new DaemonThreadFactory("StateChange-" + _prettyGroupNodeName)));
        _groupChangeExecutor = new ScheduledThreadPoolExecutor(2, new DaemonThreadFactory("Group-Change-Learner:" + _prettyGroupNodeName));

        // create environment in a separate thread to avoid renaming of the current thread by JE
        EnvHomeRegistry.getInstance().registerHome(_environmentDirectory);
        boolean success = false;
        try
        {
            createEnvironment(true, new Runnable(){
                @Override
                public void run()
                {
                    populateExistingRemoteReplicationNodes();
                    int numberOfRemoteNodes = _remoteReplicationNodes.size();
                    if (numberOfRemoteNodes > 0)
                    {
                        int newPoolSize = numberOfRemoteNodes
                                          + 1 /* for this node */
                                          + 1 /* for coordination */;
                        _groupChangeExecutor.setCorePoolSize(newPoolSize);
                        LOGGER.debug("Setting group change executor core pool size to {}", newPoolSize);
                    }
                    _groupChangeExecutor.submit(new RemoteNodeStateLearner());
                }
            });
            success = true;
        }
        finally
        {
            if (!success)
            {
                EnvHomeRegistry.getInstance().deregisterHome(_environmentDirectory);
            }
        }
    }

    @Override
    public Transaction beginTransaction(TransactionConfig transactionConfig)
    {
        return getEnvironment().beginTransaction(null, transactionConfig);
    }

    @Override
    public void commit(final Transaction tx, boolean syncCommit)
    {
        try
        {
            // Using commit() instead of commitNoSync() for the HA store to allow
            // the HA durability configuration to influence resulting behaviour.
            tx.commit(_realMessageStoreDurability);
        }
        catch (DatabaseException de)
        {
            throw handleDatabaseException("Got DatabaseException on commit, closing environment", de);
        }

        if (_coalescingCommiter != null && _realMessageStoreDurability.getLocalSync() == SyncPolicy.NO_SYNC
                && _messageStoreDurability.getLocalSync() == SyncPolicy.SYNC)
        {
            _coalescingCommiter.commit(tx, syncCommit);
        }

    }

    @Override
    public <X> ListenableFuture<X> commitAsync(final Transaction tx, final X val)
    {
        try
        {
            // Using commit() instead of commitNoSync() for the HA store to allow
            // the HA durability configuration to influence resulting behaviour.
            tx.commit(_realMessageStoreDurability);
        }
        catch (DatabaseException de)
        {
            throw handleDatabaseException("Got DatabaseException on commit, closing environment", de);
        }

        if (_coalescingCommiter != null && _realMessageStoreDurability.getLocalSync() == SyncPolicy.NO_SYNC
            && _messageStoreDurability.getLocalSync() == SyncPolicy.SYNC)
        {
            return _coalescingCommiter.commitAsync(tx, val);
        }
        return Futures.immediateFuture(val);
    }

    @Override
    public void close()
    {
        if (_state.compareAndSet(State.OPENING, State.CLOSING) ||
            _state.compareAndSet(State.OPEN, State.CLOSING) ||
            _state.compareAndSet(State.RESTARTING, State.CLOSING) )
        {
            try
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Closing replicated environment facade for " + _prettyGroupNodeName + " current state is " + _state.get());
                }

                long timeout = Math.max(_executorShutdownTimeout, _envSetupTimeoutMillis);
                shutdownAndAwaitExecutorService(_environmentJobExecutor,
                                                timeout,
                                                TimeUnit.MILLISECONDS);
                shutdownAndAwaitExecutorService(_groupChangeExecutor, _executorShutdownTimeout, TimeUnit.MILLISECONDS);
                shutdownAndAwaitExecutorService(_stateChangeExecutor, _executorShutdownTimeout, TimeUnit.MILLISECONDS);

                try
                {
                    if (_coalescingCommiter != null)
                    {
                        _coalescingCommiter.stop();
                    }
                    closeSequences();
                    closeDatabases();
                }
                finally
                {
                    try
                    {
                        if (LOGGER.isDebugEnabled())
                        {
                            LOGGER.debug("Closing replicated environment");
                        }

                        closeEnvironment();
                    }
                    finally
                    {
                        if (LOGGER.isDebugEnabled())
                        {
                            LOGGER.debug("Deregistering environment home " + _environmentDirectory);
                        }

                        EnvHomeRegistry.getInstance().deregisterHome(_environmentDirectory);
                    }
                }
            }
            finally
            {
                _state.compareAndSet(State.CLOSING, State.CLOSED);
            }
        }
    }

    private void shutdownAndAwaitExecutorService(ExecutorService executorService, long executorShutdownTimeout, TimeUnit timeUnit)
    {
        executorService.shutdown();
        try
        {
            boolean wasShutdown = executorService.awaitTermination(executorShutdownTimeout, timeUnit);
            if (!wasShutdown)
            {
                LOGGER.warn("Executor service " + executorService +
                            " did not shutdown within allowed time period " + _executorShutdownTimeout
                            + " " + timeUnit + ", ignoring");
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            LOGGER.warn("Shutdown of executor service " + executorService + " was interrupted");
        }
    }

    @Override
    public RuntimeException handleDatabaseException(String contextMessage, final RuntimeException dbe)
    {
        if (dbe instanceof LogWriteException)
        {
            // something wrong with the disk (for example, no space left on device)
            // store cannot operate
            throw new ServerScopedRuntimeException("Cannot save data into the store", dbe);
        }

        if (dbe instanceof ServerScopedRuntimeException )
        {
            // always throw ServerScopedRuntimeException to prevent its swallowing
            throw dbe;
        }
        else if (dbe instanceof ConnectionScopedRuntimeException)
        {
            return dbe;
        }
        else if (dbe instanceof DatabaseException)
        {
            boolean noMajority = dbe instanceof InsufficientReplicasException || dbe instanceof InsufficientAcksException;

            if (noMajority)
            {
                ReplicationGroupListener listener = _replicationGroupListener.get();
                if (listener != null)
                {
                    listener.onNoMajority();
                }
            }

            if (dbe instanceof UnknownMasterException)
            {
                // when Master transits into Unknown state ( for example, due to mastership transfer)
                // we need to abort any ongoing je operation without halting the Broker or VHN/VH
                return new ConnectionScopedRuntimeException(String.format("Environment '%s' cannot finish JE operation because master is unknown", getNodeName()), dbe);
            }

            if (dbe instanceof ReplicaWriteException || dbe instanceof ReplicaConsistencyException)
            {
                // Master transited into Detached/Replica but underlying Configured Object has not been notified yet
                // and attempted to perform JE operation.
                // We need to abort any ongoing JE operation without halting the Broker or VHN/VH
                return new ConnectionScopedRuntimeException(String.format("Environment '%s' cannot finish JE operation because node is not master", getNodeName()), dbe);
            }

            boolean restart = (noMajority || dbe instanceof RestartRequiredException);
            if (restart)
            {
                tryToRestartEnvironment((DatabaseException)dbe);
                return new ConnectionScopedRuntimeException(noMajority ? "Required number of nodes not reachable" : "Underlying JE environment is being restarted", dbe);
            }
        }
        else
        {
            if (dbe instanceof IllegalStateException && getFacadeState() == State.RESTARTING)
            {
                return new ConnectionScopedRuntimeException("Underlying JE environment is being restarted", dbe);
            }
        }
        return new StoreException(contextMessage, dbe);
    }

    private void tryToRestartEnvironment(final DatabaseException dbe)
    {
        if (_state.compareAndSet(State.OPEN, State.RESTARTING) || _state.compareAndSet(State.OPENING, State.RESTARTING))
        {
            if (dbe != null && LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Environment restarting due to exception {}", dbe.getMessage(), dbe);
            }

            // Tell the virtualhostnode that we are no longer attached to the group.  It will close the virtualhost,
            // closing the connections, housekeeping etc meaning all transactions are finished before we
            // restart the environment.
            _stateChangeExecutor.submit(new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    StateChangeListener listener = _stateChangeListener.get();
                    if (listener != null && _state.get() == State.RESTARTING)
                    {
                        try
                        {
                            StateChangeEvent detached = new StateChangeEvent(ReplicatedEnvironment.State.DETACHED, NameIdPair.NULL);
                            listener.stateChange(detached);
                        }
                        catch (Throwable t)
                        {
                            handleUncaughtExceptionInExecutorService(t);
                        }
                    }

                    return null;
                }
            }).addListener(new Runnable()
            {
                @Override
                public void run()
                {
                    int attemptNumber = 1;
                    boolean restarted = false;
                    Exception lastException = null;
                    while(_state.get() == State.RESTARTING && attemptNumber <= _environmentRestartRetryLimit)
                    {
                        try
                        {
                            restartEnvironment();
                            restarted = true;
                            break;
                        }
                        catch(EnvironmentFailureException e)
                        {
                            LOGGER.warn("Failure whilst trying to restart environment (attempt number "
                                    + "{} of {})", attemptNumber, _environmentRestartRetryLimit, e);
                            lastException = e;
                        }
                        catch (Exception e)
                        {
                            LOGGER.error("Fatal failure whilst trying to restart environment", e);
                            lastException = e;
                            break;
                        }
                        attemptNumber++;
                    }

                    if (!restarted)
                    {
                        LOGGER.error("Failed to restart environment.");
                        if (lastException != null)
                        {
                            handleUncaughtExceptionInExecutorService(lastException);
                        }
                    }
                }
            }, _environmentJobExecutor);
        }
        else if (_state.get() == State.RESTARTING)
        {
            LOGGER.debug("Environment restart already in progress, ignoring restart request.");
        }
        else
        {
            LOGGER.debug("Ignoring restart because the environment because state is {}", _state.get());
        }
    }

    @Override
    public Database openDatabase(String name, DatabaseConfig databaseConfig)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("openDatabase " + name + " for " + _prettyGroupNodeName);
        }
        if (_state.get() != State.OPEN)
        {
            throw new ConnectionScopedRuntimeException("Environment facade is not in opened state");
        }

        ReplicatedEnvironment environment = getEnvironment();

        Database cachedHandle = _cachedDatabases.get(name);
        if (cachedHandle == null)
        {
            Database handle = environment.openDatabase(null, name, databaseConfig);
            Database existingHandle = _cachedDatabases.putIfAbsent(name, handle);
            if (existingHandle == null)
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("openDatabase " + name + " new handle");
                }

                cachedHandle = handle;
            }
            else
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("openDatabase " + name + " existing handle");
                }
                cachedHandle = existingHandle;
                handle.close();
            }
        }
        return cachedHandle;
    }

    @Override
    public Database clearDatabase(Transaction txn, String databaseName, DatabaseConfig databaseConfig)
    {
        closeDatabase(databaseName);
        getEnvironment().removeDatabase(txn, databaseName);
        return getEnvironment().openDatabase(txn, databaseName, databaseConfig);
    }

    @Override
    public void closeDatabase(final String databaseName)
    {
        Database cachedHandle = _cachedDatabases.remove(databaseName);
        if (cachedHandle != null)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Closing " + databaseName + " on " + _prettyGroupNodeName);
            }
            if (cachedHandle.getEnvironment().isValid())
            {
                cachedHandle.close();
            }
        }
    }
    @Override
    public Sequence openSequence(final Database database,
                                 final DatabaseEntry sequenceKey,
                                 final SequenceConfig sequenceConfig)
    {
        Sequence cachedSequence = _cachedSequences.get(sequenceKey);
        if (cachedSequence == null)
        {
            Sequence handle = database.openSequence(null, sequenceKey, sequenceConfig);
            Sequence existingHandle = _cachedSequences.putIfAbsent(sequenceKey, handle);
            if (existingHandle == null)
            {
                cachedSequence = handle;
            }
            else
            {
                cachedSequence = existingHandle;
                handle.close();
            }
        }
        return cachedSequence;
    }


    private void closeSequence(final DatabaseEntry sequenceKey)
    {
        Sequence cachedHandle = _cachedSequences.remove(sequenceKey);
        if (cachedHandle != null)
        {
            cachedHandle.close();
        }
    }

    @Override
    public void stateChange(final StateChangeEvent stateChangeEvent)
    {
        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("The node '" + _prettyGroupNodeName + "' state is " + stateChangeEvent.getState());
        }

        if (_state.get() != State.CLOSING && _state.get() != State.CLOSED)
        {
            _stateChangeExecutor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        stateChanged(stateChangeEvent);
                    }
                    catch (Throwable e)
                    {
                        handleUncaughtExceptionInExecutorService(e);
                    }
                }
            });
        }
        else
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Ignoring the state environment change event as the environment facade for node '"
                             + _prettyGroupNodeName
                             + "' is in state "
                             + _state.get());
            }
        }
    }

    @Override
    public long getTotalLogSize()
    {
        return getEnvironment().getStats(null).getTotalLogSize();
    }

    @Override
    public void reduceSizeOnDisk()
    {
        BDBUtils.runCleaner(getEnvironment());
    }

    @Override
    public void flushLog()
    {
        try
        {
            getEnvironment().flushLog(true);
        }
        catch (RuntimeException e)
        {
            throw handleDatabaseException("Exception whilst syncing data to disk", e);
        }
    }

    @Override
    public void setCacheSize(final long cacheSize)
    {
        LOGGER.debug("Submitting a job to set cache size on {} to {}", _prettyGroupNodeName, cacheSize);

        Callable<Void> task = new Callable<Void>()
        {
            @Override
            public Void call()
            {
                setCacheSizeInternal(cacheSize);
                return null;
            }
        };
        submitEnvironmentTask(1, task, "setting cache size");
    }

    @Override
    public void updateMutableConfig(final ConfiguredObject<?> object)
    {
        LOGGER.debug("Submitting a job to set update mutable config on {}", _prettyGroupNodeName);

        Callable<Void> task = new Callable<Void>()
        {
            @Override
            public Void call()
            {
                EnvironmentUtils.updateMutableConfig(getEnvironment(), PARAMS_SET_BY_DEFAULT, true, object);
                return null;
            }
        };
        submitEnvironmentTask(5, task, "updating mutable config");

    }


    @Override
    public int cleanLog()
    {
        LOGGER.debug("Submitting a job to clean log files on {} ", _prettyGroupNodeName);
        int timeout = 5;

        Callable<Integer> task = new Callable<Integer>()
        {
            @Override
            public Integer call()
            {
                return getEnvironment().cleanLog();
            }
        };


        Integer fileCount = submitEnvironmentTask(timeout, task, "cleaning log files");
        return fileCount == null ? 0 : fileCount;
    }

    @Override
    public void checkpoint(final boolean force)
    {
        LOGGER.debug("Submitting a job to perform checkpoint on {} ", _prettyGroupNodeName);
        int timeout = 5;

        Callable<Void> task = new Callable<Void>()
        {
            @Override
            public Void call()
            {
                CheckpointConfig checkpointConfig = new CheckpointConfig();
                checkpointConfig.setForce(force);
                getEnvironment().checkpoint(checkpointConfig);
                return null;
            }
        };

        submitEnvironmentTask(timeout, task, "perform checkpoint");
    }

    @Override
    public Map<String, Map<String, Object>> getEnvironmentStatistics(final boolean reset)
    {
        LOGGER.debug("Submitting a job to get environment statistics on {} ", _prettyGroupNodeName);
        int timeout = 5;

        Callable<Map<String,Map<String,Object>>> task = new Callable<Map<String,Map<String,Object>>>()
        {
            @Override
            public Map<String,Map<String,Object>> call()
            {
                return EnvironmentUtils.getEnvironmentStatistics(getEnvironment(), reset);

            }
        };

        return submitEnvironmentTask(timeout, task, "get environment statistics");
    }

    @Override
    public Map<String, Object> getTransactionStatistics(final boolean reset)
    {
        LOGGER.debug("Submitting a job to get transaction statistics on {} ", _prettyGroupNodeName);
        int timeout = 5;

        Callable<Map<String,Object>> task = new Callable<Map<String,Object>>()
        {
            @Override
            public Map<String,Object> call()
            {
                return EnvironmentUtils.getTransactionStatistics(getEnvironment(), reset);
            }
        };

        return submitEnvironmentTask(timeout, task, "get transaction statistics");
    }

    @Override
    public Map<String,Object> getDatabaseStatistics(final String database, final boolean reset)
    {
        LOGGER.debug("Submitting a job to get database statistics for {} on {} ", database, _prettyGroupNodeName);
        int timeout = 5;

        Callable<Map<String,Object>> task = new Callable<Map<String,Object>>()
        {
            @Override
            public Map<String, Object> call()
            {

                return EnvironmentUtils.getDatabaseStatistics(getEnvironment(), database, reset);
            }
        };

        return submitEnvironmentTask(timeout, task, "get database statistics for '" + database + "'");

    }

    @Override
    public void deleteDatabase(final String databaseName)
    {
        closeDatabase(databaseName);
        getEnvironment().removeDatabase(null, databaseName);
    }


    private <T> T submitEnvironmentTask(final int timeout, final Callable<T> task, String action)
    {
        Future<T> future = _environmentJobExecutor.submit(task);
        try
        {
            return future.get(timeout, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Error)
            {
                throw (Error) cause;
            }
            else if (cause instanceof RuntimeException)
            {
                throw (RuntimeException) cause;
            }
            else
            {
                throw new ConnectionScopedRuntimeException("Unexpected exception while " + action, e);
            }
        }
        catch (TimeoutException e)
        {
            LOGGER.info("{}  on {} timed out after {} seconds", action, _prettyGroupNodeName, timeout);
        }

        return null;
    }


    @Override
    public void flushLogFailed(final RuntimeException e)
    {
        LOGGER.warn("Syncing data to disk failed", e);
        if (!(e instanceof ConnectionScopedRuntimeException))
        {
            throw e;
        }
    }

    void setCacheSizeInternal(long cacheSize)
    {
        final ReplicatedEnvironment environment = _environment.get();
        if (environment != null)
        {
            try
            {
                final EnvironmentMutableConfig oldConfig = environment.getMutableConfig();
                final EnvironmentMutableConfig newConfig = oldConfig.setCacheSize(cacheSize);
                environment.setMutableConfig(newConfig);

                LOGGER.debug("Node {} cache size has been changed to {}", _prettyGroupNodeName, cacheSize);
            }
            catch (RuntimeException e)
            {
                RuntimeException handled = handleDatabaseException("Exception on setting cache size", e);
                if (handled instanceof ConnectionScopedRuntimeException || handled instanceof ServerScopedRuntimeException)
                {
                    throw handled;
                }
                throw new ConnectionScopedRuntimeException("Cannot set cache size to " + cacheSize + " on node " + _prettyGroupNodeName, e);
            }
        }
        else
        {
            throw new ConnectionScopedRuntimeException("Cannot set cache size to "
                                                       + cacheSize
                                                       + " on node "
                                                       + _prettyGroupNodeName
                                                       + " as environment does not exist");
        }
    }


    public Set<ReplicationNode> getNodes()
    {
        return getEnvironment().getGroup().getNodes();
    }

    private void stateChanged(StateChangeEvent stateChangeEvent)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Received BDB event, new BDB state " + stateChangeEvent.getState() + " Facade state : " + _state.get());
        }
        ReplicatedEnvironment.State state = stateChangeEvent.getState();

        if ( _state.get() != State.CLOSED && _state.get() != State.CLOSING)
        {
            if (state == ReplicatedEnvironment.State.REPLICA || state == ReplicatedEnvironment.State.MASTER)
            {
                if (_state.compareAndSet(State.OPENING, State.OPEN) || _state.compareAndSet(State.RESTARTING, State.OPEN))
                {
                    LOGGER.info("The environment facade is in open state for node " + _prettyGroupNodeName);
                    _joinTime = System.currentTimeMillis();
                }
                if (state == ReplicatedEnvironment.State.MASTER)
                {
                    closeSequencesAndDatabasesSafely();
                }
            }

            StateChangeListener listener = _stateChangeListener.get();
            if (listener != null && (_state.get() == State.OPEN || _state.get() == State.RESTARTING))
            {
                listener.stateChange(stateChangeEvent);
            }

            if (_lastKnownEnvironmentState == ReplicatedEnvironment.State.MASTER && state == ReplicatedEnvironment.State.DETACHED && _state.get() == State.OPEN)
            {
                tryToRestartEnvironment(null);
            }
        }
        _lastKnownEnvironmentState = state;
    }

    public String getGroupName()
    {
        return (String)_configuration.getGroupName();
    }

    public String getNodeName()
    {
        return _configuration.getName();
    }

    public String getHostPort()
    {
        return (String)_configuration.getHostPort();
    }

    public String getHelperHostPort()
    {
        return (String)_configuration.getHelperHostPort();
    }

    Durability getRealMessageStoreDurability()
    {
        return _realMessageStoreDurability;
    }

    public Durability getMessageStoreDurability()
    {
        return _messageStoreDurability;
    }

    public boolean isCoalescingSync()
    {
        return _coalescingCommiter != null;
    }

    public String getNodeState()
    {
        if (_state.get() != State.OPEN)
        {
            return ReplicatedEnvironment.State.UNKNOWN.name();
        }

        try
        {
            ReplicatedEnvironment.State state = getEnvironment().getState();
            return state.toString();
        }
        catch (RuntimeException e)
        {
            throw handleDatabaseException("Cannot get environment state", e);
        }
    }

    public boolean isDesignatedPrimary()
    {
        return getEnvironment().getRepMutableConfig().getDesignatedPrimary();
    }

    public Future<Void> reapplyDesignatedPrimary()
    {
        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Submitting a job to set designated primary on {}", _prettyGroupNodeName);
        }

        return _environmentJobExecutor.submit(() ->
                                              {
                                                  mutateEnvironmentConfigValue(ReplicationMutableConfig.DESIGNATED_PRIMARY,
                                                                               _configuration.isDesignatedPrimary());
                                                  return null;
                                              });
    }

    int getPriority()
    {
        ReplicationMutableConfig repConfig = getEnvironment().getRepMutableConfig();
        return repConfig.getNodePriority();
    }

    public Future<Void> reapplyPriority()
    {
        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Submitting a job to set priority on {} ", _prettyGroupNodeName);
        }

        return _environmentJobExecutor.submit(() ->
                                              {
                                                  mutateEnvironmentConfigValue(ReplicationMutableConfig.NODE_PRIORITY,
                                                                               _configuration.getPriority());
                                                  return null;
                                              });
    }

    int getElectableGroupSizeOverride()
    {
        ReplicationMutableConfig repConfig = getEnvironment().getRepMutableConfig();
        return repConfig.getElectableGroupSizeOverride();
    }

    public Future<Void> reapplyElectableGroupSizeOverride()
    {
        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Submitting a job to reapply electable group override on {} ", _prettyGroupNodeName);
        }

        return _environmentJobExecutor.submit(() ->
                                              {
                                                  mutateEnvironmentConfigValue(ReplicationMutableConfig.ELECTABLE_GROUP_SIZE_OVERRIDE,
                                                                               _configuration.getQuorumOverride()
                                                                              );
                                                  return null;
                                              });
    }

    void mutateEnvironmentConfigValue(final String paramName, final Object newValue)
    {
        final ReplicatedEnvironment environment = _environment.get();
        if (environment != null)
        {
            try
            {
                final ReplicationMutableConfig oldConfig = environment.getRepMutableConfig();
                final ReplicationMutableConfig newConfig = oldConfig.setConfigParam(paramName, String.valueOf(newValue));
                environment.setRepMutableConfig(newConfig);

                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Node {} param {} has been changed to {}",
                                 _prettyGroupNodeName,
                                 paramName,
                                 newValue);
                }
            }
            catch (RuntimeException e)
            {
                RuntimeException handled = handleDatabaseException(String.format("Exception on setting %s", paramName), e);
                if (handled instanceof ConnectionScopedRuntimeException
                    || handled instanceof ServerScopedRuntimeException)
                {
                    throw handled;
                }
                throw new ConnectionScopedRuntimeException(String.format("Cannot set %s to %s on node %s",
                                                                         paramName, newValue, _prettyGroupNodeName), e);
            }
        }
        else
        {
            throw new ConnectionScopedRuntimeException(String.format("Cannot set %s to %s on node %s as environment does not currently exists",
                                                                     paramName, newValue, _prettyGroupNodeName));
        }
    }

    public Future<Void> transferMasterToSelfAsynchronously()
    {
        final String nodeName = getNodeName();
        return transferMasterAsynchronously(nodeName);
    }

    public Future<Void> transferMasterAsynchronously(final String nodeName)
    {
        // Transfer master contacts the group (using GroupAdmin) to request the mastership change.
        // It needs to be done asynchronously but not on the _environmentJobExecutor, as there is
        // no point delaying transfer master because we are restarting.
        return _groupChangeExecutor.submit(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                try
                {
                    ReplicationGroupAdmin admin = createReplicationGroupAdmin();
                    String newMaster = admin.transferMaster(Collections.singleton(nodeName),
                                                            _masterTransferTimeout, TimeUnit.MILLISECONDS, true);
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("The mastership has been transferred to " + newMaster);
                    }
                }
                catch (RuntimeException e)
                {
                    String message = "Exception on transferring the mastership to " + _prettyGroupNodeName
                            + " Master transfer timeout : " + _masterTransferTimeout;
                    LOGGER.warn(message, e);
                    throw handleDatabaseException(message, e);
                }
                return null;
            }
        });
    }

    public void removeNodeFromGroup(final String nodeName)
    {
        try
        {
            createReplicationGroupAdmin().removeMember(nodeName);
        }
        catch (MasterStateException e)
        {
            throw new IllegalStateTransitionException(String.format("Node '%s' cannot be deleted when role is a master",
                                                                    nodeName));
        }
        catch (MemberNotFoundException e)
        {
            throw new IllegalArgumentException(String.format("Node '%s' is not a member of the group", nodeName), e);
        }
        catch (RuntimeException e)
        {
            throw handleDatabaseException("Exception on node removal from group", e);
        }
    }

    public long getJoinTime()
    {
        return _joinTime;
    }

    public long getLastKnownReplicationTransactionId()
    {
        if (_state.get() == State.OPEN)
        {
            try
            {
                VLSNRange range = RepInternal.getRepImpl(getEnvironment()).getVLSNIndex().getRange();
                VLSN lastTxnEnd = range.getLastTxnEnd();
                return lastTxnEnd.getSequence();
            }
            catch (RuntimeException e)
            {
                throw handleDatabaseException("Exception on getting last known replication transaction id", e);
            }
        }
        else
        {
            return -1L;
        }
    }

    private ReplicationGroupAdmin createReplicationGroupAdmin()
    {
        final Set<InetSocketAddress> helpers = new HashSet<InetSocketAddress>();
        final ReplicationConfig repConfig = getEnvironment().getRepConfig();

        helpers.addAll(repConfig.getHelperSockets());
        helpers.add(HostPortPair.getSocket(HostPortPair.getString(repConfig.getNodeHostname(), repConfig.getNodePort())));

        return new ReplicationGroupAdmin(_configuration.getGroupName(), helpers);
    }

    private ReplicatedEnvironment getEnvironment()
    {
        if (getFacadeState() == State.RESTARTING)
        {
            throw new ConnectionScopedRuntimeException("Environment is restarting");
        }

        final ReplicatedEnvironment environment = _environment.get();
        if (environment == null)
        {
            throw new IllegalStateException("Environment is null.");
        }

        return environment;
    }

    @Override
    public void upgradeIfNecessary(ConfiguredObject<?> parent)
    {
        Upgrader upgrader = new Upgrader(getEnvironment(), parent);
        upgrader.upgradeIfNecessary();
    }

    public State getFacadeState()
    {
        return _state.get();
    }

    public void setStateChangeListener(StateChangeListener stateChangeListener)
    {
        if (_stateChangeListener.compareAndSet(null, stateChangeListener))
        {
            final ReplicatedEnvironment environment = _environment.get();
            if (environment != null)
            {
                environment.setStateChangeListener(this);
            }
        }
        else
        {
            throw new IllegalStateException("StateChangeListener is already set on " + _prettyGroupNodeName);
        }
    }

    private void closeEnvironment()
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Closing JE environment for " + _prettyGroupNodeName);
        }

        // Clean the log before closing. This makes sure it doesn't contain
        // redundant data. Closing without doing this means the cleaner may not
        // get a chance to finish.
        ReplicatedEnvironment environment = _environment.getAndSet(null);
        if (environment != null)
        {
            try
            {
                if (environment.isValid())
                {
                    BDBUtils.runCleaner(environment);
                }
            }
            finally
            {
                // Try closing the environment but swallow EnvironmentFailureException
                // if the environment becomes invalid while closing.
                // This can be caused by potential race between facade close and DatabasePinger open.
                try
                {
                    environment.close();
                }
                catch (EnvironmentFailureException efe)
                {
                    if (!environment.isValid())
                    {
                        LOGGER.debug("Environment became invalid on close, so ignore", efe);
                    }
                    else
                    {
                        throw efe;
                    }
                }
            }
        }
    }

    private void restartEnvironment()
    {
        LOGGER.info("Restarting environment");

        closeEnvironmentOnRestart();

        createEnvironment(false, null);

        LOGGER.info("Environment is restarted");
    }

    private void closeEnvironmentOnRestart()
    {
        ReplicatedEnvironment environment = _environment.getAndSet(null);
        if (environment != null)
        {
            closeSequencesAndDatabasesSafely();
            try
            {
                environment.close();
            }
            catch (EnvironmentFailureException efe)
            {
                LOGGER.warn("Ignoring an exception whilst closing environment", efe);
            }
        }
    }

    private void closeSequencesAndDatabasesSafely()
    {
        try
        {
            closeSequences();
            closeDatabases();
        }
        catch(Exception e)
        {
            LOGGER.warn("Ignoring an exception whilst closing databases", e);
        }
    }

    private void closeSequences()
    {
        RuntimeException firstThrownException = null;
        for (DatabaseEntry  sequenceKey : _cachedSequences.keySet())
        {
            try
            {
                closeSequence(sequenceKey);
            }
            catch(DatabaseException de)
            {
                if (firstThrownException == null)
                {
                    firstThrownException = de;
                }
            }
        }
        if (firstThrownException != null)
        {
            throw firstThrownException;
        }
    }

    private void closeDatabases()
    {
        RuntimeException firstThrownException = null;

        Iterator<String> itr = _cachedDatabases.keySet().iterator();
        while (itr.hasNext())
        {
            String databaseName = itr.next();

            if (databaseName != null)
            {
                try
                {
                    closeDatabase(databaseName);
                }
                catch(RuntimeException e)
                {
                    LOGGER.error("Failed to close database " + databaseName + " on " + _prettyGroupNodeName, e);
                    if (firstThrownException == null)
                    {
                        firstThrownException = e;
                    }
                }
            }
        }

        if (firstThrownException != null)
        {
            throw firstThrownException;
        }
    }

    private void createEnvironment(boolean createEnvironmentInSeparateThread, Runnable postCreationAction)
    {
        String groupName = _configuration.getGroupName();
        String helperHostPort = _configuration.getHelperHostPort();
        String hostPort = _configuration.getHostPort();
        boolean designatedPrimary = _configuration.isDesignatedPrimary();
        int priority = _configuration.getPriority();
        int quorumOverride = _configuration.getQuorumOverride();
        String nodeName = _configuration.getName();
        String helperNodeName = _configuration.getHelperNodeName();

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Creating environment");
            LOGGER.info("Environment path " + _environmentDirectory.getAbsolutePath());
            LOGGER.info("Group name " + groupName);
            LOGGER.info("Node name " + nodeName);
            LOGGER.info("Node host port " + hostPort);
            LOGGER.info("Helper host port " + helperHostPort);
            LOGGER.info("Helper node name " + helperNodeName);
            LOGGER.info("Durability " + _defaultDurability);
            LOGGER.info("Designated primary (applicable to 2 node case only) " + designatedPrimary);
            LOGGER.info("Node priority " + priority);
            LOGGER.info("Quorum override " + quorumOverride);
            LOGGER.info("Permitted node list " + _permittedNodes);

        }

        Map<String, String> replicationEnvironmentParameters = new HashMap<>(ReplicatedEnvironmentFacade.REPCONFIG_DEFAULTS);
        replicationEnvironmentParameters.putAll(_configuration.getReplicationParameters());

        ReplicationConfig replicationConfig = new ReplicationConfig(groupName, nodeName, hostPort);
        replicationConfig.setHelperHosts(helperHostPort);
        replicationConfig.setDesignatedPrimary(designatedPrimary);
        replicationConfig.setNodePriority(priority);
        replicationConfig.setElectableGroupSizeOverride(quorumOverride);

        for (Map.Entry<String, String> configItem : replicationEnvironmentParameters.entrySet())
        {
            if (LOGGER.isInfoEnabled())
            {
                LOGGER.info("Setting ReplicationConfig key " + configItem.getKey() + " to '" + configItem.getValue() + "'");
            }
            replicationConfig.setConfigParam(configItem.getKey(), configItem.getValue());
        }

        Map<String, String> environmentParameters = new HashMap<>(EnvironmentFacade.ENVCONFIG_DEFAULTS);
        environmentParameters.putAll(_configuration.getParameters());

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setExceptionListener(new ExceptionListener());
        envConfig.setDurability(_defaultDurability);
        envConfig.setCacheMode(_configuration.getCacheMode());


        envConfig.setLoggingHandler(new Slf4jLoggingHandler(_configuration));

        LOGGER.info("Cache mode {}", envConfig.getCacheMode());

        for (Map.Entry<String, String> configItem : environmentParameters.entrySet())
        {
            if (LOGGER.isInfoEnabled())
            {
                LOGGER.info("Setting EnvironmentConfig key " + configItem.getKey() + " to '" + configItem.getValue() + "'");
            }
            envConfig.setConfigParam(configItem.getKey(), configItem.getValue());
        }

        DbInternal.setLoadPropertyFile(envConfig, false);

        File propsFile = new File(_environmentDirectory, "je.properties");
        if(propsFile.exists())
        {
            LOGGER.warn("The BDB configuration file at '" + _environmentDirectory + File.separator +  "je.properties' will NOT be loaded.  Configure BDB using Qpid context variables instead.");
        }


        if (createEnvironmentInSeparateThread)
        {
            createEnvironmentInSeparateThread(_environmentDirectory, envConfig, replicationConfig, postCreationAction);
        }
        else
        {
            createEnvironment(_environmentDirectory, envConfig, replicationConfig, postCreationAction);
        }
    }

    private void createEnvironmentInSeparateThread(final File environmentPathFile, final EnvironmentConfig envConfig,
                                                   final ReplicationConfig replicationConfig, final Runnable postCreationAction)
    {
        Future<Void> environmentFuture = _environmentJobExecutor.submit(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                createEnvironment(environmentPathFile, envConfig, replicationConfig, postCreationAction);
                return null;
            }
        });

        final long setUpTimeOutMillis = extractEnvSetupTimeoutMillis(replicationConfig);
        final long initialTimeOutMillis = Math.max(setUpTimeOutMillis / 4, 1000);
        final long remainingTimeOutMillis = setUpTimeOutMillis - initialTimeOutMillis;
        try
        {
            try
            {
                environmentFuture.get(initialTimeOutMillis, TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException te)
            {
                if (remainingTimeOutMillis > 0)
                {
                    LOGGER.warn("Slow replicated environment creation for " + _prettyGroupNodeName
                                + ". Will continue to wait for further " + remainingTimeOutMillis
                                + "ms. for environment creation to complete.");
                    environmentFuture.get(remainingTimeOutMillis, TimeUnit.MILLISECONDS);
                }
                else
                {
                    throw te;
                }
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Environment creation was interrupted", e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Unexpected exception on environment creation", e.getCause());
        }
        catch (TimeoutException e)
        {
            throw new RuntimeException("JE replicated environment creation took too long (permitted time "
                                       + setUpTimeOutMillis + "ms)");
        }
    }

    private void createEnvironment(File environmentPathFile, EnvironmentConfig envConfig,
                                   final ReplicationConfig replicationConfig, Runnable action)
    {
        String originalThreadName = Thread.currentThread().getName();
        try
        {
            _envSetupTimeoutMillis = extractEnvSetupTimeoutMillis(replicationConfig);
            _environment.set(new ReplicatedEnvironment(environmentPathFile, replicationConfig, envConfig));
        }
        catch (final InsufficientLogException ile)
        {
            LOGGER.warn("The log files of this node are too old. Network restore will begin now.", ile);
            NetworkRestore restore = new NetworkRestore();
            NetworkRestoreConfig config = new NetworkRestoreConfig();
            config.setRetainLogFiles(false);
            restore.execute(ile, config);
            LOGGER.warn("Network restore complete.");
            _environment.set(new ReplicatedEnvironment(environmentPathFile, replicationConfig, envConfig));
        }
        finally
        {
            Thread.currentThread().setName(originalThreadName);
        }

        if (action != null)
        {
            action.run();
        }

        if (_stateChangeListener.get() != null)
        {
            final ReplicatedEnvironment environment = _environment.get();
            if (environment != null)
            {
                environment.setStateChangeListener(this);
            }
        }
        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Environment is created for node " + _prettyGroupNodeName);
        }
    }

    private long extractEnvSetupTimeoutMillis(ReplicationConfig replicationConfig)
    {
        return (long) PropUtil.parseDuration(replicationConfig.getConfigParam(ReplicationConfig.ENV_SETUP_TIMEOUT));
    }

    public int getNumberOfElectableGroupMembers()
    {
        try
        {
            return getEnvironment().getGroup().getElectableNodes().size();
        }
        catch(RuntimeException e)
        {
            throw handleDatabaseException("Exception on getting number of electable group members", e);
        }
    }

    public boolean isMaster()
    {
        return ReplicatedEnvironment.State.MASTER.name().equals(getNodeState());
    }

    public void setReplicationGroupListener(ReplicationGroupListener replicationGroupListener)
    {
        if (_replicationGroupListener.compareAndSet(null, replicationGroupListener))
        {
            notifyExistingRemoteReplicationNodes(replicationGroupListener);
            notifyNodeRolledbackIfNecessary(replicationGroupListener);
        }
        else
        {
            throw new IllegalStateException("ReplicationGroupListener is already set on " + _prettyGroupNodeName);
        }
    }

    /**
     * This method should only be invoked from configuration thread on virtual host activation.
     * Otherwise, invocation of this method whilst coalescing committer is committing transactions might result in transaction aborts.
     */
    public void setMessageStoreDurability(SyncPolicy localTransactionSynchronizationPolicy, SyncPolicy remoteTransactionSynchronizationPolicy, ReplicaAckPolicy replicaAcknowledgmentPolicy)
    {
        if (_messageStoreDurability == null || localTransactionSynchronizationPolicy != _messageStoreDurability.getLocalSync()
                || remoteTransactionSynchronizationPolicy != _messageStoreDurability.getReplicaSync()
                || replicaAcknowledgmentPolicy != _messageStoreDurability.getReplicaAck())
        {
            _messageStoreDurability = new Durability(localTransactionSynchronizationPolicy, remoteTransactionSynchronizationPolicy, replicaAcknowledgmentPolicy);

            if (_coalescingCommiter != null)
            {
                _coalescingCommiter.stop();
                _coalescingCommiter = null;
            }

            if (localTransactionSynchronizationPolicy == LOCAL_TRANSACTION_SYNCHRONIZATION_POLICY)
            {
                localTransactionSynchronizationPolicy = SyncPolicy.NO_SYNC;
                _coalescingCommiter = new CoalescingCommiter(_configuration.getGroupName(), this);
                _coalescingCommiter.start();
            }
            _realMessageStoreDurability = new Durability(localTransactionSynchronizationPolicy, remoteTransactionSynchronizationPolicy, replicaAcknowledgmentPolicy);
        }
    }

    public void setPermittedNodes(Collection<String> permittedNodes)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug(_prettyGroupNodeName + " permitted nodes set to " + permittedNodes);
        }

        _permittedNodes.clear();
        if (permittedNodes != null)
        {
            _permittedNodes.addAll(permittedNodes);
            // We register an app state monitor containing with permitted node list on
            // all nodes so that any node can be used as the helper when adding more nodes
            // to the group
            registerAppStateMonitorIfPermittedNodesSpecified(_permittedNodes);

            ReplicationGroupListener listener = _replicationGroupListener.get();
            int count = 0;
            for(ReplicationNode node: _remoteReplicationNodes.values())
            {
                if (!isNodePermitted(node))
                {
                    onIntruder(listener, node);
                }
                count++;
            }
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug(_prettyGroupNodeName + " checked  " + count + " node(s)");
            }
        }
    }

    static NodeState getRemoteNodeState(String groupName, ReplicationNode repNode, int dbPingSocketTimeout) throws IOException, ServiceConnectFailedException
    {
        if (repNode == null)
        {
            throw new IllegalArgumentException("Node cannot be null");
        }
        return new DbPing(repNode, groupName, dbPingSocketTimeout).getNodeState();
    }

    public static Set<String> convertApplicationStateBytesToPermittedNodeList(byte[] applicationState)
    {
        if (applicationState == null || applicationState.length == 0)
        {
            return Collections.emptySet();
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            Map<String, Object> settings = objectMapper.readValue(applicationState, Map.class);
            return new HashSet<String>((Collection<String>)settings.get(PERMITTED_NODE_LIST));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unexpected exception on de-serializing of application state", e);
        }
    }

    public static Collection<String> connectToHelperNodeAndCheckPermittedHosts(final String nodeName, final String hostPort, final String groupName, final String helperNodeName, final String helperHostPort, final int dbPingSocketTimeout)
    {
        ExecutorService executor = null;
        Future<Collection<String>> future = null;
        try
        {
            executor = Executors.newSingleThreadExecutor(new DaemonThreadFactory(String.format(
                            "PermittedHostsCheck-%s-%s",
                            groupName,
                            nodeName)));
            future = executor.submit(new Callable<Collection<String>>()
            {
                @Override
                public Collection<String> call() throws Exception
                {
                    return getPermittedHostsFromHelper(nodeName,
                                                       groupName,
                                                       helperNodeName,
                                                       helperHostPort,
                                                       dbPingSocketTimeout);
                }
            });

            try
            {
                final long timeout = (long) (dbPingSocketTimeout * 1.25);
                final Collection<String> permittedNodes =
                        dbPingSocketTimeout <= 0 ? future.get() : future.get(timeout, TimeUnit.MILLISECONDS);

                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug(String.format("Node '%s' permits nodes: '%s'", helperNodeName, String.valueOf(permittedNodes)));
                }

                if (permittedNodes == null || !permittedNodes.contains(hostPort))
                {
                    throw new IllegalConfigurationException(String.format("Node using address '%s' is not permitted to join the group '%s'",
                                                                          hostPort, groupName));
                }

                return permittedNodes;
            }
            catch (ExecutionException e)
            {
                final Throwable cause = e.getCause();
                if (cause instanceof RuntimeException)
                {
                    throw (RuntimeException) cause;
                }
                else
                {
                    throw new RuntimeException(cause);
                }
            }
            catch (TimeoutException e)
            {
                future.cancel(true);
                throw new ExternalServiceTimeoutException(String.format(
                        "Task timed out trying to connect to existing node '%s' at '%s'", nodeName, hostPort));
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new ExternalServiceException(String.format(
                        "Task failed to connect to existing node '%s' at '%s'", nodeName, hostPort));
            }
        }
        finally
        {
            executor.shutdown();
        }
    }

    private static Collection<String> getPermittedHostsFromHelper(final String nodeName,
                                                                  final String groupName,
                                                                  final String helperNodeName,
                                                                  final String helperHostPort,
                                                                  final int dbPingSocketTimeout)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug(String.format("Requesting state of the node '%s' at '%s'", helperNodeName, helperHostPort));
        }

        if (helperNodeName == null || "".equals(helperNodeName))
        {
            throw new IllegalConfigurationException(String.format("A helper node is not specified for node '%s'"
                                                                  + " joining the group '%s'", nodeName, groupName));
        }

        Collection<String> permittedNodes = null;
        try
        {
            ReplicationNodeImpl node = new ReplicationNodeImpl(helperNodeName, helperHostPort);
            NodeState state = getRemoteNodeState(groupName, node, dbPingSocketTimeout);
            byte[] applicationState = state.getAppState();
            return convertApplicationStateBytesToPermittedNodeList(applicationState);
        }
        catch (SocketTimeoutException ste)
        {
            throw new ExternalServiceTimeoutException(String.format("Timed out trying to connect to existing node '%s' at '%s'",
                                                                    helperNodeName, helperHostPort), ste);
        }
        catch (IOException | ServiceConnectFailedException e)
        {
            throw new ExternalServiceException(String.format("Cannot connect to existing node '%s' at '%s'",
                                                             helperNodeName, helperHostPort), e);
        }
        catch (BinaryProtocol.ProtocolException e)
        {
            String message = String.format("Unexpected protocol exception '%s' encountered while retrieving state for node '%s' (%s) from group '%s'",
                                          e.getUnexpectedMessage(), helperNodeName, helperHostPort, groupName);
            LOGGER.warn(message, e);
            throw new ExternalServiceException(message, e) ;
        }
        catch (RuntimeException e)
        {
            throw new ExternalServiceException(String.format("Cannot retrieve state for node '%s' (%s) from group '%s'",
                    helperNodeName, helperHostPort, groupName), e);
        }
    }

    private void registerAppStateMonitorIfPermittedNodesSpecified(final Set<String> permittedNodes)
    {
        if (!permittedNodes.isEmpty())
        {
            byte[] data = permittedNodeListToBytes(permittedNodes);
            try
            {
                getEnvironment().registerAppStateMonitor(new EnvironmentStateHolder(data));
            }
            catch (RuntimeException e)
            {
                throw handleDatabaseException("Exception on registering app state monitor", e);
            }
        }
    }

    private boolean isNodePermitted(ReplicationNode replicationNode)
    {
        if (_permittedNodes.isEmpty())
        {
            return true;
        }

        String nodeHostPort = getHostPort(replicationNode);
        return _permittedNodes.contains(nodeHostPort);
    }

    private String getHostPort(ReplicationNode replicationNode)
    {
        return replicationNode.getHostName() + ":" + replicationNode.getPort();
    }


    private boolean onIntruder(ReplicationGroupListener replicationGroupListener, ReplicationNode replicationNode)
    {
        if (replicationGroupListener != null)
        {
            return replicationGroupListener.onIntruderNode(replicationNode);
        }
        else
        {
            LOGGER.warn(String.format(
                    "Found an intruder node '%s' from ''%s' . The node is not listed in permitted list: %s",
                    replicationNode.getName(),
                    getHostPort(replicationNode),
                    String.valueOf(_permittedNodes)));
            return true;
        }
    }

    private byte[] permittedNodeListToBytes(Set<String> permittedNodeList)
    {
        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put(PERMITTED_NODE_LIST, permittedNodeList);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            objectMapper.writeValue(baos, data);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unexpected exception on serializing of permitted node list into json", e);
        }
        return baos.toByteArray();
    }

    private void populateExistingRemoteReplicationNodes()
    {
        try
        {
            ReplicationGroup group = getEnvironment().getGroup();
            Set<ReplicationNode> nodes = new HashSet<>(group.getElectableNodes());
            String localNodeName = getNodeName();

            for (ReplicationNode replicationNode : nodes)
            {
                String discoveredNodeName = replicationNode.getName();
                if (!discoveredNodeName.equals(localNodeName))
                {
                    _remoteReplicationNodes.put(replicationNode.getName(), replicationNode);
                }
            }
        }
        catch (RuntimeException e)
        {
            // should never happen
            handleDatabaseException("Exception on discovery of existing nodes", e);
        }
    }

    private void notifyExistingRemoteReplicationNodes(ReplicationGroupListener listener)
    {
        for (ReplicationNode value : _remoteReplicationNodes.values())
        {
            listener.onReplicationNodeRecovered(value);
        }
    }

    private void notifyNodeRolledbackIfNecessary(ReplicationGroupListener listener)
    {
        if (_nodeRolledback)
        {
            listener.onNodeRolledback();
            _nodeRolledback = false;
        }
    }

    private void onException(final Exception e)
    {
        _groupChangeExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                ReplicationGroupListener listener = _replicationGroupListener.get();
                if (listener != null)
                {
                    listener.onException(e);
                }
            }
        });
    }

    private void handleUncaughtExceptionInExecutorService(Throwable e)
    {
        LOGGER.error("Unexpected exception", e);
        Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        if (uncaughtExceptionHandler != null)
        {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
        }
        else
        {
            // it should never happen as we set default UncaughtExceptionHandler in main
            e.printStackTrace();
            Runtime.getRuntime().halt(1);
        }
    }


    private class RemoteNodeStateLearner implements Callable<Void>
    {
        private static final long TIMEOUT_WARN_GAP = 1000 * 60 * 5;
        private final Map<ReplicationNode, Long> _currentlyTimedOutNodes = new HashMap<>();
        private Map<String, ReplicatedEnvironment.State> _previousGroupState = Collections.emptyMap();
        private boolean _previousDesignatedPrimary;
        private int _previousElectableGroupOverride;

        @Override
        public Void call()
        {
            boolean continueMonitoring = true;
            try
            {
                if (_state.get() == State.OPEN)
                {
                    try
                    {
                        continueMonitoring = detectGroupChangesAndNotify();
                    }
                    catch(RuntimeException e)
                    {
                        RuntimeException handledException = handleDatabaseException("Exception on replication group check", e);
                        LOGGER.debug("Non fatal exception on performing replication group check. Ignoring...", handledException);
                    }
                    if (continueMonitoring)
                    {

                        Map<ReplicationNode, NodeState> nodeStates = discoverNodeStates(_remoteReplicationNodes.values());

                        executeDatabasePingerOnNodeChangesIfMaster(nodeStates);

                        notifyGroupListenerAboutNodeStates(nodeStates);
                    }

                }
            }
            catch(Error e)
            {
                continueMonitoring = false;
                handleUncaughtExceptionInExecutorService(e);
            }
            catch (ServerScopedRuntimeException e)
            {
                State currentState = _state.get();
                if (currentState != State.CLOSING && currentState != State.CLOSED)
                {
                    continueMonitoring = false;
                    handleUncaughtExceptionInExecutorService(e);
                }
            }
            catch(RuntimeException e)
            {
                LOGGER.warn("Unexpected exception on discovering node states", e);
            }
            finally
            {
                State state = _state.get();
                if (state != State.CLOSED && state != State.CLOSING && continueMonitoring)
                {
                    _groupChangeExecutor.schedule(this, _remoteNodeMonitorInterval, TimeUnit.MILLISECONDS);
                }
                else
                {
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Monitoring task is not scheduled:  state " + state + ", continue monitoring flag " + continueMonitoring);
                    }
                }
            }
            return null;
        }

        private boolean detectGroupChangesAndNotify()
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Checking for changes in the group " + _configuration.getGroupName() + " on node " + _configuration.getName());
            }
            boolean shouldContinue = true;
            String groupName = _configuration.getGroupName();
            ReplicatedEnvironment env = _environment.get();
            ReplicationGroupListener replicationGroupListener = _replicationGroupListener.get();
            if (env != null)
            {
                ReplicationGroup group = env.getGroup();
                Set<ReplicationNode> nodes = new HashSet<>(group.getNodes());
                String localNodeName = getNodeName();

                int numberOfKnownRemoteNodes = _remoteReplicationNodes.size();
                int groupSize = nodes.size();

                Map<String, ReplicationNode> removalMap = new HashMap<>(_remoteReplicationNodes);
                for (ReplicationNode replicationNode : nodes)
                {
                    String discoveredNodeName = replicationNode.getName();
                    if (!discoveredNodeName.equals(localNodeName))
                    {
                        if (!_remoteReplicationNodes.containsKey(discoveredNodeName))
                        {
                            if (LOGGER.isDebugEnabled())
                            {
                                LOGGER.debug("Remote replication node added '" + replicationNode + "' to '" + groupName + "'");
                            }

                            _remoteReplicationNodes.put(discoveredNodeName, replicationNode);

                            if (isNodePermitted(replicationNode))
                            {
                                if (replicationGroupListener != null)
                                {
                                    replicationGroupListener.onReplicationNodeAddedToGroup(replicationNode);
                                }
                            }
                            else
                            {
                                if (!onIntruder(replicationGroupListener, replicationNode))
                                {
                                    shouldContinue = false;
                                }
                            }
                        }
                        else
                        {
                            removalMap.remove(discoveredNodeName);
                        }
                    }
                }

                if (!removalMap.isEmpty())
                {
                    for (Map.Entry<String, ReplicationNode> replicationNodeEntry : removalMap.entrySet())
                    {
                        String replicationNodeName = replicationNodeEntry.getKey();
                        if (LOGGER.isDebugEnabled())
                        {
                            LOGGER.debug("Remote replication node removed '" + replicationNodeName + "' from '" + groupName + "'");
                        }
                        _remoteReplicationNodes.remove(replicationNodeName);
                        if (replicationGroupListener != null)
                        {
                            replicationGroupListener.onReplicationNodeRemovedFromGroup(replicationNodeEntry.getValue());
                        }
                    }
                }

                if (shouldContinue && numberOfKnownRemoteNodes + 1 != groupSize)
                {
                    int newPoolSize = groupSize + 1;
                    LOGGER.debug("Setting group change executor core pool size to {}", newPoolSize);
                    _groupChangeExecutor.setCorePoolSize(newPoolSize);
                }
            }
            return shouldContinue;
        }

        private Map<ReplicationNode, NodeState> discoverNodeStates(Collection<ReplicationNode> electableNodes)
        {
            final Map<ReplicationNode, NodeState> nodeStates = new HashMap<ReplicationNode, NodeState>();
            Map<ReplicationNode, Future<Void>> futureMap = new HashMap<ReplicationNode, Future<Void>>();

            for (final ReplicationNode node : electableNodes)
            {
                nodeStates.put(node, null);

                Future<Void> future = _groupChangeExecutor.submit(new Callable<Void>()
                {
                    @Override
                    public Void call()
                    {
                        NodeState nodeStateObject = null;
                        try
                        {
                            nodeStateObject = getRemoteNodeState(_configuration.getGroupName(), node, _dbPingSocketTimeout);
                        }
                        catch (IOException | ServiceConnectFailedException | com.sleepycat.je.rep.utilint.BinaryProtocol.ProtocolException e )
                        {
                            // Cannot discover node states. The node state should be treated as UNKNOWN
                        }

                        nodeStates.put(node, nodeStateObject);
                        return null;
                    }
                });
                futureMap.put(node, future);
            }

            boolean atLeastOneNodeTimesOut = false;

            for (Map.Entry<ReplicationNode, Future<Void>> entry : futureMap.entrySet())
            {
                ReplicationNode node = entry.getKey();
                String nodeName = node.getName();
                Future<Void> future = entry.getValue();
                try
                {
                    future.get(_remoteNodeMonitorTimeout, TimeUnit.MILLISECONDS);
                    if (_currentlyTimedOutNodes.remove(node) != null)
                    {
                        LOGGER.warn("Node '" + nodeName + "' from group " + _configuration.getGroupName()
                                    + " is responding again.");
                    }
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
                catch (ExecutionException e)
                {
                    Throwable cause = e.getCause();
                    LOGGER.warn("Cannot determine state for node '" + nodeName + "' from group "
                            + _configuration.getGroupName(), cause);

                    if (cause instanceof Error)
                    {
                        throw (Error) cause;
                    }
                    else  if (cause instanceof RuntimeException)
                    {
                        throw (RuntimeException) cause;
                    }
                    else
                    {
                        throw new RuntimeException("Unexpected exception", cause);
                    }
                }
                catch (TimeoutException e)
                {
                    atLeastOneNodeTimesOut = true;
                    if (! _currentlyTimedOutNodes.containsKey(node))
                    {
                        LOGGER.warn("Timeout whilst determining state for node '" + nodeName + "' from group "
                                    + _configuration.getGroupName());
                        _currentlyTimedOutNodes.put(node, System.currentTimeMillis());
                    }
                    else if (_currentlyTimedOutNodes.get(node) > (System.currentTimeMillis() + TIMEOUT_WARN_GAP))
                    {
                        LOGGER.warn("Node '" + nodeName + "' from group "
                                    + _configuration.getGroupName()
                                    + " is still timing out.");
                        _currentlyTimedOutNodes.put(node, System.currentTimeMillis());
                    }

                    future.cancel(true);
                }
            }

            if (!atLeastOneNodeTimesOut)
            {
                _currentlyTimedOutNodes.clear();
            }
            return nodeStates;
        }

        /**
         * If the state of the group changes or the user alters the parameters used to determine if the
         * there  is quorum in the group, execute a single small transaction to discover is quorum is
         * still available.  This allows us to discover if quorum is lost in a timely manner, rather than
         * having to await the next user transaction.
         */
        private void executeDatabasePingerOnNodeChangesIfMaster(final Map<ReplicationNode, NodeState> nodeStates)
        {
            try
            {
                if (ReplicatedEnvironment.State.MASTER == getEnvironment().getState())
                {
                    Map<String, ReplicatedEnvironment.State> currentGroupState = new HashMap<>();
                    for (Map.Entry<ReplicationNode, NodeState> entry : nodeStates.entrySet())
                    {
                        ReplicationNode node = entry.getKey();
                        NodeState nodeState = entry.getValue();
                        ReplicatedEnvironment.State state = nodeState == null ? ReplicatedEnvironment.State.UNKNOWN : nodeState.getNodeState();
                        currentGroupState.put(node.getName(), state);
                    }

                    boolean currentDesignatedPrimary = ReplicatedEnvironmentFacade.this.isDesignatedPrimary();
                    int currentElectableGroupSizeOverride = ReplicatedEnvironmentFacade.this.getElectableGroupSizeOverride();

                    boolean stateChanged = !_previousGroupState.equals(currentGroupState)
                            || currentDesignatedPrimary != _previousDesignatedPrimary
                            || currentElectableGroupSizeOverride != _previousElectableGroupOverride;

                    _previousGroupState = currentGroupState;
                    _previousDesignatedPrimary = currentDesignatedPrimary;
                    _previousElectableGroupOverride = currentElectableGroupSizeOverride;

                    if (stateChanged && State.OPEN == _state.get())
                    {
                        new DatabasePinger().pingDb(ReplicatedEnvironmentFacade.this);
                    }
                }
            }
            catch(RuntimeException e)
            {
                Exception handledException = handleDatabaseException("Exception on master check", e);
                LOGGER.debug("Non fatal exception on performing ping. Ignoring...", handledException);
            }
        }

        private void notifyGroupListenerAboutNodeStates(final Map<ReplicationNode, NodeState> nodeStates)
        {
            ReplicationGroupListener replicationGroupListener = _replicationGroupListener.get();
            if (replicationGroupListener != null)
            {
                for (Map.Entry<ReplicationNode, NodeState> entry : nodeStates.entrySet())
                {
                    replicationGroupListener.onNodeState(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    public static enum State
    {
        OPENING,
        OPEN,
        RESTARTING,
        CLOSING,
        CLOSED
    }

    private static class EnvironmentStateHolder implements AppStateMonitor
    {
        private byte[] _data;

        private EnvironmentStateHolder(byte[] data)
        {
            this._data = data;
        }

        @Override
        public byte[] getAppState()
        {
            return _data;
        }
    }

    public static class ReplicationNodeImpl implements ReplicationNode
    {

        private final InetSocketAddress _address;
        private final String _nodeName;
        private final String _host;
        private final int _port;

        public ReplicationNodeImpl(String nodeName, String hostPort)
        {
            String[] tokens = hostPort.split(":");
            if (tokens.length != 2)
            {
                throw new IllegalArgumentException("Unexpected host port value :" + hostPort);
            }
            _host = tokens[0];
            _port = Integer.parseInt(tokens[1]);
            _nodeName = nodeName;
            _address = new InetSocketAddress(_host, _port);
        }

        @Override
        public String getName()
        {
            return _nodeName;
        }

        @Override
        public NodeType getType()
        {
            return NodeType.ELECTABLE;
        }

        @Override
        public InetSocketAddress getSocketAddress()
        {
            return _address;
        }

        @Override
        public String getHostName()
        {
            return _host;
        }

        @Override
        public int getPort()
        {
            return _port;
        }

        @Override
        public String toString()
        {
            return "ReplicationNodeImpl{" +
                    "_nodeName='" + _nodeName + '\'' +
                    ", _host='" + _host + '\'' +
                    ", _port=" + _port +
                    '}';
        }
    }

    private class ExceptionListener implements com.sleepycat.je.ExceptionListener
    {
        @Override
        public void exceptionThrown(final ExceptionEvent event)
        {
            Exception exception = event.getException();

            if (exception instanceof LogWriteException)
            {
                // TODO: calling handleUncaughtExceptionInExecutorService() looks more attractive then delegating broker close to VHN
                onException(exception);
            }

            if (exception instanceof RollbackException)
            {
                // Usually caused use of weak durability options: node priority zero,
                // designated primary, electable group override.
                RollbackException re = (RollbackException) exception;

                LOGGER.warn(_prettyGroupNodeName + " has transaction(s) ahead of the current master. These"
                            + " must be discarded to allow this node to rejoin the group."
                           + " This condition is normally caused by the use of weak durability options.");
                _nodeRolledback = true;
                tryToRestartEnvironment(re);
            }
            else
            {
                LOGGER.error("Asynchronous exception thrown by BDB thread '" + event.getThreadName() + "'", event.getException());
            }
        }
    }
}
