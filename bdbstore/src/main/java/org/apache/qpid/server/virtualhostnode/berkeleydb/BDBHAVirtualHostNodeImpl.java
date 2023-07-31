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
package org.apache.qpid.server.virtualhostnode.berkeleydb;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LogWriteException;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.Outcome;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.logging.messages.ConfigStoreMessages;
import org.apache.qpid.server.logging.messages.HighAvailabilityMessages;
import org.apache.qpid.server.logging.subjects.BDBHAVirtualHostNodeLogSubject;
import org.apache.qpid.server.logging.subjects.GroupLogSubject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.RemoteReplicationNode;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.VirtualHostStoreUpgraderAndRecoverer;
import org.apache.qpid.server.store.berkeleydb.BDBCacheSizeSetter;
import org.apache.qpid.server.store.berkeleydb.BDBConfigurationStore;
import org.apache.qpid.server.store.berkeleydb.EnvironmentFacade;
import org.apache.qpid.server.store.berkeleydb.replication.ReplicatedEnvironmentFacade;
import org.apache.qpid.server.store.berkeleydb.replication.ReplicatedEnvironmentFacadeFactory;
import org.apache.qpid.server.store.berkeleydb.replication.ReplicationGroupListener;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreAttributes;
import org.apache.qpid.server.store.preferences.ProvidedPreferenceStoreFactoryService;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.PortUtil;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBHAVirtualHostImpl;
import org.apache.qpid.server.virtualhostnode.AbstractVirtualHostNode;

@ManagedObject( category = false, type = BDBHAVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE,
        validChildTypes = "org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNodeImpl#getSupportedChildTypes()" )
public class BDBHAVirtualHostNodeImpl extends AbstractVirtualHostNode<BDBHAVirtualHostNodeImpl> implements
        BDBHAVirtualHostNode<BDBHAVirtualHostNodeImpl>
{
    public static final String VIRTUAL_HOST_NODE_TYPE = "BDB_HA";
    public static final String VIRTUAL_HOST_PRINCIPAL_NAME_FORMAT = "grp(/{0})/vhn(/{1})";

    /**
     * Length of time we synchronously await the a JE mutation to complete.  It is not considered an error if we exceed this timeout, although a
     * a warning will be logged.
     */
    static final int MUTATE_JE_TIMEOUT_MS = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(BDBHAVirtualHostNodeImpl.class);

    private final AtomicReference<ReplicatedEnvironmentFacade> _environmentFacade = new AtomicReference<>();

    private final AtomicReference<NodeRole> _lastRole = new AtomicReference<>(NodeRole.DETACHED);
    private final SystemConfig _systemConfig;
    private BDBHAVirtualHostNodeLogSubject _virtualHostNodeLogSubject;
    private GroupLogSubject _groupLogSubject;
    private String _virtualHostNodePrincipalName;

    @ManagedAttributeField
    private String _storePath;

    @ManagedAttributeField
    private String _groupName;

    @ManagedAttributeField
    private String _helperAddress;

    @ManagedAttributeField
    private String _address;

    @ManagedAttributeField(afterSet="postSetDesignatedPrimary")
    private boolean _designatedPrimary;

    @ManagedAttributeField(afterSet="postSetPriority")
    private int _priority;

    @ManagedAttributeField(afterSet="postSetQuorumOverride")
    private int _quorumOverride;

    @ManagedAttributeField(afterSet="postSetRole")
    private NodeRole _role;

    @ManagedAttributeField
    private String _helperNodeName;

    @ManagedAttributeField(afterSet = "postSetPermittedNodes")
    private List<String> _permittedNodes;

    private volatile boolean _isClosedOrDeleted;

    @ManagedObjectFactoryConstructor(conditionallyAvailable = true, condition = "org.apache.qpid.server.JECheck#isAvailable()")
    public BDBHAVirtualHostNodeImpl(Map<String, Object> attributes, Broker<?> broker)
    {
        super(broker, attributes);
        _systemConfig = (SystemConfig) broker.getParent();
        addChangeListener(new BDBCacheSizeSetter());
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        BDBHAVirtualHostNode<?> proposed = (BDBHAVirtualHostNode<?>)proxyForValidation;

        if (changedAttributes.contains(ROLE))
        {
            NodeRole currentRole = getRole();
            if (NodeRole.REPLICA != currentRole)
            {
                throw new IllegalStateException("Cannot transfer mastership when not a " + NodeRole.REPLICA + ", current role is " + currentRole);
            }
            if (NodeRole.MASTER != proposed.getAttribute(ROLE))
            {
                throw new IllegalArgumentException("Changing role to other value then " + NodeRole.MASTER + " is unsupported");
            }
        }

        if (changedAttributes.contains(PERMITTED_NODES))
        {
            validatePermittedNodes(proposed.getPermittedNodes());
        }
    }

    @Override
    public String getStorePath()
    {
        return _storePath;
    }

    @Override
    public String getGroupName()
    {
        return _groupName;
    }

    @Override
    public String getHelperAddress()
    {
        return _helperAddress;
    }

    @Override
    public String getAddress()
    {
        return _address;
    }

    @Override
    public boolean isDesignatedPrimary()
    {
        return _designatedPrimary;
    }

    @Override
    public int getPriority()
    {
        return _priority;
    }

    @Override
    public int getQuorumOverride()
    {
        return _quorumOverride;
    }

    @Override
    public NodeRole getRole()
    {
        return _lastRole.get();
    }

    @Override
    public Long getLastKnownReplicationTransactionId()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            return environmentFacade.getLastKnownReplicationTransactionId();
        }
        return -1L;
    }

    @Override
    public Long getJoinTime()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            return environmentFacade.getJoinTime();
        }
        return -1L;
    }

    @Override
    public String getHelperNodeName()
    {
        return _helperNodeName;
    }

    @Override
    public List<String> getPermittedNodes()
    {
        return _permittedNodes;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Collection<? extends RemoteReplicationNode> getRemoteReplicationNodes()
    {
        Collection<RemoteReplicationNode> remoteNodes = getChildren(RemoteReplicationNode.class);
        return (Collection<? extends RemoteReplicationNode>)remoteNodes;
    }

    @Override
    public String toString()
    {
        return "BDBHAVirtualHostNodeImpl [id=" + getId() + ", name=" + getName() + ", storePath=" + _storePath + ", groupName=" + _groupName + ", address=" + _address
                + ", state=" + getState() + ", priority=" + _priority + ", designatedPrimary=" + _designatedPrimary + ", quorumOverride=" + _quorumOverride + ", role=" + getRole() + "]";
    }

    @Override
    public BDBConfigurationStore getConfigurationStore()
    {
        return (BDBConfigurationStore) super.getConfigurationStore();
    }

    @Override
    public void onCreate()
    {
        super.onCreate();
        if (!isFirstNodeInAGroup())
        {
            List<String> permittedNodes = new ArrayList<>(getPermittedNodesFromHelper());
            setAttributes(Collections.singletonMap(PERMITTED_NODES, permittedNodes));
        }
    }

    @Override
    public void onOpen()
    {
        validatePermittedNodesFormat(_permittedNodes);
        super.onOpen();
    }

    protected ReplicatedEnvironmentFacade getReplicatedEnvironmentFacade()
    {
        return _environmentFacade.get();
    }

    @Override
    protected DurableConfigurationStore createConfigurationStore()
    {
        return new BDBConfigurationStore(new ReplicatedEnvironmentFacadeFactory(), VirtualHost.class);
    }

    @Override
    protected ListenableFuture<Void> activate()
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Activating virtualhost node " + this);
        }

        // activating the environment does not cause a state change.  Adjust the role
        // so that our observers will see WAITING rather than our previous role in the group.
        // the role will change again after the election at which point it will become master or replica.
        _lastRole.set(NodeRole.WAITING);
        attributeSet(ROLE, _role, NodeRole.WAITING);

        getConfigurationStore().init(this);

        getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.CREATED());
        getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.STORE_LOCATION(getStorePath()));

        ReplicatedEnvironmentFacade environmentFacade = (ReplicatedEnvironmentFacade) getConfigurationStore().getEnvironmentFacade();
        if (environmentFacade == null)
        {
            throw new IllegalStateException("Environment facade is not created");
        }

        try
        {
            Set<ReplicationNode> remoteNodes = environmentFacade.getNodes();
            for (ReplicationNode node : remoteNodes)
            {
                String nodeAddress = node.getHostName() + ":" + node.getPort();
                if (!_permittedNodes.contains(nodeAddress))
                {
                    getEventLogger().message(getGroupLogSubject(), HighAvailabilityMessages.INTRUDER_DETECTED(node.getName(), nodeAddress));
                    shutdownOnIntruder(nodeAddress);

                    throw new IllegalStateException("Intruder node detected: " + nodeAddress);
                }
            }
        }
        catch (DatabaseException dbe)
        {
            environmentFacade.handleDatabaseException("DB exception while checking for intruder node", dbe);
        }

        if (_environmentFacade.compareAndSet(null, environmentFacade))
        {
            environmentFacade.setStateChangeListener(new EnvironmentStateChangeListener());
            environmentFacade.setReplicationGroupListener(new RemoteNodesDiscoverer());
            environmentFacade.setPermittedNodes(_permittedNodes);
        }

        return Futures.immediateFuture(null);
    }

    @Override
    @StateTransition( currentState = { State.UNINITIALIZED, State.ACTIVE, State.ERRORED }, desiredState = State.STOPPED )
    protected ListenableFuture<Void> doStop()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();

        ListenableFuture<Void> superFuture = super.doStop();
        addFutureCallback(superFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(final Void result)
            {
                doFinally();
            }

            @Override
            public void onFailure(final Throwable t)
            {
                doFinally();
            }

            private void doFinally()
            {
                try
                {
                    closeEnvironment();

                    // closing the environment does not cause a state change.  Adjust the role
                    // so that our observers will see DETACHED rather than our previous role in the group.
                    _lastRole.set(NodeRole.DETACHED);
                    attributeSet(ROLE, _role, NodeRole.DETACHED);
                }
                finally
                {
                    returnVal.set(null);
                }
            }
        }, getTaskExecutor());
        return returnVal;
    }

    private void closeEnvironment()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null && _environmentFacade.compareAndSet(environmentFacade, null))
        {
            try
            {
                environmentFacade.close();
            }
            catch (RuntimeException e)
            {
                throw new StoreException("Exception occurred on environment facade close", e);
            }
        }
    }

    @Override
    protected ListenableFuture<Void> beforeDelete()
    {
        _isClosedOrDeleted = true;
        return super.beforeDelete();
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        final Set<InetSocketAddress> helpers = getRemoteNodeAddresses();

        return doAfterAlways(closeVirtualHostIfExists(),
                             () -> {

                                 closeEnvironment();

                                 DurableConfigurationStore configurationStore = getConfigurationStore();
                                 if (configurationStore != null)
                                 {
                                     configurationStore.closeConfigurationStore();
                                     configurationStore.onDelete(BDBHAVirtualHostNodeImpl.this);
                                     getEventLogger().message(getVirtualHostNodeLogSubject(),
                                                              HighAvailabilityMessages.DELETE(getName(),
                                                                                              String.valueOf(Outcome.SUCCESS)));
                                 }

                                 if (!helpers.isEmpty())
                                 {
                                     try
                                     {
                                         new ReplicationGroupAdmin(_groupName, helpers).removeMember(getName());
                                     }
                                     catch(DatabaseException e)
                                     {
                                         LOGGER.warn(String.format(
                                                 "The deletion of node %s on remote nodes failed due to: %s. To finish deletion a "
                                                 + "removal of the node from any of remote nodes (%s) is required.",
                                                 this, e.getMessage(), helpers));
                                     }
                                 }
                                 onCloseOrDelete();
                             });
    }

    private Set<InetSocketAddress> getRemoteNodeAddresses()
    {
        Set<InetSocketAddress> helpers = new HashSet<>();
        @SuppressWarnings("rawtypes")
        Collection<? extends RemoteReplicationNode> remoteNodes = getRemoteReplicationNodes();
        for (RemoteReplicationNode<?> node : remoteNodes)
        {
            BDBHARemoteReplicationNode<?> bdbHaRemoteReplicationNode = (BDBHARemoteReplicationNode<?>)node;
            String remoteNodeAddress = bdbHaRemoteReplicationNode.getAddress();
            helpers.add(HostPortPair.getSocket(remoteNodeAddress));
        }
        return helpers;
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        return doAfterAlways(super.onClose(), this::closeEnvironment);
    }

    @Override
    protected void postResolve()
    {
        super.postResolve();
        _virtualHostNodeLogSubject =  new BDBHAVirtualHostNodeLogSubject(getGroupName(), getName());
        _groupLogSubject = new GroupLogSubject(getGroupName());
        _virtualHostNodePrincipalName = MessageFormat.format(VIRTUAL_HOST_PRINCIPAL_NAME_FORMAT, getGroupName(), getName());
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();

        validateAddress();

        validateStorePath();

        if (!isFirstNodeInAGroup())
        {
            getPermittedNodesFromHelper();
        }
    }

    @Override
    public void onValidate()
    {
        super.onValidate();

        PreferenceStoreAttributes preferenceStoreAttributes = getPreferenceStoreAttributes();
        if (!preferenceStoreAttributes.getType().equals(ProvidedPreferenceStoreFactoryService.TYPE))
        {
            throw new IllegalConfigurationException(String.format(
                    "BDBHAVirtualHostNode only supports Provided preference store but configured '%s'", preferenceStoreAttributes.getType()));
        }
    }

    private Collection<String> getPermittedNodesFromHelper()
    {
        int dbPingSocketTimeout = getContextKeys(false).contains("qpid.bdb.ha.db_ping_socket_timeout") ? getContextValue(Integer.class, "qpid.bdb.ha.db_ping_socket_timeout") : 10000 /* JE's own default */;
        return ReplicatedEnvironmentFacade.connectToHelperNodeAndCheckPermittedHosts(getName(), getAddress(), getGroupName(), getHelperNodeName(), getHelperAddress(), dbPingSocketTimeout);
    }

    private void validateStorePath()
    {
        File storePath = new File(getStorePath());
        while (!storePath.exists())
        {
            storePath = storePath.getParentFile();
            if (storePath == null)
            {
                throw new IllegalConfigurationException(String.format("Store path '%s' is invalid", getStorePath()));
            }
        }

        if (!storePath.isDirectory())
        {
            throw new IllegalConfigurationException(String.format("Store path '%s' is not a folder", getStorePath()));
        }

        if (!storePath.canWrite())
        {
            throw new IllegalConfigurationException(String.format("Store path '%s' is not writable", getStorePath()));
        }

    }

    private void validateAddress()
    {
        String address = getAddress();

        URI uri = addressToURI(address);

        if (!PortUtil.isPortAvailable(uri.getHost(), uri.getPort()))
        {
            throw new IllegalConfigurationException(String.format("Cannot bind to address '%s'. Address is already in use.", address));
        }
    }

    private URI addressToURI(String address)
    {
        if (address == null || "".equals(address))
        {
            throw new IllegalConfigurationException("Node address is not set");
        }

        URI uri = null;
        try
        {
            uri = new URI( "tcp://" + address);
        }
        catch (URISyntaxException e)
        {
            throw new IllegalConfigurationException(String.format("Invalid address specified '%s'. ", address));
        }
        return uri;
    }

    private void onMaster()
    {
        boolean success = false;
        try
        {
            boolean firstOpening = false;
            closeVirtualHostIfExist().get();
            getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.RECOVERY_START());
            VirtualHostStoreUpgraderAndRecoverer upgraderAndRecoverer = new VirtualHostStoreUpgraderAndRecoverer(this);
            if(getConfigurationStore().isOpen())
            {
                upgraderAndRecoverer.reloadAndRecover(getConfigurationStore());
            }
            else
            {
                getConfigurationStore().upgradeStoreStructure();
                ConfiguredObjectRecord[] initialRecords = getInitialRecords();
                if(upgraderAndRecoverer.upgradeAndRecover(getConfigurationStore(), initialRecords))
                {
                    setAttributes(Collections.singletonMap(VIRTUALHOST_INITIAL_CONFIGURATION, "{}"));
                    firstOpening = initialRecords.length == 0;
                }

            }

            getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.RECOVERY_COMPLETE());

            VirtualHost<?> host = getVirtualHost();


            if (host == null)
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Creating new virtualhost with name : " + getGroupName());
                }
                Map<String, Object> hostAttributes = new HashMap<>();

                hostAttributes.put(VirtualHost.MODEL_VERSION, BrokerModel.MODEL_VERSION);
                hostAttributes.put(VirtualHost.NAME, getGroupName());
                hostAttributes.put(VirtualHost.TYPE, BDBHAVirtualHostImpl.VIRTUAL_HOST_TYPE);
                createChild(VirtualHost.class, hostAttributes);

            }
            else
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Recovered virtualhost with name : " +  getGroupName());
                }

                final VirtualHost<?> recoveredHost = host;
                // Since we are the master, the host should always be a proper (queue managing) virtual host
                // so the following test should always return true
                if(recoveredHost instanceof QueueManagingVirtualHost)
                {
                    ((QueueManagingVirtualHost<?>) recoveredHost).setFirstOpening(firstOpening);
                }
                Subject.doAs(getSubjectWithAddedSystemRights(), (PrivilegedAction<Object>) () ->
                {
                    recoveredHost.open();
                    return null;
                });
            }
            success = true;

        }
        catch (Exception e)
        {
            LOGGER.error("Failed to activate on hearing MASTER change event", e);
        }
        finally
        {
            setState(success ? State.ACTIVE : State.ERRORED);
        }
    }

    private void onReplica()
    {
        boolean success = false;
        try
        {
            createReplicaVirtualHost();
            success = true;
        }
        finally
        {
            setState(success ? State.ACTIVE : State.ERRORED);
        }
    }

    private void createReplicaVirtualHost()
    {
        try
        {
            closeVirtualHostIfExist().get();
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
            else  if (cause instanceof ServerScopedRuntimeException)
            {
                throw (ServerScopedRuntimeException) cause;
            }
            else
            {
                // we do not want to kill the broker on VH close failures
                LOGGER.error("Unexpected exception on virtual host close", cause);
            }
        }

        Map<String, Object> hostAttributes = new HashMap<>();
        hostAttributes.put(VirtualHost.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        hostAttributes.put(VirtualHost.NAME, getGroupName());
        hostAttributes.put(VirtualHost.TYPE, "BDB_HA_REPLICA");
        hostAttributes.put(VirtualHost.DURABLE, false);
        createChild(VirtualHost.class, hostAttributes);
    }

    protected ListenableFuture<Void> closeVirtualHostIfExist()
    {
        final VirtualHost<?> virtualHost = getVirtualHost();
        if (virtualHost != null)
        {
            return virtualHost.closeAsync();
        }
        else
        {
            return Futures.immediateFuture(null);
        }
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        _isClosedOrDeleted = true;
        return super.beforeClose();
    }

    @Override
    public void setBDBCacheSize(long cacheSize)
    {
        EnvironmentFacade environmentFacade = _environmentFacade.get();
        if (environmentFacade != null)
        {
            environmentFacade.setCacheSize(cacheSize);
        }
    }

    @Override
    public PreferenceStore getPreferenceStore()
    {
        return getConfigurationStore().getPreferenceStore();
    }

    private class EnvironmentStateChangeListener implements StateChangeListener
    {
        @Override
        public void stateChange(StateChangeEvent stateChangeEvent) throws RuntimeException
        {
            if (_isClosedOrDeleted)
            {
                LOGGER.debug("Ignoring state transition into state {} because VHN is already closed or closing", stateChangeEvent.getState());
            }
            else
            {
                try
                {
                    doStateChange(stateChangeEvent);
                }
                catch (RuntimeException e)
                {
                    // Ignore exception when VHN is closed or closing
                    if (!_isClosedOrDeleted)
                    {
                        throw e;
                    }
                    else
                    {
                        LOGGER.debug("Ignoring any runtime exception thrown on state transition after node close", e);
                    }
                }
            }
        }

        private void doStateChange(StateChangeEvent stateChangeEvent)
        {
            com.sleepycat.je.rep.ReplicatedEnvironment.State state = stateChangeEvent.getState();
            NodeRole previousRole = getRole();

            LOGGER.info("Received BDB event indicating transition from state {} to {} for {}", previousRole, state, getName());

            try
            {
                switch (state)
                {
                    case MASTER:
                        onMaster();
                        break;
                    case REPLICA:
                        onReplica();
                        break;
                    case DETACHED:
                        closeVirtualHostIfExist().get();
                        break;
                    case UNKNOWN:
                        closeVirtualHostIfExist().get();
                        break;
                    default:
                        LOGGER.error("Unexpected state change: " + state);
                }
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new ServerScopedRuntimeException(e);
            }
            finally
            {
                NodeRole newRole = NodeRole.fromJeState(state);
                _lastRole.set(newRole);
                attributeSet(ROLE, _role, newRole);
                getEventLogger().message(getGroupLogSubject(),
                        HighAvailabilityMessages.ROLE_CHANGED(getName(), getAddress(), previousRole.name(), newRole.name()));
            }
        }
    }

    // used as post action by field _priority
    @SuppressWarnings("unused")
    private void postSetPriority()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            resolveFuture(environmentFacade.reapplyPriority(),
                    "Change node priority did not complete within " + MUTATE_JE_TIMEOUT_MS + "ms. New value " + _priority + " will become effective once the JE task thread is free.",
                    "Failed to set priority node to value " + _priority + " on " + this);
            getEventLogger().message(getVirtualHostNodeLogSubject(),
                    HighAvailabilityMessages.PRIORITY_CHANGED(String.valueOf(_priority)));
        }
    }

    // used as post action by field _designatedPrimary
    @SuppressWarnings("unused")
    private void postSetDesignatedPrimary()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            resolveFuture(environmentFacade.reapplyDesignatedPrimary(),
                    "Change designated primary did not complete within " + MUTATE_JE_TIMEOUT_MS + "ms. New value " + _designatedPrimary + " will become effective once the JE task thread is free.",
                    "Failed to set designated primary to value " + _designatedPrimary + " on " + this);
            getEventLogger().message(getVirtualHostNodeLogSubject(),
                    HighAvailabilityMessages.DESIGNATED_PRIMARY_CHANGED(String.valueOf(_designatedPrimary)));
        }
    }

    // used as post action by field _quorumOverride
    @SuppressWarnings("unused")
    private void postSetQuorumOverride()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            resolveFuture(environmentFacade.reapplyElectableGroupSizeOverride(),
                    "Change quorum override did not complete within " + MUTATE_JE_TIMEOUT_MS + "ms. New value " + _quorumOverride + " will become effective once the JE task thread is free.",
                    "Failed to set quorum override to value " + _quorumOverride + " on " + this);
            getEventLogger().message(getVirtualHostNodeLogSubject(),
                    HighAvailabilityMessages.QUORUM_OVERRIDE_CHANGED(String.valueOf(_quorumOverride)));
        }
    }

    // used as post action by field _role
    @SuppressWarnings("unused")
    private void postSetRole()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            getEventLogger().message(getGroupLogSubject(), HighAvailabilityMessages.TRANSFER_MASTER(getName(), getAddress()));
            resolveFuture(environmentFacade.transferMasterToSelfAsynchronously(),
                    "Transfer master did not complete within " + MUTATE_JE_TIMEOUT_MS + "ms. Node may still be elected master at a later time.",
                    "Failed to transfer master to " + this);
        }
        else
        {
            // Ignored
        }
    }

    private void resolveFuture(Future future, String timeoutLogMessage, String exceptionMessage)
    {
        try
        {
            future.get(MUTATE_JE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            LOGGER.warn(timeoutLogMessage);
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
            else  if (cause instanceof ServerScopedRuntimeException)
            {
                throw (ServerScopedRuntimeException) cause;
            }
            else
            {
                throw new ConnectionScopedRuntimeException(exceptionMessage, cause);
            }
        }
    }

    private boolean isFirstNodeInAGroup()
    {
        return getHelperNodeName() == null;
    }

    BDBHAVirtualHostNodeLogSubject getVirtualHostNodeLogSubject()
    {
        return _virtualHostNodeLogSubject;
    }

    GroupLogSubject getGroupLogSubject()
    {
        return _groupLogSubject;
    }

    @Override
    protected ConfiguredObjectRecord enrichInitialVirtualHostRootRecord(final ConfiguredObjectRecord vhostRecord)
    {
        Map<String,Object> hostAttributes = new LinkedHashMap<>(vhostRecord.getAttributes());
        hostAttributes.put(VirtualHost.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        hostAttributes.put(VirtualHost.NAME, getGroupName());
        hostAttributes.put(VirtualHost.TYPE, BDBHAVirtualHostImpl.VIRTUAL_HOST_TYPE);
        return new ConfiguredObjectRecordImpl(vhostRecord.getId(), vhostRecord.getType(),
                                              hostAttributes, vhostRecord.getParents());
    }

    protected void postSetPermittedNodes()
    {
        ReplicatedEnvironmentFacade replicatedEnvironmentFacade = getReplicatedEnvironmentFacade();
        if (replicatedEnvironmentFacade != null)
        {
            replicatedEnvironmentFacade.setPermittedNodes(_permittedNodes);
        }
    }

    private void validatePermittedNodes(Collection<String> proposedPermittedNodes)
    {
        if (getRemoteReplicationNodes().size() > 0 && getRole() != NodeRole.MASTER && !(getState() == State.STOPPED || getState() == State.ERRORED))
        {
            throw new IllegalArgumentException(String.format("Attribute '%s' can only be set on '%s' node or node in '%s' or '%s' state", PERMITTED_NODES, NodeRole.MASTER, State.STOPPED, State.ERRORED));
        }
        else if (proposedPermittedNodes == null || proposedPermittedNodes.isEmpty())
        {
            throw new IllegalArgumentException(String.format("Attribute '%s' is mandatory and must be set", PERMITTED_NODES));
        }

        if (_permittedNodes != null)
        {
            String missingNodeAddress = null;

            if (_permittedNodes.contains(getAddress()) && !proposedPermittedNodes.contains(getAddress()))
            {
                missingNodeAddress = getAddress();
            }
            else
            {
                for (final RemoteReplicationNode<?> node : getRemoteReplicationNodes())
                {
                    final BDBHARemoteReplicationNode<?> bdbHaRemoteReplicationNode = (BDBHARemoteReplicationNode<?>) node;
                    final String remoteNodeAddress = bdbHaRemoteReplicationNode.getAddress();
                    if (_permittedNodes.contains(remoteNodeAddress) && !proposedPermittedNodes.contains(remoteNodeAddress))
                    {
                        missingNodeAddress = remoteNodeAddress;
                        break;
                    }
                }
            }

            if (missingNodeAddress != null)
            {
                throw new IllegalArgumentException(String.format("The current group node '%s' cannot be removed from '%s' as its already a group member", missingNodeAddress, PERMITTED_NODES));
            }
        }

        validatePermittedNodesFormat(proposedPermittedNodes);
    }

    private void validatePermittedNodesFormat(Collection<String> permittedNodes)
    {
        if (permittedNodes == null || permittedNodes.isEmpty())
        {
            throw new IllegalConfigurationException("Permitted nodes are not set");
        }

        for (String permittedNode: permittedNodes)
        {
            addressToURI(permittedNode);
        }
    }

    private class RemoteNodesDiscoverer implements ReplicationGroupListener
    {
        @Override
        public void onReplicationNodeAddedToGroup(final ReplicationNode node)
        {
            getTaskExecutor().submit(new VirtualHostNodeGroupTask(node)
            {
                @Override
                public String getAction()
                {
                    return "remote node added";
                }

                @Override
                public void perform()
                {
                    addRemoteReplicationNode(node);
                }
            });
        }

        private void addRemoteReplicationNode(ReplicationNode node)
        {
            BDBHARemoteReplicationNodeImpl remoteNode = new BDBHARemoteReplicationNodeImpl(BDBHAVirtualHostNodeImpl.this, nodeToAttributes(node), getReplicatedEnvironmentFacade());
            remoteNode.create();
            childAdded(remoteNode);
            getEventLogger().message(getGroupLogSubject(), HighAvailabilityMessages.ADDED(remoteNode.getName(), remoteNode.getAddress()));
        }

        @Override
        public void onReplicationNodeRecovered(final ReplicationNode node)
        {
            getTaskExecutor().submit(new VirtualHostNodeGroupTask(node)
            {
                @Override
                public String getAction()
                {
                    return "remote node recovered";
                }

                @Override
                public void perform()
                {
                    recoverRemoteReplicationNode(node);
                }
            });
        }

        private void recoverRemoteReplicationNode(ReplicationNode node)
        {
            BDBHARemoteReplicationNodeImpl remoteNode = new BDBHARemoteReplicationNodeImpl(BDBHAVirtualHostNodeImpl.this, nodeToAttributes(node), getReplicatedEnvironmentFacade());
            remoteNode.registerWithParents();
            remoteNode.open();

            getEventLogger().message(getGroupLogSubject(), HighAvailabilityMessages.JOINED(remoteNode.getName(), remoteNode.getAddress()));
        }

        @Override
        public void onReplicationNodeRemovedFromGroup(final ReplicationNode node)
        {
            getTaskExecutor().submit(new VirtualHostNodeGroupTask(node)
            {
                @Override
                public String getAction()
                {
                    return "remote node removed";
                }

                @Override
                public void perform()
                {
                    removeRemoteReplicationNode(node);
                }
            });
        }

        private void removeRemoteReplicationNode(ReplicationNode node)
        {
            BDBHARemoteReplicationNodeImpl remoteNode = getChildByName(BDBHARemoteReplicationNodeImpl.class, node.getName());
            if (remoteNode != null)
            {
                remoteNode.setNodeLeft(true);
                doAfter(remoteNode.deleteNoChecks(),
                        () -> getEventLogger().message(getGroupLogSubject(),
                                                 HighAvailabilityMessages.REMOVED(remoteNode.getName(),
                                                                                  remoteNode.getAddress())));
            }
        }

        @Override
        public void onNodeState(final ReplicationNode node, final NodeState nodeState)
        {
            Subject.doAs(getSystemTaskSubject(_virtualHostNodePrincipalName), (PrivilegedAction<Void>) () ->
            {
                processNodeState(node, nodeState);
                return null;
            });
        }

        private void processNodeState(ReplicationNode node, NodeState nodeState)
        {
            BDBHARemoteReplicationNodeImpl remoteNode = getChildByName(BDBHARemoteReplicationNodeImpl.class, node.getName());
            if (remoteNode != null)
            {
                final NodeRole previousRole = remoteNode.getRole();
                final NodeRole newRole;
                if (nodeState == null)
                {
                    newRole = NodeRole.UNREACHABLE;
                    remoteNode.setRole(newRole);
                    remoteNode.setLastTransactionId(-1);
                    if (previousRole != NodeRole.UNREACHABLE)
                    {
                        getEventLogger().message(getGroupLogSubject(), HighAvailabilityMessages.LEFT(remoteNode.getName(), remoteNode.getAddress()));
                    }
                }
                else
                {
                    LOGGER.debug("Node {} processing state update. Node state {} joinTime {} currentTxnEndVLSN {}",
                                 remoteNode.getName(),
                                 nodeState.getNodeState(),
                                 nodeState.getJoinTime(),
                                 nodeState.getCurrentTxnEndVLSN());

                    remoteNode.setJoinTime(nodeState.getJoinTime());
                    remoteNode.setLastTransactionId(nodeState.getCurrentTxnEndVLSN());
                    ReplicatedEnvironment.State state = nodeState.getNodeState();
                    newRole = NodeRole.fromJeState(state);
                    remoteNode.setRole(newRole);

                    if (previousRole == NodeRole.UNREACHABLE)
                    {
                        getEventLogger().message(getGroupLogSubject(), HighAvailabilityMessages.JOINED(remoteNode.getName(), remoteNode.getAddress()));
                    }

                    if (NodeRole.MASTER == newRole)
                    {
                        byte[] applicationState = nodeState.getAppState();
                        if (applicationState != null)
                        {
                            Set<String> permittedNodes = ReplicatedEnvironmentFacade.convertApplicationStateBytesToPermittedNodeList(applicationState);
                            if (_permittedNodes.size() != permittedNodes.size() || !_permittedNodes.containsAll(permittedNodes))
                            {
                                if (_permittedNodes.contains(remoteNode.getAddress()))
                                {
                                    setAttributes(Collections.singletonMap(PERMITTED_NODES, new ArrayList<>(permittedNodes)));
                                } else
                                {
                                    LOGGER.warn("Cannot accept the new permitted node list from the master as the master '" + remoteNode.getName()
                                            + "' (" + remoteNode.getAddress() + ") was not in previous permitted list " + _permittedNodes);
                                }
                            }
                        }
                        else
                        {
                            if (LOGGER.isDebugEnabled())
                            {
                                LOGGER.debug(String.format("Application state returned by JE was 'null' so skipping permitted node handling: %s", nodeState));
                            }
                        }
                    }
                }

                if (newRole != previousRole)
                {
                    getEventLogger().message(getGroupLogSubject(), HighAvailabilityMessages.ROLE_CHANGED(remoteNode.getName(),
                                                                                                         remoteNode.getAddress(),
                                                                                                         previousRole.name(),
                                                                                                         newRole.name()));
                }
            }
        }

        @Override
        public boolean onIntruderNode(final ReplicationNode node)
        {
            return Subject.doAs(getSystemTaskSubject(_virtualHostNodePrincipalName), (PrivilegedAction<Boolean>) () ->
                    processIntruderNode(node));
        }

        private boolean processIntruderNode(final ReplicationNode node)
        {
            final String hostAndPort = node.getHostName() + ":" + node.getPort();
            getEventLogger().message(getGroupLogSubject(), HighAvailabilityMessages.INTRUDER_DETECTED(node.getName(), hostAndPort));

            boolean inManagementMode = ((Broker) getParent()).isManagementMode();
            if (inManagementMode)
            {
                BDBHARemoteReplicationNodeImpl remoteNode = getChildByName(BDBHARemoteReplicationNodeImpl.class, node.getName());
                if (remoteNode == null)
                {
                    addRemoteReplicationNode(node);
                }
                return true;
            }
            else
            {
                LOGGER.error(String.format("Intruder node '%s' from '%s' detected. Shutting down virtual host node " +
                                           "'%s' (last role %s) owing to the presence of a node not in permitted nodes '%s'",
                                           node.getName(),
                                           hostAndPort,
                                           BDBHAVirtualHostNodeImpl.this.getName(),
                                           _lastRole.get(),
                                           String.valueOf(BDBHAVirtualHostNodeImpl.this.getPermittedNodes()) ));
                getTaskExecutor().submit(new Task<Void, RuntimeException>()
                {
                    @Override
                    public Void execute()
                    {
                        shutdownOnIntruder(hostAndPort);
                        return null;
                    }

                    @Override
                    public String getObject()
                    {
                        return BDBHAVirtualHostNodeImpl.this.toString();
                    }

                    @Override
                    public String getAction()
                    {
                        return "intruder detected";
                    }

                    @Override
                    public String getArguments()
                    {
                        return "ReplicationNode[" + node.getName() + " from " + hostAndPort + "]";
                    }
                });

                return false;
            }
        }

        @Override
        public void onNoMajority()
        {
            getEventLogger().message(getVirtualHostNodeLogSubject(), HighAvailabilityMessages.QUORUM_LOST());
        }

        @Override
        public void onNodeRolledback()
        {
            getEventLogger().message(getVirtualHostNodeLogSubject(), HighAvailabilityMessages.NODE_ROLLEDBACK());
        }

        @Override
        public void onException(Exception e)
        {
            if (e instanceof LogWriteException)
            {
                // TODO: Used when the node is a replica and it runs out of disk. Currently we cause the whole Broker
                // to stop as that is what happens when a master runs out of disk.  In the long term, we should
                // close the node / transition to actual state ERROR.

                try
                {
                    closeAsync();
                }
                finally
                {
                    _systemConfig.getEventLogger().message(BrokerMessages.FATAL_ERROR(e.getMessage()));
                    _systemConfig.closeAsync();
                }
            }
        }

        private Map<String, Object> nodeToAttributes(ReplicationNode replicationNode)
        {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(ConfiguredObject.NAME, replicationNode.getName());
            attributes.put(ConfiguredObject.DURABLE, false);
            attributes.put(BDBHARemoteReplicationNode.ADDRESS, replicationNode.getHostName() + ":" + replicationNode.getPort());
            attributes.put(BDBHARemoteReplicationNode.MONITOR, replicationNode.getType() == NodeType.MONITOR);
            return attributes;
        }
    }

    protected void shutdownOnIntruder(String intruderHostAndPort)
    {
        LOGGER.info("Intruder detected (" + intruderHostAndPort + "), stopping and setting state to ERRORED");

        final State initialState = getState();


        ListenableFuture<Void> future = doAfterAlways(stopAndSetStateTo(State.ERRORED), () ->
        {
            _lastRole.set(NodeRole.DETACHED);
            attributeSet(ROLE, _role, NodeRole.DETACHED);
            notifyStateChanged(initialState, State.ERRORED);
        });

        addFutureCallback(future, new FutureCallback<>()
        {
            @Override
            public void onSuccess(final Void result)
            {
            }

            @Override
            public void onFailure(final Throwable t)
            {
                LOGGER.error("Failed to close children when handling intruder", t);
            }
        }, getTaskExecutor());
    }

    private abstract class VirtualHostNodeGroupTask implements Task<Void, RuntimeException>
    {
        private final ReplicationNode _replicationNode;

        public VirtualHostNodeGroupTask(ReplicationNode replicationNode)
        {
            _replicationNode = replicationNode;
        }

        @Override
        public Void execute()
        {
            return Subject.doAs(getSystemTaskSubject(_virtualHostNodePrincipalName), (PrivilegedAction<Void>) () ->
            {
                perform();
                return null;
            });
        }

        @Override
        public String getObject()
        {
            return BDBHAVirtualHostNodeImpl.this.toString();
        }

        @Override
        public String getArguments()
        {
            return "ReplicationNode[" + _replicationNode.getName() + " from " + _replicationNode.getHostName() + ":" + _replicationNode.getPort() + "]";
        }

        abstract void perform();
    }

    @Override
    public void updateMutableConfig()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            environmentFacade.updateMutableConfig(this);
        }
    }

    @Override
    public int cleanLog()
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            return environmentFacade.cleanLog();
        }
        return 0;
    }

    @Override
    public void checkpoint(final boolean force)
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            environmentFacade.checkpoint(force);
        }
    }

    @Override
    public Map<String, Map<String, Object>> environmentStatistics(final boolean reset)
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            return environmentFacade.getEnvironmentStatistics(reset);
        }
        else
        {
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String, Object> transactionStatistics(final boolean reset)
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            return environmentFacade.getTransactionStatistics(reset);
        }
        else
        {
            return Collections.emptyMap();
        }
    }
    @Override
    public Map<String, Object> databaseStatistics(String database, final boolean reset)
    {
        ReplicatedEnvironmentFacade environmentFacade = getReplicatedEnvironmentFacade();
        if (environmentFacade != null)
        {
            return environmentFacade.getDatabaseStatistics(database, reset);
        }
        else
        {
            return Collections.emptyMap();
        }
    }

    @Override
    public EnvironmentFacade getEnvironmentFacade()
    {
        return _environmentFacade.get();
    }

    public static Map<String, Collection<String>> getSupportedChildTypes()
    {
        return Collections.singletonMap(VirtualHost.class.getSimpleName(), (Collection<String>) Collections.singleton(BDBHAVirtualHostImpl.VIRTUAL_HOST_TYPE));
    }

    @Override
    protected void logCreated(final Map<String, Object> attributes,
                              final Outcome outcome)
    {
        getEventLogger().message(getVirtualHostNodeLogSubject(),
                                 HighAvailabilityMessages.CREATE(getName(),
                                                                 String.valueOf(outcome),
                                                                 attributesAsString(attributes)));
    }

    @Override
    protected void logDeleted(final Outcome outcome)
    {
        LOGGER.debug("{} : {} ({}) : Delete : {}",
                     LogMessage.getActor(),
                     getCategoryClass().getSimpleName(),
                     getName(),
                     outcome);
    }

    @Override
    protected void logUpdated(final Map<String, Object> attributes, final Outcome outcome)
    {
        getEventLogger().message(getVirtualHostNodeLogSubject(),
                                 HighAvailabilityMessages.UPDATE(getName(),
                                                                 String.valueOf(outcome),
                                                                 attributesAsString(attributes)));
    }
}
