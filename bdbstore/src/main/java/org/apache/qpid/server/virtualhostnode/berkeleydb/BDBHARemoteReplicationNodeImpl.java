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

import static org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNodeImpl.MUTATE_JE_TIMEOUT_MS;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.HighAvailabilityMessages;
import org.apache.qpid.server.logging.subjects.BDBHAVirtualHostNodeLogSubject;
import org.apache.qpid.server.logging.subjects.GroupLogSubject;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.store.berkeleydb.replication.ReplicatedEnvironmentFacade;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class BDBHARemoteReplicationNodeImpl extends AbstractConfiguredObject<BDBHARemoteReplicationNodeImpl> implements BDBHARemoteReplicationNode<BDBHARemoteReplicationNodeImpl>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(BDBHARemoteReplicationNodeImpl.class);

    private final ReplicatedEnvironmentFacade _replicatedEnvironmentFacade;
    private final String _address;
    private final Broker<?> _broker;

    private volatile Date _joinTime;
    private volatile long _lastTransactionId;

    @ManagedAttributeField(afterSet="afterSetRole")
    private volatile NodeRole _role;

    private final boolean _isMonitor;
    private BDBHAVirtualHostNodeLogSubject _virtualHostNodeLogSubject;
    private GroupLogSubject _groupLogSubject;
    private volatile NodeRole _lastKnownRole;
    private volatile  boolean _nodeLeft = false;

    public BDBHARemoteReplicationNodeImpl(BDBHAVirtualHostNode<?> virtualHostNode, Map<String, Object> attributes, ReplicatedEnvironmentFacade replicatedEnvironmentFacade)
    {
        super(virtualHostNode, attributes);
        _broker = (Broker<?>) virtualHostNode.getParent();
        _address = (String)attributes.get(ADDRESS);
        _replicatedEnvironmentFacade = replicatedEnvironmentFacade;
        setRole(NodeRole.UNREACHABLE);
        _isMonitor = (Boolean)attributes.get(MONITOR);
    }

    @Override
    public String getGroupName()
    {
        return _replicatedEnvironmentFacade.getGroupName();
    }

    @Override
    public String getAddress()
    {
        return _address;
    }

    @Override
    public NodeRole getRole()
    {
        return _lastKnownRole;
    }

    @Override
    public Date getJoinTime()
    {
        return _joinTime;
    }

    @Override
    public long getLastKnownReplicationTransactionId()
    {
        return _lastTransactionId;
    }

    @Override
    public boolean isMonitor()
    {
        return _isMonitor;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[id=" + getId() + ", name=" + getName() + ", address=" + getAddress()
               + ", state=" + getState() + ", role=" + getRole() + "]";
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        if (!_nodeLeft)
        {
            SettableFuture<Void> future = SettableFuture.create();

            String nodeName = getName();
            try
            {
                _replicatedEnvironmentFacade.removeNodeFromGroup(nodeName);
                getEventLogger().message(_virtualHostNodeLogSubject, HighAvailabilityMessages.DELETED());
                future.set(null);
            }
            catch (RuntimeException e)
            {
                future.setException(e);
            }
            return future;
        }
        else
        {
            return super.onDelete();
        }
    }

    @SuppressWarnings("unchecked")
    protected void afterSetRole()
    {
        try
        {
            String nodeName = getName();
            getEventLogger().message(_groupLogSubject, HighAvailabilityMessages.TRANSFER_MASTER(getName(), getAddress()));

            _replicatedEnvironmentFacade.transferMasterAsynchronously(nodeName).get(MUTATE_JE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            LOGGER.warn("Transfer master did not complete within " + MUTATE_JE_TIMEOUT_MS + "ms. Node may still be elected master at a later time.");
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
                throw new ConnectionScopedRuntimeException("Unexpected exception on master transfer: " + cause.getMessage(), cause);
            }
        }
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        if (changedAttributes.contains(ROLE))
        {
            NodeRole currentRole = getRole();
            if (NodeRole.REPLICA != currentRole)
            {
                throw new IllegalArgumentException("Cannot transfer mastership when not in replica role."
                                                 + " Current role " + currentRole);
            }
            NodeRole newRole = (NodeRole) ((BDBHARemoteReplicationNode<?>) proxyForValidation).getAttribute(ROLE);
            if (NodeRole.MASTER != newRole)
            {
                throw new IllegalArgumentException("Changing role to other value then " + NodeRole.MASTER + " is unsupported");
            }
        }

        if (changedAttributes.contains(JOIN_TIME))
        {
            throw new IllegalArgumentException("Cannot change derived attribute " + JOIN_TIME);
        }

        if (changedAttributes.contains(LAST_KNOWN_REPLICATION_TRANSACTION_ID))
        {
            throw new IllegalArgumentException("Cannot change derived attribute " + LAST_KNOWN_REPLICATION_TRANSACTION_ID);
        }
    }

    void setRole(NodeRole role)
    {
        _lastKnownRole = role;
        _role = role;
        updateModelStateFromRole(role);
    }

    void setJoinTime(long joinTime)
    {
        _joinTime = new Date(joinTime);
    }

    void setLastTransactionId(long lastTransactionId)
    {
        _lastTransactionId = lastTransactionId;
    }

    private void updateModelStateFromRole(NodeRole role)
    {
        State currentState = getState();
        if (currentState == State.DELETED)
        {
            return;
        }

        boolean isActive = NodeRole.MASTER == role || NodeRole.REPLICA == role;
        setState(isActive ? State.ACTIVE : State.UNAVAILABLE);
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        _virtualHostNodeLogSubject =  new BDBHAVirtualHostNodeLogSubject(getGroupName(), getName());
        _groupLogSubject = new GroupLogSubject(getGroupName());
    }

    private EventLogger getEventLogger()
    {
        return _broker.getEventLogger();
    }

    void setNodeLeft(final boolean nodeLeft)
    {
        _nodeLeft = nodeLeft;
    }

    @Override
    protected ListenableFuture<Void> deleteNoChecks()
    {
        return super.deleteNoChecks();
    }
}
