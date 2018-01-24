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
package org.apache.qpid.server.virtualhostnode;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.RemoteReplicationNode;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreAttributes;


public class RedirectingVirtualHostNodeImpl
        extends AbstractConfiguredObject<RedirectingVirtualHostNodeImpl> implements RedirectingVirtualHostNode<RedirectingVirtualHostNodeImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RedirectingVirtualHostImpl.class);
    public static final String VIRTUAL_HOST_NODE_TYPE = "Redirector";
    private final Broker<?> _broker;

    @ManagedAttributeField
    private String _virtualHostInitialConfiguration;

    @ManagedAttributeField
    private boolean _defaultVirtualHostNode;

    @ManagedAttributeField
    private PreferenceStoreAttributes _preferenceStoreAttributes;

    @ManagedAttributeField
    private Map<Port<?>,String> _redirects;

    private volatile RedirectingVirtualHostImpl _virtualHost;

    @ManagedObjectFactoryConstructor
    public RedirectingVirtualHostNodeImpl(Map<String, Object> attributes, Broker<?> parent)
    {
        super(parent, attributes);
        _broker = parent;
    }

    @StateTransition( currentState = {State.UNINITIALIZED, State.STOPPED, State.ERRORED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> doActivate()
    {
        final SettableFuture<Void> resultFuture = SettableFuture.create();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, getName());
        attributes.put(ConfiguredObject.TYPE, RedirectingVirtualHostImpl.VIRTUAL_HOST_TYPE);

        final ListenableFuture<VirtualHost> virtualHostFuture = getObjectFactory().createAsync(VirtualHost.class, attributes, this);

        addFutureCallback(virtualHostFuture, new FutureCallback<VirtualHost>()
        {
            @Override
            public void onSuccess(final VirtualHost virtualHost)
            {
                _virtualHost = (RedirectingVirtualHostImpl) virtualHost;
                setState(State.ACTIVE);
                resultFuture.set(null);

            }

            @Override
            public void onFailure(final Throwable t)
            {
                setState(State.ERRORED);
                if (((Broker) getParent()).isManagementMode())
                {
                    LOGGER.warn("Failed to make {} active.", this, t);
                    resultFuture.set(null);
                }
                else
                {
                    resultFuture.setException(t);
                }
            }
        }, getTaskExecutor());

        return resultFuture;
    }

    @StateTransition( currentState = { State.ACTIVE, State.ERRORED, State.UNINITIALIZED }, desiredState = State.STOPPED )
    private ListenableFuture<Void> doStop()
    {
        final ListenableFuture<Void> future = Futures.immediateFuture(null);
        final RedirectingVirtualHostImpl virtualHost = _virtualHost;
        if (virtualHost != null)
        {
            return doAfter(virtualHost.closeAsync(), new Callable<ListenableFuture<Void>>()
            {
                @Override
                public ListenableFuture<Void> call() throws Exception
                {
                    _virtualHost = null;
                    setState(State.STOPPED);
                    return future;
                }
            });
        }
        else
        {
            setState(State.STOPPED);
            return future;
        }
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        final ListenableFuture<Void> superFuture = super.beforeClose();
        return closeVirtualHost(superFuture);
    }

    @Override
    protected ListenableFuture<Void> beforeDelete()
    {
        final ListenableFuture<Void> superFuture = super.beforeDelete();
        return closeVirtualHost(superFuture);
    }

    private ListenableFuture<Void> closeVirtualHost(final ListenableFuture<Void> superFuture)
    {
        final RedirectingVirtualHostImpl virtualHost = _virtualHost;
        if (virtualHost != null)
        {
            return doAfter(virtualHost.closeAsync(), () -> {
                _virtualHost = null;
                return superFuture;
            });
        }
        else
        {
            return superFuture;
        }
    }

    @Override
    public String getVirtualHostInitialConfiguration()
    {
        return _virtualHostInitialConfiguration;
    }

    @Override
    public boolean isDefaultVirtualHostNode()
    {
        return _defaultVirtualHostNode;
    }

    @Override
    public VirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }

    @Override
    public DurableConfigurationStore getConfigurationStore()
    {
        return null;
    }

    @Override
    public Collection<? extends RemoteReplicationNode> getRemoteReplicationNodes()
    {
        return Collections.emptySet();
    }

    @Override
    public PreferenceStore createPreferenceStore()
    {
        return null;
    }

    @Override
    public PreferenceStoreAttributes getPreferenceStoreAttributes()
    {
        return _preferenceStoreAttributes;
    }

    @Override
    public Map<Port<?>, String> getRedirects()
    {
        return _redirects;
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();

        if (isDefaultVirtualHostNode())
        {
            VirtualHostNode existingDefault = _broker.findDefautVirtualHostNode();

            if (existingDefault != null)
            {
                throw new IntegrityViolationException("The existing virtual host node '" + existingDefault.getName()
                                                      + "' is already the default for the Broker.");
            }
        }
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        VirtualHostNode updated = (VirtualHostNode) proxyForValidation;
        if (changedAttributes.contains(DEFAULT_VIRTUAL_HOST_NODE) && updated.isDefaultVirtualHostNode())
        {
            VirtualHostNode existingDefault = _broker.findDefautVirtualHostNode();

            if (existingDefault != null && existingDefault != this)
            {
                throw new IntegrityViolationException("Cannot make '" + getName() + "' the default virtual host node for"
                                                      + " the Broker as virtual host node '" + existingDefault.getName()
                                                      + "' is already the default.");
            }
        }
    }

    @Override
    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass,
                                                                             Map<String, Object> attributes)
    {
        if(childClass == VirtualHost.class)
        {
            throw new UnsupportedOperationException("The redirecting virtualhost node automatically manages the creation"
                                                    + " of the redirecting virtualhost. Creating it explicitly is not supported.");
        }
        else
        {
            return super.addChildAsync(childClass, attributes);
        }
    }

    public static Map<String, Collection<String>> getSupportedChildTypes()
    {
        Collection<String> validVhostTypes = Collections.singleton(RedirectingVirtualHostImpl.TYPE);
        return Collections.singletonMap(VirtualHost.class.getSimpleName(), validVhostTypes);
    }

}
