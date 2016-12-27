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
package org.apache.qpid.server.model.port;

import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.HostNameAlias;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.PatternMatchingAlias;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemAddressSpaceAlias;
import org.apache.qpid.server.model.VirtualHostAlias;
import org.apache.qpid.server.util.PortUtil;

public class HttpPortImpl extends AbstractClientAuthCapablePortWithAuthProvider<HttpPortImpl> implements HttpPort<HttpPortImpl>
{
    private PortManager _portManager;

    @ManagedAttributeField
    private String _bindingAddress;

    @ManagedAttributeField
    private int _threadPoolMaximum;

    @ManagedAttributeField
    private int _threadPoolMinimum;

    @ManagedAttributeField
    private boolean _allowConfidentialOperationsOnInsecureChannels;

    @ManagedAttributeField
    private boolean _manageBrokerOnNoAliasMatch;

    @ManagedObjectFactoryConstructor
    public HttpPortImpl(final Map<String, Object> attributes,
                        final Container<?> container)
    {
        super(attributes, container);
    }

    public void setPortManager(PortManager manager)
    {
        _portManager = manager;
    }

    @Override
    public int getBoundPort()
    {
        return _portManager == null ? -1 : _portManager.getBoundPort(this);
    }

    @Override
    public String getBindingAddress()
    {
        return _bindingAddress;
    }

    @Override
    public int getThreadPoolMaximum()
    {
        return _threadPoolMaximum;
    }

    @Override
    public int getThreadPoolMinimum()
    {
        return _threadPoolMinimum;
    }

    @Override
    public boolean isAllowConfidentialOperationsOnInsecureChannels()
    {
        return _allowConfidentialOperationsOnInsecureChannels;
    }

    @Override
    public boolean isManageBrokerOnNoAliasMatch()
    {
        return _manageBrokerOnNoAliasMatch;
    }

    @Override
    protected State onActivate()
    {
        if(_portManager != null && _portManager.isActivationAllowed(this))
        {
            return super.onActivate();
        }
        else
        {
            return State.QUIESCED;
        }
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        validateThreadPoolSettings(this);

        final double additionalInternalThreads = getContextValue(Integer.class, HttpPort.PORT_HTTP_ADDITIONAL_INTERNAL_THREADS);
        if (additionalInternalThreads < 1)
        {
            throw new IllegalConfigurationException(String.format("Number of additional internal threads %d is too small. Must be greater than zero.", additionalInternalThreads));
        }

        final double maximumQueuedRequests = getContextValue(Integer.class, HttpPort.PORT_HTTP_MAXIMUM_QUEUED_REQUESTS);
        if (maximumQueuedRequests < 1)
        {
            throw new IllegalConfigurationException(String.format("Number of additional internal threads %d is too small. Must be greater than zero.", maximumQueuedRequests));
        }
    }

    @Override
    public void validateOnCreate()
    {
        super.validateOnCreate();
        String bindingAddress = getBindingAddress();
        if (!PortUtil.isPortAvailable(bindingAddress, getPort()))
        {
            throw new IllegalConfigurationException(String.format("Cannot bind to port %d and binding address '%s'. Port is already is use.",
                    getPort(), bindingAddress == null || "".equals(bindingAddress) ? "*" : bindingAddress));
        }
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        HttpPort changed = (HttpPort) proxyForValidation;
        if (changedAttributes.contains(THREAD_POOL_MAXIMUM) || changedAttributes.contains(THREAD_POOL_MINIMUM))
        {
            validateThreadPoolSettings(changed);
        }
    }

    private void validateThreadPoolSettings(HttpPort<?> httpPort)
    {
        if (httpPort.getThreadPoolMaximum() < 1)
        {
            throw new IllegalConfigurationException(String.format("Thread pool maximum %d is too small. Must be greater than zero.", httpPort.getThreadPoolMaximum()));
        }
        if (httpPort.getThreadPoolMinimum() < 1)
        {
            throw new IllegalConfigurationException(String.format("Thread pool minimum %d is too small. Must be greater than zero.", httpPort.getThreadPoolMinimum()));
        }
        if (httpPort.getThreadPoolMinimum() > httpPort.getThreadPoolMaximum())
        {
            throw new IllegalConfigurationException(String.format("Thread pool minimum %d cannot be greater than thread pool maximum %d.", httpPort.getThreadPoolMinimum() , httpPort.getThreadPoolMaximum()));
        }
    }

}
