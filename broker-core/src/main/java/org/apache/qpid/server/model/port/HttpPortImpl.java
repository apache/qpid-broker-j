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

import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
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
    private int _numberOfAcceptors;
    private int _numberOfSelectors;
    private int _acceptsBacklogSize;

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
    public int getNumberOfAcceptors()
    {
        return _numberOfAcceptors;
    }

    @Override
    public int getNumberOfSelectors()
    {
        return _numberOfSelectors;
    }

    @Override
    public int getAcceptsBacklogSize()
    {
        return _acceptsBacklogSize;
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();

        int maxThreads = getThreadPoolMaximum();
        int min = Math.max(1, Math.min(4, maxThreads / 2 - 1));
        _acceptsBacklogSize = getContextValue(Integer.class, HttpPort.PORT_HTTP_ACCEPT_BACKLOG);
        _numberOfAcceptors = getContextValue(Integer.class, HttpPort.PORT_HTTP_NUMBER_OF_ACCEPTORS);
        if (_numberOfAcceptors < 0) // if _numberOfAcceptors == 0, selectors threads are used as acceptors
        {
            _numberOfAcceptors = Math.max(1, Math.min(min, Runtime.getRuntime().availableProcessors() / 8));
        }
        _numberOfSelectors = getContextValue(Integer.class, HttpPort.PORT_HTTP_NUMBER_OF_SELECTORS);
        if (_numberOfSelectors <= 0)
        {
            _numberOfSelectors =  Math.max(1, Math.min(min, Runtime.getRuntime().availableProcessors() / 2));
        }
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

        final double acceptsBacklogSize = getContextValue(Integer.class, HttpPort.PORT_HTTP_ACCEPT_BACKLOG);
        if (acceptsBacklogSize < 1)
        {
            throw new IllegalConfigurationException(String.format("The size of accepts backlog %d is too small. Must be greater than zero.", acceptsBacklogSize));
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
