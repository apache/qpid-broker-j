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

import javax.net.ssl.SSLContext;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.Transport;

public class HttpPortImpl extends AbstractPort<HttpPortImpl> implements HttpPort<HttpPortImpl>
{
    private PortManager _portManager;

    @ManagedAttributeField
    private int _threadPoolMaximum;

    @ManagedAttributeField
    private int _threadPoolMinimum;

    @ManagedAttributeField
    private boolean _manageBrokerOnNoAliasMatch;

    private volatile int _numberOfAcceptors;
    private volatile int _numberOfSelectors;
    private volatile int _acceptsBacklogSize;
    private volatile long _absoluteSessionTimeout;
    private volatile int _tlsSessionTimeout;
    private volatile int _tlsSessionCacheSize;

    @ManagedObjectFactoryConstructor
    public HttpPortImpl(final Map<String, Object> attributes,
                        final Container<?> container)
    {
        super(attributes, container);
    }

    @Override
    public void setPortManager(PortManager manager)
    {
        _portManager = manager;
    }

    @Override
    public int getBoundPort()
    {
        final PortManager portManager = getPortManager();
        return portManager == null ? -1 : portManager.getBoundPort(this);
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
    public boolean isManageBrokerOnNoAliasMatch()
    {
        return _manageBrokerOnNoAliasMatch;
    }

    @Override
    public int getDesiredNumberOfAcceptors()
    {
        return _numberOfAcceptors;
    }

    @Override
    public int getDesiredNumberOfSelectors()
    {
        return _numberOfSelectors;
    }

    @Override
    public int getAcceptBacklogSize()
    {
        return _acceptsBacklogSize;
    }

    @Override
    public int getNumberOfAcceptors()
    {
        final PortManager portManager = getPortManager();
        return portManager == null ? 0 : portManager.getNumberOfAcceptors(this) ;
    }

    @Override
    public int getNumberOfSelectors()
    {
        final PortManager portManager = getPortManager();
        return portManager == null ? 0 : portManager.getNumberOfSelectors(this) ;
    }

    @Override
    public int getTLSSessionTimeout()
    {
        return _tlsSessionTimeout;
    }

    @Override
    public int getTLSSessionCacheSize()
    {
        return _tlsSessionCacheSize;
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();

        _acceptsBacklogSize = getContextValue(Integer.class, HttpPort.PORT_HTTP_ACCEPT_BACKLOG);
        _numberOfAcceptors = getContextValue(Integer.class, HttpPort.PORT_HTTP_NUMBER_OF_ACCEPTORS);
        _numberOfSelectors =  getContextValue(Integer.class, HttpPort.PORT_HTTP_NUMBER_OF_SELECTORS);
        _absoluteSessionTimeout =  getContextValue(Long.class, HttpPort.ABSOLUTE_SESSION_TIMEOUT);
        _tlsSessionTimeout = getContextValue(Integer.class, HttpPort.TLS_SESSION_TIMEOUT);
        _tlsSessionCacheSize = getContextValue(Integer.class, HttpPort.TLS_SESSION_CACHE_SIZE);
    }

    @Override
    protected State onActivate()
    {
        if(getPortManager() != null)
        {
            return super.onActivate();
        }
        else
        {
            return State.QUIESCED;
        }
    }

    @Override
    public SSLContext getSSLContext()
    {
        final PortManager portManager = getPortManager();
        return portManager == null ? null : portManager.getSSLContext(this);
    }

    @Override
    protected boolean updateSSLContext()
    {
        if (getTransports().contains(Transport.SSL))
        {
            final PortManager portManager = getPortManager();
            return portManager != null && portManager.updateSSLContext(this);
        }
        return false;
    }

    private PortManager getPortManager()
    {
        return _portManager;
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        validateThreadPoolSettings(this);

        final int acceptsBacklogSize = getContextValue(Integer.class, HttpPort.PORT_HTTP_ACCEPT_BACKLOG);
        if (acceptsBacklogSize < 1)
        {
            throw new IllegalConfigurationException(String.format(
                    "The size of accept backlog %d is too small. Must be greater than zero.",
                    acceptsBacklogSize));
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

    @Override
    public long getAbsoluteSessionTimeout()
    {
        return _absoluteSessionTimeout;
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
