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
package org.apache.qpid.server.security.access.plugins;

import java.util.Collections;
import java.util.Map;

import org.apache.qpid.server.logging.messages.AccessControlMessages;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.access.config.AclFileParser;
import org.apache.qpid.server.security.access.config.RuleBasedAccessControl;
import org.apache.qpid.server.util.StringUtil;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

public class AclFileAccessControlProviderImpl
        extends AbstractLegacyAccessControlProvider<AclFileAccessControlProviderImpl, Broker<?>, AccessControlProvider<?>>
        implements AclFileAccessControlProvider<AclFileAccessControlProviderImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AclFileAccessControlProviderImpl.class);

    static
    {
        Handler.register();
    }

    private final Broker _broker;

    @ManagedAttributeField( afterSet = "reloadAclFile")
    private String _path;

    @ManagedObjectFactoryConstructor
    public AclFileAccessControlProviderImpl(Map<String, Object> attributes, Broker broker)
    {
        super(attributes, broker);
        _broker = broker;
    }

    @Override
    protected RuleBasedAccessControl createRuleBasedAccessController()
    {
        return new RuleBasedAccessControl(AclFileParser.parse(getPath(), this), getModel());
    }

    @Override
    public void reload()
    {
        authorise(Operation.UPDATE);
        reloadAclFile();
    }

    private void reloadAclFile()
    {
        try
        {
            recreateAccessController();
            LOGGER.debug("Calling changeAttributes to try to force update");
            // force the change listener to fire, causing the parent broker to update its cache
            changeAttributes(Collections.<String,Object>emptyMap());
            getEventLogger().message(AccessControlMessages.LOADED(StringUtil.elideDataUrl(getPath())));

        }
        catch(RuntimeException e)
        {
            throw new IllegalConfigurationException(e.getMessage(), e);
        }
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings("unused")
    private ListenableFuture<Void> activate()
    {

        try
        {
            recreateAccessController();
            setState(_broker.isManagementMode() ? State.QUIESCED : State.ACTIVE);
        }
        catch (RuntimeException e)
        {
            setState(State.ERRORED);
            if (_broker.isManagementMode())
            {
                LOGGER.warn("Failed to activate ACL provider: " + getName(), e);
            }
            else
            {
                throw e;
            }
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public String getPath()
    {
        return _path;
    }

    @Override
    public int compareTo(final AccessControlProvider<?> o)
    {
        return ACCESS_CONTROL_PROVIDER_COMPARATOR.compare(this, o);
    }

}
