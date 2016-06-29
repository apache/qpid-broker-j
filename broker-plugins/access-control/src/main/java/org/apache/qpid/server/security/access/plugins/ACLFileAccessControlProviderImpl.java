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

import java.util.Map;

import org.apache.qpid.server.logging.messages.AccessControlMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.security.access.config.AclFileParser;
import org.apache.qpid.server.security.access.config.RuleBasedAccessControl;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

public class ACLFileAccessControlProviderImpl
        extends AbstractRuleBasedAccessControlProvider<ACLFileAccessControlProviderImpl>
        implements ACLFileAccessControlProvider<ACLFileAccessControlProviderImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ACLFileAccessControlProviderImpl.class);

    static
    {
        Handler.register();
    }

    @ManagedAttributeField( afterSet = "reloadAclFile")
    private String _path;

    @ManagedObjectFactoryConstructor
    public ACLFileAccessControlProviderImpl(Map<String, Object> attributes, Broker broker)
    {
        super(attributes, broker);
    }

    @Override
    protected RuleBasedAccessControl createRuleBasedAccessController()
    {
        return new RuleBasedAccessControl(AclFileParser.parse(getPath(), getBroker()), getBroker().getModel());
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
    }

    @Override
    public void reload()
    {
        getSecurityManager().authoriseUpdate(this);
        reloadAclFile();
    }

    private void reloadAclFile()
    {
        try
        {
            recreateAccessController();
            getEventLogger().message(AccessControlMessages.LOADED(String.valueOf(getPath()).startsWith("data:") ? "data:..." : getPath()));

        }
        catch(RuntimeException e)
        {
            throw new IllegalConfigurationException(e.getMessage(), e);
        }
    }

    @Override
    public String getPath()
    {
        return _path;
    }

}
