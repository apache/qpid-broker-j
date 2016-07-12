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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.CustomRestHeaders;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostAccessControlProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.RuleOutcome;
import org.apache.qpid.server.security.access.config.AclAction;
import org.apache.qpid.server.security.access.config.AclFileParser;
import org.apache.qpid.server.security.access.config.AclRulePredicates;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.ObjectType;
import org.apache.qpid.server.security.access.config.Rule;
import org.apache.qpid.server.security.access.config.RuleBasedAccessControl;
import org.apache.qpid.server.security.access.config.RuleSet;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

public class RuleBasedVirtualHostAccessControlProviderImpl
        extends AbstractRuleBasedAccessControlProvider<RuleBasedVirtualHostAccessControlProviderImpl>
        implements RuleBasedVirtualHostAccessControlProvider<RuleBasedVirtualHostAccessControlProviderImpl>
{
    private static final EnumSet<ObjectType> ALLOWED_OBJECT_TYPES = EnumSet.of(ObjectType.ALL,
                                                                               ObjectType.QUEUE,
                                                                               ObjectType.EXCHANGE,
                                                                               ObjectType.VIRTUALHOST,
                                                                               ObjectType.METHOD);

    private static final Logger LOGGER = LoggerFactory.getLogger(RuleBasedVirtualHostAccessControlProviderImpl.class);

    static
    {
        Handler.register();
    }

    @ManagedAttributeField
    private Result _defaultResult;
    @ManagedAttributeField
    private List<AclRule> _rules;


    @ManagedObjectFactoryConstructor
    public RuleBasedVirtualHostAccessControlProviderImpl(Map<String, Object> attributes, VirtualHost<?> virtualHost)
    {
        super(attributes, virtualHost);
    }


    @Override
    protected void changeAttributes(final Map<String, Object> attributes)
    {
        super.changeAttributes(attributes);
        if(attributes.containsKey(DEFAULT_RESULT) || attributes.containsKey(RULES))
        {
            recreateAccessController();
        }
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        if(changedAttributes.contains(RULES))
        {
            for(AclRule rule : ((RuleBasedVirtualHostAccessControlProvider<?>)proxyForValidation).getRules())
            {
                if(!ALLOWED_OBJECT_TYPES.contains(rule.getObjectType()))
                {
                    throw new IllegalArgumentException("Cannot use the object type " + rule.getObjectType() + " only the following object types are allowed: " + ALLOWED_OBJECT_TYPES);
                }
            }
        }
    }

    @Override
    protected RuleBasedAccessControl createRuleBasedAccessController()
    {
        List<Rule> rules = new ArrayList<>();
        for(AclRule configuredRule : _rules)
        {
            rules.add(new Rule(configuredRule.getIdentity(),
                               new AclAction(configuredRule.getOperation(),
                                             configuredRule.getObjectType(),
                                             new AclRulePredicates(configuredRule.getAttributes())),
                               configuredRule.getOutcome()));
        }
        return new RuleBasedAccessControl(new RuleSet(this, rules, _defaultResult), getModel());
    }

    @Override
    public Result getDefaultResult()
    {
        return _defaultResult;
    }

    @Override
    public List<AclRule> getRules()
    {
        return _rules;
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings("unused")
    private ListenableFuture<Void> activate()
    {

        final boolean isManagementMode = getModel().getAncestor(SystemConfig.class, this).isManagementMode();
        try
        {
            recreateAccessController();
            setState(isManagementMode ? State.QUIESCED : State.ACTIVE);
        }
        catch (RuntimeException e)
        {
            setState(State.ERRORED);
            if (isManagementMode)
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
    public void loadFromFile(final String path)
    {
        RuleSet ruleSet = AclFileParser.parse(path, this);
        List<AclRule> aclRules = new ArrayList<>();
        for(Rule rule : ruleSet.getAllRules())
        {
            aclRules.add(new AclRuleImpl(rule));
        }
        Map<String,Object> attrs = new HashMap<>();
        attrs.put(DEFAULT_RESULT, ruleSet.getDefault());
        attrs.put(RULES, aclRules);
        setAttributes(attrs);
    }

    @Override
    public int compareTo(final VirtualHostAccessControlProvider o)
    {
        return VIRTUAL_HOST_ACCESS_CONTROL_POVIDER_COMPARATOR.compare(this, o);
    }

    public static class AclRuleImpl implements AclRule
    {
        private final Rule _rule;

        AclRuleImpl(final Rule rule)
        {
            _rule = rule;
        }

        @Override
        public String getIdentity()
        {
            return _rule.getIdentity();
        }

        @Override
        public ObjectType getObjectType()
        {
            return _rule.getAction().getObjectType();
        }

        @Override
        public LegacyOperation getOperation()
        {
            return _rule.getAction().getOperation();
        }

        @Override
        public Map<ObjectProperties.Property, String> getAttributes()
        {
            return _rule.getAction().getProperties().asPropertyMap();
        }

        @Override
        public RuleOutcome getOutcome()
        {
            return _rule.getRuleOutcome();
        }
    }

    @Override
    public Content extractRules()
    {
        StringBuilder sb = new StringBuilder();
        for(AclRule rule : _rules)
        {
            sb.append("ACL ");
            sb.append(rule.getOutcome().name().replace('_','-'));
            sb.append(' ');
            sb.append(rule.getIdentity());
            sb.append(' ');
            sb.append(rule.getOperation().name());
            sb.append(' ');
            sb.append(rule.getObjectType().name());
            for(Map.Entry<ObjectProperties.Property,String> entry : rule.getAttributes().entrySet())
            {
                sb.append(' ');
                sb.append(entry.getKey().getCanonicalName());
                sb.append(" = \"");
                sb.append(entry.getValue());
                sb.append("\"");
            }
            sb.append('\n');
        }
        return new StringContent(sb.toString());
    }

    private static class StringContent implements Content, CustomRestHeaders
    {

        private final String _content;

        public StringContent(final String content)
        {
            _content = content;
        }

        @Override
        public void write(final OutputStream outputStream) throws IOException
        {
            outputStream.write(_content.getBytes(StandardCharsets.UTF_8));
        }

        @RestContentHeader("Content-Type")
        public String getContentType()
        {
            return "text/plain";
        }

        @Override
        public void release()
        {

        }
    }

}
