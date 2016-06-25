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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.messages.AccessControlMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.CustomRestHeaders;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.ObjectProperties;
import org.apache.qpid.server.security.access.ObjectType;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.access.RuleOutcome;
import org.apache.qpid.server.security.access.config.AclAction;
import org.apache.qpid.server.security.access.config.AclFileParser;
import org.apache.qpid.server.security.access.config.AclRulePredicates;
import org.apache.qpid.server.security.access.config.Rule;
import org.apache.qpid.server.security.access.config.RuleBasedAccessControl;
import org.apache.qpid.server.security.access.config.RuleSet;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

public class RuleBasedAccessControlProviderImpl
        extends AbstractRuleBasedAccessControlProvider<RuleBasedAccessControlProviderImpl>
        implements RuleBasedAccessControlProvider<RuleBasedAccessControlProviderImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleBasedAccessControlProviderImpl.class);

    static
    {
        Handler.register();
    }

    @ManagedAttributeField
    private Result _defaultResult;
    @ManagedAttributeField
    private List<AclRule> _rules;


    @ManagedObjectFactoryConstructor
    public RuleBasedAccessControlProviderImpl(Map<String, Object> attributes, Broker broker)
    {
        super(attributes, broker);
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
        return new RuleBasedAccessControl(new RuleSet(getBroker(), rules, _defaultResult));
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

    @Override
    public void loadFromFile(final String path)
    {
        RuleSet ruleSet = AclFileParser.parse(path, getBroker());
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
        public Operation getOperation()
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
