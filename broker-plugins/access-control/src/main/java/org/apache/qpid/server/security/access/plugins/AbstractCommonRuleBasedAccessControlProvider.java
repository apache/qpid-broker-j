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

import static org.apache.qpid.server.security.access.plugins.RuleBasedAccessControlProvider.DEFAULT_RESULT;
import static org.apache.qpid.server.security.access.plugins.RuleBasedAccessControlProvider.RULES;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.CommonAccessControlProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.CustomRestHeaders;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.config.AclAction;
import org.apache.qpid.server.security.access.config.AclFileParser;
import org.apache.qpid.server.security.access.config.AclRulePredicates;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.ObjectType;
import org.apache.qpid.server.security.access.config.Rule;
import org.apache.qpid.server.security.access.config.RuleBasedAccessControl;
import org.apache.qpid.server.security.access.config.RuleSet;


abstract class AbstractCommonRuleBasedAccessControlProvider<X extends AbstractCommonRuleBasedAccessControlProvider<X,T,Y>, T extends EventLoggerProvider & ConfiguredObject<?>, Y extends CommonAccessControlProvider<Y>>
        extends AbstractLegacyAccessControlProvider<X, T, Y> implements EventLoggerProvider
{

    @ManagedAttributeField
    private Result _defaultResult;
    @ManagedAttributeField
    private volatile List<AclRule> _rules;

    AbstractCommonRuleBasedAccessControlProvider(final Map<String, Object> attributes, final T parent)
    {
        super(attributes, parent);
    }

    @Override
    protected void postSetAttributes(final Set<String> actualUpdatedAttributes)
    {
        super.postSetAttributes(actualUpdatedAttributes);
        if (actualUpdatedAttributes.contains(RuleBasedVirtualHostAccessControlProvider.DEFAULT_RESULT)
            || actualUpdatedAttributes.contains(RuleBasedVirtualHostAccessControlProvider.RULES))
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
        return new RuleBasedAccessControl(new RuleSet(this, rules, _defaultResult), getModel());
    }

    public Result getDefaultResult()
    {
        return _defaultResult;
    }

    public List<AclRule> getRules()
    {
        return _rules;
    }


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
            return _rule.getAttributes();
        }

        @Override
        public RuleOutcome getOutcome()
        {
            return _rule.getRuleOutcome();
        }
    }

    public Content extractRules()
    {
        StringBuilder sb = new StringBuilder();
        switch (_defaultResult)
        {
            case DENIED:
                // This is the default assumed by ResultSet for ACL files without a CONFIG directive
                break;
            case ALLOWED:
            case DEFER:
                sb.append(String.format("CONFIG %s=true\n", _defaultResult == Result.ALLOWED ? AclFileParser.DEFAULT_ALLOW : AclFileParser.DEFAULT_DEFER));
                break;
        }
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
        return new StringContent(getName(), sb.toString());
    }

    private static class StringContent implements Content, CustomRestHeaders
    {
        private final static DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss");
        private final String _content;
        private final String _name;

        public StringContent(final String name, final String content)
        {
            _content = content;
            _name = name;
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

        @RestContentHeader("Content-Disposition")
        public String getContentDisposition()
        {
            return String.format("attachment; filename=\"%s-%s.acl\"", _name, FORMATTER.format(LocalDateTime.now()));
        }

        @Override
        public void release()
        {

        }
    }

}
