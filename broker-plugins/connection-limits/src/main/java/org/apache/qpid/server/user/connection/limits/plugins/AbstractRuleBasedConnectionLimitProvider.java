/*
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
 */
package org.apache.qpid.server.user.connection.limits.plugins;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.CustomRestHeaders;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.limit.ConnectionLimitProvider;
import org.apache.qpid.server.user.connection.limits.config.FileParser;
import org.apache.qpid.server.user.connection.limits.config.Rule;
import org.apache.qpid.server.user.connection.limits.config.RulePredicates.Property;
import org.apache.qpid.server.user.connection.limits.config.RuleSetCreator;

public abstract class AbstractRuleBasedConnectionLimitProvider<C extends AbstractRuleBasedConnectionLimitProvider<C>>
        extends AbstractConnectionLimitProvider<C> implements RuleBasedConnectionLimitProvider<C>
{
    private static final String RULES = "rules";

    static final String RULE_BASED_TYPE = "RuleBased";

    @ManagedAttributeField
    private Long _defaultFrequencyPeriod;

    @ManagedAttributeField
    private List<ConnectionLimitRule> _rules = new ArrayList<>();

    public AbstractRuleBasedConnectionLimitProvider(ConfiguredObject<?> parent, Map<String, Object> attributes)
    {
        super(parent, attributes);
    }

    @Override
    public List<ConnectionLimitRule> getRules()
    {
        return Collections.unmodifiableList(_rules);
    }

    @Override
    public Long getDefaultFrequencyPeriod()
    {
        return Optional.ofNullable(_defaultFrequencyPeriod).orElseGet(
                () -> getContextValue(Long.class, ConnectionLimitProvider.CONNECTION_FREQUENCY_PERIOD));
    }

    @Override
    public Content extractRules()
    {
        return new StringContent(getName(), getDefaultFrequencyPeriod(), getRules());
    }

    @Override
    public void loadFromFile(String path)
    {
        authorise(Operation.UPDATE);

        final List<ConnectionLimitRule> connectionLimitRules = new ArrayList<>(getRules());
        for (final Rule rule : FileParser.parse(path).updateRulesWithDefaultFrequencyPeriod())
        {
            connectionLimitRules.add(new ConnectionLimitRuleImpl(rule));
        }
        changeAttributes(Collections.singletonMap(RULES, connectionLimitRules));
    }

    @Override
    public void clearRules()
    {
        authorise(Operation.UPDATE);
        changeAttributes(Collections.singletonMap(RULES, new ArrayList<ConnectionLimitRule>()));
    }

    @Override
    public void resetCounters()
    {
        changeAttributes(Collections.emptyMap());
    }

    @Override
    protected void postSetAttributes(final Set<String> actualUpdatedAttributes)
    {
        super.postSetAttributes(actualUpdatedAttributes);
        forceNewRuleSetCreator();
    }

    @Override
    protected RuleSetCreator newRuleSetCreator()
    {
        final RuleSetCreator creator = new RuleSetCreator();
        creator.setDefaultFrequencyPeriod(getDefaultFrequencyPeriod());
        for (final ConnectionLimitRule rule : getRules())
        {
            creator.add(Rule.newInstance(rule));
        }
        return creator;
    }

    private static final class StringContent implements Content, CustomRestHeaders
    {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss");
        private final String _name;
        private final long _defaultFrequencyPeriod;
        private final List<ConnectionLimitRule> _rules;

        StringContent(String name, long defaultFrequencyPeriod, List<? extends ConnectionLimitRule> rules)
        {
            _name = Objects.requireNonNull(name);
            _defaultFrequencyPeriod = defaultFrequencyPeriod;
            _rules = new ArrayList<>(rules);
        }

        @Override
        public void write(final OutputStream outputStream) throws IOException
        {
            final byte[] lineSeparator = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

            outputStream.write(
                    String.format("CONFIG %s=%d", FileParser.DEFAULT_FREQUENCY_PERIOD, _defaultFrequencyPeriod)
                            .getBytes(StandardCharsets.UTF_8));
            outputStream.write(lineSeparator);

            for (final ConnectionLimitRule rule : _rules)
            {
                outputStream.write(convertToString(rule).getBytes(StandardCharsets.UTF_8));
                outputStream.write(lineSeparator);
            }
        }

        @RestContentHeader("Content-Type")
        public String getContentType()
        {
            return "text/plain";
        }

        @RestContentHeader("Content-Disposition")
        public String getContentDisposition()
        {
            return String.format("attachment; filename=\"%s-%s.clt\"", _name, FORMATTER.format(LocalDateTime.now()));
        }

        @Override
        public void release()
        {
            // Nothing to do
        }

        private String convertToString(ConnectionLimitRule rule)
        {
            final String prefix = FileParser.CONNECTION_LIMIT.toUpperCase(Locale.ENGLISH);
            final StringBuilder builder = new StringBuilder();
            builder.append(String.format("%s %s", prefix, rule.getIdentity()));
            if (Boolean.TRUE.equals(rule.getBlocked()))
            {
                builder.append(String.format(" %s", FileParser.BLOCK));
            }
            else
            {
                appendCountLimit(builder, rule);
                appendFrequencyLimit(builder, rule);
            }
            appendPort(builder, rule);
            return builder.toString();
        }

        private void appendPort(StringBuilder builder, ConnectionLimitRule rule)
        {
            if (rule.getPort() != null)
            {
                builder.append(String.format(" %s=%s", Property.PORT, rule.getPort()));
            }
        }

        private void appendFrequencyLimit(StringBuilder builder, ConnectionLimitRule rule)
        {
            if (rule.getFrequencyLimit() != null)
            {
                if (rule.getFrequencyPeriod() == null)
                {
                    builder.append(
                            String.format(" %s=%d", Property.CONNECTION_FREQUENCY_LIMIT, rule.getFrequencyLimit()));
                }
                else
                {
                    builder.append(String.format(" %s=%d/%s",
                            Property.CONNECTION_FREQUENCY_LIMIT,
                            rule.getFrequencyLimit(),
                            Duration.ofMillis(rule.getFrequencyPeriod())));
                }
            }
        }

        private void appendCountLimit(StringBuilder builder, ConnectionLimitRule rule)
        {
            if (rule.getCountLimit() != null)
            {
                builder.append(String.format(" %s=%d", Property.CONNECTION_LIMIT, rule.getCountLimit()));
            }
        }
    }
}
