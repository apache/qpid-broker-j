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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.limit.ConnectionLimitProvider;
import org.apache.qpid.server.user.connection.limits.config.FileParser;
import org.apache.qpid.server.user.connection.limits.config.Rule;
import org.apache.qpid.server.user.connection.limits.config.RuleSetCreator;

public abstract class AbstractFileBasedConnectionLimitProvider<C extends AbstractFileBasedConnectionLimitProvider<C>>
        extends AbstractConnectionLimitProvider<C> implements FileBasedConnectionLimitProvider<C>
{
    static final String FILE_PROVIDER_TYPE = "ConnectionLimitFile";

    @ManagedAttributeField(afterSet = "reloadSourceFile")
    private String _path;

    public AbstractFileBasedConnectionLimitProvider(ConfiguredObject<?> parent, Map<String, Object> attributes)
    {
        super(parent, attributes);
    }

    @Override
    public String getPath()
    {
        return _path;
    }

    @Override
    public void reload()
    {
        authorise(Operation.UPDATE);
        reloadSourceFile();
    }

    @Override
    public Long getDefaultFrequencyPeriod()
    {
        return Optional.ofNullable(creator())
                .map(RuleSetCreator::getDefaultFrequencyPeriod)
                .orElseGet(() -> getContextValue(Long.class, ConnectionLimitProvider.CONNECTION_FREQUENCY_PERIOD));
    }

    @Override
    public List<ConnectionLimitRule> getRules()
    {
        return Optional.<List<Rule>>ofNullable(creator()).orElseGet(Collections::emptyList)
                .stream().map(ConnectionLimitRuleImpl::new).collect(Collectors.toList());
    }

    @Override
    public void resetCounters()
    {
        changeAttributes(Collections.emptyMap());
    }

    private void reloadSourceFile()
    {
        try
        {
            forceNewRuleSetCreator();
            // force the change listener to fire, causing the parent broker to update its cache
            changeAttributes(Collections.emptyMap());
        }
        catch (RuntimeException e)
        {
            throw new IllegalConfigurationException("Failed to reload the connection limit file", e);
        }
    }

    @Override
    protected RuleSetCreator newRuleSetCreator()
    {
        final RuleSetCreator creator = FileParser.parse(getPath());
        if (!creator.isDefaultFrequencyPeriodSet())
        {
            creator.setDefaultFrequencyPeriod(
                    getContextValue(Long.class, ConnectionLimitProvider.CONNECTION_FREQUENCY_PERIOD));
        }
        return creator;
    }
}
