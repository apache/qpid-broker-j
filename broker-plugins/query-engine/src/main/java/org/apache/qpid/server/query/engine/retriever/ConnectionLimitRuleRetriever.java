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
package org.apache.qpid.server.query.engine.retriever;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.qpid.server.model.BrokerConnectionLimitProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.limit.ConnectionLimitProvider;
import org.apache.qpid.server.user.connection.limits.plugins.ConnectionLimitRule;
import org.apache.qpid.server.user.connection.limits.plugins.FileBasedBrokerConnectionLimitProvider;
import org.apache.qpid.server.user.connection.limits.plugins.RuleBasedBrokerConnectionLimitProvider;

/**
 * Retrieves ConnectionLimitRule entities
 *
 * @param <C> Descendant of ConfiguredObject
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ConnectionLimitRuleRetriever<C extends ConfiguredObject<?>> extends ConfiguredObjectRetriever<C> implements EntityRetriever<C>
{
    /**
     * Target type
     */
    @SuppressWarnings({"java:S1170", "rawtypes", "RedundantCast", "unchecked"})
    private final Class<C> _type = (Class<C>) (Class<? extends ConfiguredObject>) BrokerConnectionLimitProvider.class;

    /**
     * List of entity field names
     */
    private final List<String> _fieldNames = List.of("blocked", "countLimit", "frequencyLimit", "frequencyPeriod",
            "identity", "port");

    /**
     * Mapping function for a ConnectionLimitRule
     */
    private final Function<ConnectionLimitRule, Map<String, Object>> _connectionLimitRuleMapping = rule -> Map.of(
        _fieldNames.get(0), rule.getBlocked(),
        _fieldNames.get(1), rule.getCountLimit(),
        _fieldNames.get(2), rule.getFrequencyLimit(),
        _fieldNames.get(3), rule.getFrequencyPeriod(),
        _fieldNames.get(4), rule.getIdentity(),
        _fieldNames.get(5), rule.getPort());

    /**
     * Returns stream of ConnectionLimitRule entities
     *
     * @param broker Broker instance
     *
     * @return Stream of entities
     */
    @Override()
    public Stream<Map<String, ?>> retrieve(final C broker)
    {
        final Stream<ConnectionLimitProvider<?>> stream = retrieve(broker, _type)
            .map(child -> ((BrokerConnectionLimitProvider<?>)child));
        return stream.flatMap(provider  ->
        {
            if (provider instanceof FileBasedBrokerConnectionLimitProvider)
            {
                return ((FileBasedBrokerConnectionLimitProvider<?>)provider).getRules().stream().map(_connectionLimitRuleMapping);
            }
            if (provider instanceof RuleBasedBrokerConnectionLimitProvider)
            {
                return ((RuleBasedBrokerConnectionLimitProvider<?>)provider).getRules().stream().map(_connectionLimitRuleMapping);
            }
            return Stream.empty();
        });
    }

    /**
     * Returns list of entity field names
     *
     * @return List of field names
     */
    @Override()
    @SuppressWarnings("findbugs:EI_EXPOSE_REP")
    // List of field names already is an immutable collection
    public List<String> getFieldNames()
    {
        return _fieldNames;
    }
}
