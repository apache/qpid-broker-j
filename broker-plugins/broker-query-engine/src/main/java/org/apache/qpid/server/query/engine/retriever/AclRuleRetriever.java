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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.access.config.AclFileParser;
import org.apache.qpid.server.security.access.config.Rule;
import org.apache.qpid.server.security.access.config.RuleSet;
import org.apache.qpid.server.security.access.plugins.AclFileAccessControlProvider;

/**
 * Retrieves Rule entities
 *
 * @param <C> Descendant of ConfiguredObject
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class AclRuleRetriever<C extends ConfiguredObject<?>> extends ConfiguredObjectRetriever<C> implements EntityRetriever<C>
{
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AclRuleRetriever.class);

    /**
     * Target type
     */
    @SuppressWarnings({"java:S1170", "rawtypes", "RedundantCast", "unchecked"})
    private final Class<C> _type = (Class<C>) (Class<? extends ConfiguredObject>) AccessControlProvider.class;

    /**
     * List of entity field names
     */
    private final List<String> _fieldNames = Arrays.asList(
        "identity", "attributes", "objectType", "operation", "outcome"
    );

    /**
     * Mapping function for a Rule
     */
    private final Function<Rule, Map<String, Object>> _ruleMapping = rule -> ImmutableMap.<String, Object> builder()
        .put(_fieldNames.get(0), rule.getIdentity())
        .put(_fieldNames.get(1), rule.getAttributes())
        .put(_fieldNames.get(2), rule.getObjectType())
        .put(_fieldNames.get(3), rule.getOperation())
        .put(_fieldNames.get(4), rule.getOutcome())
        .build();

    /**
     * Cached RuleSet
     */
    private RuleSet _ruleSet;

    /**
     * Returns stream of AclRule / Rule entities
     *
     * @param broker Broker instance
     *
     * @return Stream of entities
     */
    @Override()
    public Stream<Map<String, ?>> retrieve(final C broker)
    {
        final Stream<AccessControlProvider<?>> stream = retrieve(broker, _type).map(child -> (AccessControlProvider<?>) child);
        return stream.flatMap(provider ->
        {
            if (provider instanceof AclFileAccessControlProvider)
            {
                try
                {
                    if (_ruleSet == null)
                    {
                        _ruleSet = AclFileParser.parse(((AclFileAccessControlProvider<?>) provider).getPath(), EventLogger::new);
                    }
                    return _ruleSet.stream().map(_ruleMapping);
                }
                catch (IllegalConfigurationException e)
                {
                    LOGGER.error("Error when retrieving ACL rules", e);
                    return Stream.empty();
                }
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
