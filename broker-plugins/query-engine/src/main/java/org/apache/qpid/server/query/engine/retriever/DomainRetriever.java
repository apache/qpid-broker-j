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
import java.util.stream.Stream;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;

/**
 * Retrieves domain entities (metadata containing queryable object types)
 *
 * @param <C> Descendant of ConfiguredObject
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class DomainRetriever<C extends ConfiguredObject<?>> extends ConfiguredObjectRetriever<C> implements EntityRetriever<C>
{
    /**
     * List of entity field names
     */
    private final List<String> _fieldNames = List.of("name");

    /**
     * Returns stream of Domain entities
     *
     * @param broker Broker instance
     *
     * @return Stream of entities
     */
    @Override
    public Stream<Map<String, ?>> retrieve(final C broker)
    {
        return broker.getModel().getDescendantCategories(Broker.class).stream()
            .map(type -> Map.of("name", type.getSimpleName()));
    }

    /**
     * Returns list of entity field names
     *
     * @return List of field names
     */
    @Override
    @SuppressWarnings("findbugs:EI_EXPOSE_REP")
    // List of field names already is an immutable collection
    public List<String> getFieldNames()
    {
        return _fieldNames;
    }
}
