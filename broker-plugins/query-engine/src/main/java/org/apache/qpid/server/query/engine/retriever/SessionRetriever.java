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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Session;

/**
 * Retrieves Session entities
 *
 * @param <C> Descendant of ConfiguredObject
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class SessionRetriever<C extends ConfiguredObject<?>> extends ConfiguredObjectRetriever<C> implements EntityRetriever<C>
{
    /**
     * Target type
     */
    @SuppressWarnings({"java:S1170", "rawtypes", "RedundantCast", "unchecked"})
    private final Class<C> _type = (Class<C>) (Class<? extends ConfiguredObject>) Connection.class;

    /**
     * List of entity field names
     */
    private final List<String> _fieldNames = new ImmutableList.Builder<String>()
        .add("connectionId")
        .add("id")
        .add("name")
        .add("description")
        .add("type")
        .add("desiredState")
        .add("state")
        .add("durable")
        .add("lifetimePolicy")
        .add("channelId")
        .add("lastOpenedTime")
        .add("producerFlowBlocked")
        .add("lastUpdatedTime")
        .add("lastUpdatedBy")
        .add("createdBy")
        .add("createdTime")
        .add("statistics")
        .build();

    /**
     * Mapping function for a Session
     */
    private final BiFunction<ConfiguredObject<?>, Session<?>, Map<String, Object>> _sessionMapping =
        (ConfiguredObject<?> parent, Session<?> session) ->
        {
            final Map<String, Object> result = new LinkedHashMap<>();
            result.put(_fieldNames.get(0), parent.getId());
            result.put(_fieldNames.get(1), session.getId());
            result.put(_fieldNames.get(2), session.getName());
            result.put(_fieldNames.get(3), session.getDescription());
            result.put(_fieldNames.get(4), session.getType());
            result.put(_fieldNames.get(5), session.getDesiredState());
            result.put(_fieldNames.get(6), session.getState());
            result.put(_fieldNames.get(7), session.isDurable());
            result.put(_fieldNames.get(8), session.getLifetimePolicy());
            result.put(_fieldNames.get(9), session.getChannelId());
            result.put(_fieldNames.get(10), session.getLastOpenedTime());
            result.put(_fieldNames.get(11), session.isProducerFlowBlocked());
            result.put(_fieldNames.get(12), session.getLastUpdatedTime());
            result.put(_fieldNames.get(13), session.getLastUpdatedBy());
            result.put(_fieldNames.get(14), session.getCreatedBy());
            result.put(_fieldNames.get(15), session.getCreatedTime());
            result.put(_fieldNames.get(16), session.getStatistics());
            return result;
        };

    /**
     * Returns stream of Session entities
     *
     * @param broker Broker instance
     *
     * @return Stream of entities
     */
    @Override()
    public Stream<Map<String, ?>> retrieve(final C broker)
    {
        final Stream<Connection<?>> stream = retrieve(broker, _type).map(connection -> ((Connection<?>) connection));
        return stream.flatMap(connection -> connection.getSessions().stream()
            .map(session -> _sessionMapping.apply(connection, session)));
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
