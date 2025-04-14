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
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;

/**
 * Retrieves Binding entities
 *
 * @param <C> Descendant of ConfiguredObject
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class BindingRetriever<C extends ConfiguredObject<?>> extends ConfiguredObjectRetriever<C> implements EntityRetriever<C>
{
    /**
     * Target type
     */
    @SuppressWarnings({"java:S1170", "rawtypes", "RedundantCast", "unchecked"})
    private final Class<C> _type = (Class<C>) (Class<? extends ConfiguredObject>) Exchange.class;

    /**
     * List of entity field names
     */
    private final List<String> _fieldNames = List.of("exchange", "bindingKey", "name", "type", "arguments", "destination");

    /**
     * Mapping function for a Binding
     */
    private final BiFunction<ConfiguredObject<?>, Binding, Map<String, Object>> _bindingMapping =
        (ConfiguredObject<?> parent, Binding binding) -> Map.of(
            _fieldNames.get(0), parent.getName(),
            _fieldNames.get(1), binding.getBindingKey(),
            _fieldNames.get(2), binding.getName(),
            _fieldNames.get(3), binding.getType(),
            _fieldNames.get(4), binding.getArguments(),
            _fieldNames.get(5), binding.getDestination());

    /**
     * Returns stream of Binding entities
     *
     * @param broker Broker instance
     *
     * @return Stream of entities
     */
    @Override()
    public Stream<Map<String, ?>> retrieve(final C broker)
    {
        final Stream<Exchange<?>> stream = retrieve(broker, _type).map(exchange -> ((Exchange<?>) exchange));
        return stream.flatMap(exchange  -> exchange.getBindings().stream()
            .map(binding -> _bindingMapping.apply(exchange, binding)));
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
