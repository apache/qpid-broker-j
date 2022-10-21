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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;

/**
 * Retrieves various ConfiguredObject entities
 *
 * @param <C> Descendant of ConfiguredObject
 */
public class ConfiguredObjectRetriever<C extends ConfiguredObject<?>>
{
    /**
     * Returns stream of ConfiguredObject entities
     *
     * @param parent Parent object
     * @param category Desired child type
     *
     * @return Stream of entities
     */
    public Stream<C> retrieve(final C parent, final Class<C> category)
    {
        final List<Class<C>> hierarchy = hierarchy(category, parent.getModel()).collect(Collectors.toList());
        return of(hierarchy, parent, retrieval(), category);
    }

    /**
     * Returns stream of classes from parent to descendant
     *
     * @param category Desired child type
     * @param model Parent object model
     *
     * @return Stream of classes from parent to descendant
     */
    @SuppressWarnings("unchecked")
    private Stream<Class<C>> hierarchy(final Class<C> category, final Model model)
    {
        final Class<C> parent = (Class<C>) model.getParentType(category);
        return parent == null || Objects.equals(category, Broker.class) ? Stream.empty() : Stream.concat(Stream.of(category), hierarchy(parent, model));
    }

    /**
     * Retrieval function
     *
     * @return Function for descendant retrieval from ConfiguredObject
     */
    private BiFunction<List<Class<C>>, C, Collection<? extends C>> retrieval()
    {
        return (hierarchy, co) -> hierarchy.isEmpty() ? new ArrayList<>() : co.getChildren(hierarchy.get(hierarchy.size() - 1));
    }

    /**
     *
     * @param hierarchy List of classes in a hierarchical order
     * @param node Current node in the object hierarchy
     * @param retrieval Retrieval function
     * @param category Target child type
     *
     * @return Stream of entities belonging to desired child type
     */
    private Stream<C> of(
        final List<Class<C>> hierarchy,
        final C node,
        final BiFunction<List<Class<C>>, C, Collection<? extends C>> retrieval,
        final Class<C> category
    )
    {
        return Stream.concat(
            category.isAssignableFrom(node.getClass()) ? Stream.of(node) : Stream.empty(),
            retrieval.apply(hierarchy, node).stream().flatMap(n -> of(hierarchy.subList(0, hierarchy.size() - 1), n, retrieval, category))
        );
    }
}
