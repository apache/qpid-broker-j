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
package org.apache.qpid.server.query.engine.evaluator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.query.engine.parsing.converter.ImplicitConverter;
import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.AbstractAggregationExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.query.ProjectionExpression;
import org.apache.qpid.server.query.engine.parsing.collector.CollectorType;
import org.apache.qpid.server.query.engine.parsing.factory.CollectorFactory;
import org.apache.qpid.server.query.engine.parsing.query.OrderItem;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Evaluates select expression
 */
public class SelectEvaluator
{
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectExpression.class);

    /**
     * Evaluates select expression
     *
     * @param selectExpression SelectExpression instance
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Stream of maps
     */
    @SuppressWarnings("unchecked")
    public <T, R> Stream<Map<String, R>> evaluate(final SelectExpression<T, R> selectExpression)
    {
        LOGGER.debug("Executing select '{}'", selectExpression);

        final EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();

        final boolean addAlias = selectExpression.getFrom() != null
            && !Objects.equals(selectExpression.getFrom().getAlias(), selectExpression.getFrom().toString());

        Stream<T> stream = selectExpression.getFrom() == null
            ? Stream.empty()
            : (Stream<T>) selectExpression.getFrom().get()
                .peek(item ->  {
                    if (addAlias)
                    {
                        ctx.putAlias(selectExpression.getFrom().getAlias(), new ConstantExpression<>(item));
                    }
                });

        if (selectExpression.getWhere() != null)
        {
            stream = stream.filter(selectExpression.getWhere());
        }

        return mapProjections(selectExpression, stream);
    }

    /**
     * Maps projections (items of the SELECT clause) according to the select expression structure
     *
     * @param selectExpression Select expression
     * @param stream Stream of entities to map
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Stream of maps
     */
    @SuppressWarnings("unchecked")
    private <T, R> Stream<Map<String, R>> mapProjections(
        final SelectExpression<T, R> selectExpression,
        final Stream<T> stream
                                                        )
    {
        final EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        final List<String> projectionAliases = selectExpression.getProjections().stream()
            .map(ProjectionExpression::getAlias).collect(Collectors.toList());

        // when ORDER BY clause has items not contained in the SELECT clause, they should be added there
        // (and removed during the sorting process)
        if (!selectExpression.isSelectAll() && selectExpression.getParent() == null)
        {
            final List<OrderItem<T, R>> orderItems = ctx.get(EvaluationContext.QUERY_ORDERING);
            final List<ProjectionExpression<T, R>> additionalProjections = orderItems == null
                ? new ArrayList<>()
                : orderItems.stream().filter(item -> !item.isAliasOrdinal())
                    .filter(item -> !projectionAliases.contains(item.getAlias()))
                    .map(item -> new ProjectionExpression<>(item.getAlias(), item.getExpression(), selectExpression.getOrdinal()))
                    .collect(Collectors.toList());
            projectionAliases.addAll(additionalProjections.stream().map(ProjectionExpression::getAlias).collect(Collectors.toList()));
            selectExpression.getProjections().addAll(additionalProjections);
            ctx.put(EvaluationContext.QUERY_ITEMS_FOR_REMOVAL, additionalProjections);
        }

        final List<ProjectionExpression<T, R>> projections = new ArrayList<>(selectExpression.getProjections());

        if (selectExpression.hasAggregationItems() && selectExpression.getGroupBy().isEmpty())
        {
            return Stream.of(aggregate(selectExpression, stream));
        }

        if (!selectExpression.getGroupBy().isEmpty())
        {
            return Stream.of(groupBy(selectExpression, stream));
        }

        final List<String> aliases = selectExpression.isSelectAll()
            ? selectExpression.getFrom().getFieldNames()
            : projectionAliases;

        final List<ProjectionExpression<T, R>> values = selectExpression.isSelectAll()
            ? selectExpression.getFrom().getProjections(aliases, (SelectExpression<T, Stream<?>>) selectExpression)
            : projections;

        Stream<Map<String, R>> mapped = selectExpression.getFrom() == null
            ? IntStream.range(0, projections.size()).mapToObj(item -> toMap(aliases, values, null))
            : stream.map(item -> toMap(aliases, values, item));

        if (selectExpression.isDistinct())
        {
            mapped = mapped.distinct();
        }

        return mapped;
    }

    /**
     * Transforms entities to map instances
     *
     * @param aliases List of aliases (entity field names)
     * @param projections List of projection expressions (function retrieving entity field values)
     * @param item Object to handle
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Map containing entity fields
     */
    private <T, R> Map<String, R> toMap(final List<String> aliases, final List<ProjectionExpression<T, R>> projections, T item)
    {
        return IntStream.range(0, projections.size()).boxed()
            .collect(
                LinkedHashMap::new,
                (map, index) -> map.put(aliases.get(index), ImplicitConverter.convert(projections.get(index).apply(item))),
                LinkedHashMap::putAll
            );
    }

    /**
     * Aggregates stream without grouping
     *
     * @param selectExpression Select expression
     * @param stream Stream to be aggregated
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Map representing aggregation result
     */
    private <T, R> Map<String, R> aggregate(final SelectExpression<T, R> selectExpression, final Stream<T> stream)
    {
        List<T> items = stream.collect(Collectors.toList());
        if (selectExpression.getHaving() != null)
        {
            selectExpression.getHaving().applyAggregation(selectExpression, items);
            items = items.stream().filter(selectExpression.getHaving()).collect(Collectors.toList());
        }
        final List<T> filtered = items;
        return IntStream.range(0, selectExpression.getProjections().size()).boxed()
            .map(index -> selectExpression.getProjections().get(index))
            .collect(
                LinkedHashMap::new,
                (map, projection) -> map.put(
                    projection.getAlias(),
                    projection.applyAggregation(selectExpression, filtered)
                ),
                LinkedHashMap::putAll
            );
    }

    /**
     * Aggregates stream with grouping
     *
     * @param selectExpression Select expression
     * @param stream Stream to be aggregated and grouped
     *
     * @param <T> Input parameter type
     * @param <A> Collector accumulator type
     * @param <R> Return parameter type
     *
     * @return Map representing aggregation result
     */
    @SuppressWarnings("unchecked")
    private <T, A, R> Map<String, R> groupBy(final SelectExpression<T, R> selectExpression, final Stream<T> stream)
    {

        final List<T> items = stream.collect(Collectors.toList());
        final List<ProjectionExpression<T, R>> groupByItems = selectExpression.getGroupBy();
        final List<ProjectionExpression<T, R>> aggregationItems = selectExpression.getAggregationItems();

        final EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        selectExpression.getProjections().forEach(item -> ctx.putAlias(item.getAlias(), item));

        if (selectExpression.getHaving() != null)
        {
            selectExpression.getHaving().applyAggregation(selectExpression, items);
        }

        final Map<String, R> result = new LinkedHashMap<>();

        for (final ProjectionExpression<T, R> projection : aggregationItems)
        {
            for (final AbstractAggregationExpression<T, R> aggregation : projection.<R>getAggregations())
            {
                final Expression<T, R> sourceExpression = aggregation.getSource();
                final CollectorType collectorType = aggregation.getCollectorType();

                Collector<T, A, R> collector = CollectorFactory.<T, A, R>collector(collectorType).apply(sourceExpression);

                for (int i = groupByItems.size() - 1; i >= 0; i--)
                {
                    collector = (Collector<T, A, R>) Collectors.groupingBy(groupByItems.get(i), collector);
                    collector = Collectors.collectingAndThen(collector, object ->
                    {
                        if (object instanceof Map)
                        {
                            for (final Map.Entry<String, R> entry : ((Map<String, R>) object).entrySet())
                            {
                                if (entry.getValue() instanceof Number)
                                {
                                    aggregation.setValue(entry.getValue());
                                    entry.setValue((R) NumberConverter.narrow((Number) projection.apply(null)));
                                }
                            }
                        }
                        return object;
                    });
                    if (selectExpression.getHaving() != null)
                    {
                        collector = (Collector<T, A, R>) CollectorFactory.filtering(selectExpression.getHaving(), collector);
                    }
                }

                result.put(projection.getAlias(), items.stream().collect(collector));
            }
        }

        if (selectExpression.getHaving() != null)
        {
            for (final Map.Entry<String, R> entry : result.entrySet())
            {
                selectExpression.getHaving().filter((Map<String, R>) entry.getValue());
            }
        }

        ctx.put(EvaluationContext.QUERY_AGGREGATED_RESULT, Boolean.TRUE);

        return result;
    }
}
