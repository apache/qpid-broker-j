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
package org.apache.qpid.server.query.engine.parsing.factory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.collector.CollectorType;

/**
 * Factory creating collectors for aggregation functions
 */
public final class CollectorFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private CollectorFactory()
    {

    }

    /**
     * Creates collector for MAX function
     *
     * @param sourceExpression Source expression
     *
     * @param <T> Input parameter type
     *
     * @return Collector instance
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <T> Collector maximizing(final Expression<T, ?> sourceExpression)
    {
        final Predicate<T> notNullPredicate = Objects::nonNull;
        final Comparator comparator = Comparator.naturalOrder();
        Collector collector = Collectors.maxBy(comparator);
        collector = filtering(notNullPredicate, collector);
        collector = Collectors.mapping(object -> sourceExpression.apply((T) object), collector);
        return Collectors.collectingAndThen(collector, opt -> ((Optional) opt).orElse(null));
    }

    /**
     * Creates collector for MIN function
     *
     * @param sourceExpression Source expression
     *
     * @param <T> Input parameter type
     *
     * @return Collector instance
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <T> Collector minimizing(final Expression<T, ?> sourceExpression)
    {
        final Predicate<T> notNullPredicate = Objects::nonNull;
        final Comparator comparator = Comparator.naturalOrder();
        Collector collector = Collectors.minBy(comparator);
        collector = filtering(notNullPredicate, collector);
        collector = Collectors.mapping(object -> sourceExpression.apply((T) object), collector);
        return Collectors.collectingAndThen(collector, opt -> ((Optional) opt).orElse(null));
    }

    /**
     * Creates collector for COUNT function
     *
     * @param sourceExpression Source expression
     *
     * @param <T> Input parameter type
     *
     * @return Collector instance
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <T> Collector counting(final Expression<T, ?> sourceExpression)
    {
        final Predicate<T> notNullPredicate = Objects::nonNull;
        Collector collector = Collectors.counting();
        collector = filtering(notNullPredicate, collector);
        return Collectors.mapping(object -> sourceExpression.apply((T) object), collector);
    }

    /**
     * Creates collector for SUM function
     *
     * @param sourceExpression Source expression
     *
     * @param <T> Input parameter type
     *
     * @return Collector instance
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <T> Collector summing(final Expression<T, ?> sourceExpression)
    {
        final Predicate<T> notNullPredicate = Objects::nonNull;
        Collector collector = Collectors.summingDouble(item -> ((Number) item).doubleValue());
        collector = filtering(notNullPredicate, collector);
        return Collectors.mapping(object -> sourceExpression.apply((T) object), collector);
    }

    /**
     * Creates collector for AVG function
     *
     * @param sourceExpression Source expression
     *
     * @param <T> Input parameter type
     *
     * @return Collector instance
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <T, R> Collector averaging(final Expression<T, R> sourceExpression)
    {
        final Predicate<T> notNullPredicate = Objects::nonNull;
        Collector collector = Collectors.averagingDouble(item -> ((Number) item).doubleValue());
        collector = filtering(notNullPredicate, collector);
        return Collectors.mapping(object -> sourceExpression.apply((T) object), collector);
    }

    /**
     * Mapping between CollectorType and collector creator
     *
     * @param <T> Input parameter type
     * @param <A> Accumulator type
     * @param <R> Return parameter type
     *
     * @return Collector creator
     */
    private static <T, A, R> Map<CollectorType, Function<Expression<T, R>, Collector<T, A, R>>> map()
    {
        final Map<CollectorType, Function<Expression<T, R>, Collector<T, A, R>>> map = new HashMap<>();
        map.put(CollectorType.AVERAGING, CollectorFactory::averaging);
        map.put(CollectorType.COUNTING, CollectorFactory::counting);
        map.put(CollectorType.MAXIMIZING, CollectorFactory::maximizing);
        map.put(CollectorType.MINIMIZING, CollectorFactory::minimizing);
        map.put(CollectorType.SUMMING, CollectorFactory::summing);
        return map;
    }

    /**
     * Creates collector from collector type
     *
     * @param collectorType Collector type
     *
     * @param <T> Input parameter type
     * @param <A> Accumulator type
     * @param <R> Return parameter type
     *
     * @return Function creating collector
     */
    public static <T, A, R> Function<Expression<T, R>, Collector<T, A, R>> collector(final CollectorType collectorType)
    {
        Objects.requireNonNull(collectorType, Errors.VALIDATION.COLLECTOR_TYPE_NULL);
        return CollectorFactory.<T, A, R>map().get(collectorType);
    }

    /**
     * Filtering collector
     *
     * @param filter Predicate to filter
     * @param collector Collector in chain
     *
     * @param <T> Input parameter type
     * @param <A> Accumulator type
     * @param <R> Return parameter type
     *
     * @return Collector instance
     */
    public static <T, A, R> Collector<T, A, R> filtering(
        final Predicate<? super T> filter,
        final Collector<T, A, R> collector
    )
    {
        return Collector.of(
            collector.supplier(),
            (accumulator, input) ->
            {
                if (filter.test(input)) {
                    collector.accumulator().accept(accumulator, input);
                }
            },
            collector.combiner(),
            collector.finisher());
    }
}
