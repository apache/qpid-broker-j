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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.query.engine.evaluator.settings.DefaultQuerySettings;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.ExpressionParser;
import org.apache.qpid.server.query.engine.parsing.ParseException;
import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.accessor.MapObjectAccessor;
import org.apache.qpid.server.query.engine.parsing.expression.set.SetExpression;
import org.apache.qpid.server.query.engine.parsing.query.ProjectionExpression;
import org.apache.qpid.server.query.engine.parsing.query.Order;
import org.apache.qpid.server.query.engine.parsing.query.OrderItem;
import org.apache.qpid.server.query.engine.parsing.query.QueryExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;
import org.apache.qpid.server.query.engine.validation.QueryExpressionValidator;

/**
 * Parses and evaluates query
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class QueryEvaluator
{
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryEvaluator.class);

    /**
     * Broker instance
     */
    private final Broker<?> _broker;

    /**
     * Default query settings
     */
    private final QuerySettings _defaultQuerySettings;

    /**
     * Cached queries
     */
    private final Map<String, QueryExpression<?, ?>> _queryCache;

    /**
     * Constructor stores broker instance
     *
     * @param broker Broker instance
     */
    // mutable broker instance is stored intentionally
    @SuppressWarnings("findbugs:EI_EXPOSE_REP2")
    public QueryEvaluator(final Broker<?> broker)
    {
        Objects.requireNonNull(broker, Errors.EVALUATION.BROKER_NOT_SUPPLIED);
        _broker = broker;
        _defaultQuerySettings = new QuerySettings();
        _queryCache = null;
    }

    /**
     * Constructor stores field values
     *
     * @param queryCache Query cache
     * @param defaultQuerySettings Default query settings
     * @param broker Broker instance
     */
    // mutable broker instance is stored intentionally
    @SuppressWarnings("findbugs:EI_EXPOSE_REP2")
    public QueryEvaluator(
        final Map<String, QueryExpression<?, ?>> queryCache,
        final QuerySettings defaultQuerySettings,
        final Broker<?> broker
    )
    {
        Objects.requireNonNull(defaultQuerySettings, Errors.EVALUATION.DEFAULT_QUERY_SETTINGS_NOT_SUPPLIED);
        Objects.requireNonNull(broker, Errors.EVALUATION.BROKER_NOT_SUPPLIED);
        _broker = broker;
        _defaultQuerySettings = defaultQuerySettings;
        _queryCache = queryCache;
    }

    /**
     * Evaluates query using default settings
     *
     * @param sql SQL query
     * @param <R> Return parameter type
     *
     * @return EvaluationResult instance
     */
    public <R> EvaluationResult<R> execute(final String sql)
    {
        Objects.requireNonNull(sql, Errors.EVALUATION.QUERY_NOT_SUPPLIED);
        if (sql.isEmpty())
        {
            throw QueryValidationException.of(Errors.VALIDATION.QUERY_EMPTY);
        }
        return execute(sql, new QuerySettings());
    }

    /**
     * Evaluates query using supplied settings
     *
     * @param sql SQL query
     * @param querySettings Query settings
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return EvaluationResult instance
     */
    @SuppressWarnings("unchecked")
    public <T, R> EvaluationResult<R> execute(final String sql, final QuerySettings querySettings)
    {
        Objects.requireNonNull(sql, Errors.EVALUATION.QUERY_NOT_SUPPLIED);
        Objects.requireNonNull(querySettings, Errors.EVALUATION.QUERY_SETTINGS_NOT_SUPPLIED);

        if (sql.isEmpty())
        {
            throw QueryValidationException.of(Errors.VALIDATION.QUERY_EMPTY);
        }

        LOGGER.debug("Executing query '{}'", sql);

        querySettings.setMaxBigDecimalValue(_defaultQuerySettings.getMaxBigDecimalValue());
        querySettings.setMaxQueryCacheSize(_defaultQuerySettings.getMaxQueryCacheSize());
        querySettings.setMaxQueryDepth(_defaultQuerySettings.getMaxQueryDepth());

        final EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, querySettings);
        ctx.put(EvaluationContext.BROKER, _broker);
        if (querySettings.getDatePattern() != null
            && !Objects.equals(DefaultQuerySettings.DATE_TIME_PATTERN, querySettings.getDateTimePattern()))
        {
            ctx.put(EvaluationContext.QUERY_DATETIME_PATTERN_OVERRIDEN, true);
        }
        ctx.startBuilding();

        QueryExpression<T, R> query;
        try
        {
            if (_queryCache != null && _queryCache.containsKey(sql + "|" + querySettings))
            {
                query = (QueryExpression<T, R>) _queryCache.get(sql + "|" + querySettings);
            }
            else
            {
                query = new ExpressionParser<T, R>().parseQuery(sql);
                if (_queryCache != null)
                {
                    _queryCache.put(sql + "|" + querySettings, query);
                }
                LOGGER.debug("Query parsed: {}", query.getSelect());
            }
        }
        catch (ParseException e)
        {
            throw new QueryParsingException(e);
        }

        return evaluate(query);
    }

    /**
     * Evaluates parsed query
     *
     * @param query QueryExpression instance
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return EvaluationResult instance
     */
    @SuppressWarnings("unchecked")
    public <T, R> EvaluationResult<R> evaluate(final QueryExpression<T, R> query)
    {
        Objects.requireNonNull(query, Errors.EVALUATION.QUERY_NOT_SUPPLIED);

        final QueryExpressionValidator queryExpressionValidator = new QueryExpressionValidator();
        queryExpressionValidator.validate(query);

        final EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        try
        {
            ctx.startExecution(query);

            if (query.getWithItems() != null)
            {
                query.getWithItems().forEach(item -> ctx.put(item.getName(), item.getQuery()));
            }

            if (query.getOrderItems() != null && !query.getOrderItems().isEmpty())
            {
                ctx.put(EvaluationContext.QUERY_ORDERING, query.getOrderItems());
            }

            Stream<Map<String, R>> stream = (Stream<Map<String, R>>) query.getSelect().apply(null);

            stream = ctx.contains(EvaluationContext.QUERY_AGGREGATED_RESULT)
                ? sortAggregatedResult(stream, query)
                : sortResult(stream, query, ctx);

            final Integer limit = query.getLimit();
            final Integer offset = query.getOffset();

            final List<Map<String, R>> list = stream.collect(Collectors.toList());
            final long total = list.size();
            stream = list.stream();

            if (offset != null)
            {
                stream = stream.skip(offset);
            }

            if (limit != null)
            {
                stream = stream.limit(limit);
            }

            return new EvaluationResult<>(stream.collect(Collectors.toList()), total);
        }
        finally
        {
            EvaluationContextHolder.clearEvaluationContext();
        }
    }

    @SuppressWarnings("unchecked")
    private <T, R> Stream<Map<String, R>> sortResult(
        Stream<Map<String, R>> stream,
        final QueryExpression<T, R> query,
        final EvaluationContext ctx
    )
    {
        final List<OrderItem<T, R>> orderItems = query.getOrderItems();

        final List<String> defaultSortingFields = Arrays.asList("alias", "identity", "name");

        Comparator<Map<String, R>> comparator = null;

        final SetExpression<T, R> setExpression = query.getSelect();
        final List<ProjectionExpression<T, R>> projections = new ArrayList<>(setExpression.getProjections());
        if (orderItems.isEmpty() && setExpression instanceof SelectExpression)
        {
            final List<Map<String, R>> list = stream.collect(Collectors.toList());
            if (!list.isEmpty())
            {
                final List<String> names = new ArrayList<>(list.get(0).keySet());
                final String name = names.stream()
                    .filter(item -> defaultSortingFields.stream().anyMatch(element -> Objects.equals(element, item)))
                    .findFirst().orElse(names.get(0));
                if (!NumberConverter.isNumber(name))
                {
                    orderItems.add(new OrderItem<>(name, (ExpressionNode<T, R>) new MapObjectAccessor<R>(name), Order.ASC));
                }
                else
                {
                    orderItems.add(new OrderItem<>(String.valueOf(names.indexOf(name) + 1), (ExpressionNode<T, R>) new MapObjectAccessor<R>(name), Order.ASC));
                }
                ctx.put(EvaluationContext.QUERY_ORDER_ITEMS_FOR_REMOVAL, orderItems.get(orderItems.size() - 1));
            }
            stream = list.stream();
        }

        for (final OrderItem<T, R> orderItem : orderItems)
        {
            if (orderItem.isAliasOrdinal() && (orderItem.getOrdinal() < 1 || orderItem.getOrdinal() - 1 >= projections.size()))
            {
                throw QueryParsingException.of(Errors.VALIDATION.ORDER_BY_ORDINAL_INVALID);
            }

            final String alias = !orderItem.isAliasOrdinal()
                ? orderItem.getAlias()
                : projections.get(orderItem.getOrdinal() - 1).getAlias();

            final ExpressionNode<T, R> expression = !orderItem.isAliasOrdinal()
                ? orderItem.getExpression()
                : (ExpressionNode<T, R>) projections.get(orderItem.getOrdinal() - 1).getExpression();

            final Comparator<Map<String, R>> itemComparator = createComparator(expression, orderItem, alias);

            if (comparator == null)
            {
                comparator = Comparator.nullsLast(itemComparator);
            }
            else
            {
                comparator = comparator.thenComparing(itemComparator);
            }
        }

        if (comparator != null)
        {
            stream = stream.sorted(comparator);
        }

        if (ctx.contains(EvaluationContext.QUERY_ITEMS_FOR_REMOVAL))
        {
            final List<ProjectionExpression<T,R>> expressions = ctx.get(EvaluationContext.QUERY_ITEMS_FOR_REMOVAL);
            setExpression.getSelections().forEach(selection -> selection.getProjections().removeAll(expressions));
            stream = stream.peek(item -> expressions.forEach(expression -> item.remove(expression.getAlias())));
        }

        if (ctx.contains(EvaluationContext.QUERY_ORDER_ITEMS_FOR_REMOVAL))
        {
            final OrderItem<T, R> item = ctx.get(EvaluationContext.QUERY_ORDER_ITEMS_FOR_REMOVAL);
            query.getOrderItems().remove(item);
        }

        return stream;
    }

    @SuppressWarnings("unchecked")
    private <T, R> Comparator<Map<String, R>> createComparator(
        final ExpressionNode<T, R> expression,
        final OrderItem<T, R> orderItem,
        final String alias
    )
    {
        return (map1, map2) -> {

            R object1 = expression.apply((T) map1);
            R object2 = expression.apply((T) map2);

            if ((object1 != null && !(object1 instanceof Comparable)) || (object2 != null && !(object2 instanceof Comparable)))
            {
                throw QueryParsingException.of(Errors.VALIDATION.INVALID_SORT_FIELD, alias);
            }

            if (object1 instanceof Number && object2 instanceof Number && !object1.getClass().isInstance(object2))
            {
                object1 = (R) NumberConverter.toDouble(object1);
                object2 = (R) NumberConverter.toDouble(object2);
            }

            final Comparable<R> comparable1 = (Comparable<R>) object1;
            final Comparable<R> comparable2 = (Comparable<R>) object2;

            if (comparable1 == null)
            {
                return comparable2 == null ? 0 : -1;
            }

            if (comparable2 == null)
            {
                return 1;
            }

            return orderItem.getOrder().equals(Order.ASC)
                ? comparable1.compareTo((R) comparable2)
                : comparable2.compareTo((R) comparable1);
        };
    }

    private <T, R> Stream<Map<String, R>> sortAggregatedResult(
        final Stream<Map<String, R>> stream,
        final QueryExpression<T, R> query
    )
    {
        Map<String, R> map = stream.findFirst().orElse(new HashMap<>());
        final List<OrderItem<T, R>> orderItems = query.getOrderItems();
        final SelectExpression<T, R> select = (SelectExpression<T, R>) query.getSelect();

        for (final OrderItem<T, R> orderItem : orderItems)
        {
            if (orderItem.isAliasOrdinal() && (orderItem.getOrdinal() < 1 || orderItem.getOrdinal() - 1 >= query.getSelect().getProjections().size()))
            {
                throw QueryParsingException.of(Errors.VALIDATION.ORDER_BY_ORDINAL_INVALID);
            }

            final String alias = !orderItem.isAliasOrdinal()
                ? orderItem.getAlias()
                : select.getProjections().get(orderItem.getOrdinal() - 1).getAlias();

            final boolean isAggregation = select.getProjections().stream()
                .filter(projection -> Objects.equals(alias, projection.getAlias()))
                .map(projection -> !projection.getAggregations().isEmpty())
                .findFirst().orElse(false);

            final int level = isAggregation ? select.getGroupBy().size() : 1 + IntStream.range(0, select.getGroupBy().size())
                .filter(i -> Objects.equals(alias, select.getGroupBy().get(i).getAlias()))
                .findFirst().orElse(-1);

            addComparator(map, level, orderItem.getOrder(), isAggregation);
        }

        map = sort(map);

        return Stream.of(map);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R> void addComparator(final Map<String, R> map, final int level, final Order order, final boolean isAggregation)
    {
        for (final Map.Entry<String, R> entry : map.entrySet())
        {
            if (Objects.equals(EvaluationContext.COMPARATORS, entry.getKey()))
            {
                continue;
            }
            if (level > 0 && entry.getValue() instanceof Map)
            {
                addComparator((Map<String, R>) entry.getValue(), level - 1, order, isAggregation);
            }
            else
            {
                final Comparator newComparator = isAggregation
                    ? Map.Entry.comparingByValue((Comparator<? super R>) (order == Order.ASC? Comparator.naturalOrder() : Comparator.reverseOrder()))
                    : Map.Entry.comparingByKey((Comparator<? super R>) (order == Order.ASC ? Comparator.naturalOrder() : Comparator.reverseOrder()));
                List<Comparator> comparators = new ArrayList<>();
                if (map.containsKey(EvaluationContext.COMPARATORS))
                {
                    comparators = (List<Comparator>) map.get(EvaluationContext.COMPARATORS);
                    comparators.add(newComparator);
                }
                else
                {
                    comparators.add(newComparator);
                }
                if (isAggregation)
                {
                    comparators.add(Map.Entry.comparingByKey(Comparator.naturalOrder()));
                }
                map.put(EvaluationContext.COMPARATORS, (R) comparators);
                break;
            }
        }
    }

    /**
     * Sorts aggregated result.
     * Sorting can be performed both by keys and by values (keys usually represent grouping field, values represent
     * result of an aggregation function). On each level there could be maximal 2 sorting rules (comparators) applied.
     *
     * @param map Map containing aggregated result
     * @param <R> Return type
     *
     * @return Sorted map
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R> Map<String, R> sort(final Map<String, R> map)
    {
        for (final Map.Entry<String, R> entry : map.entrySet())
        {
            if (entry.getValue() instanceof Map)
            {
                final Map<String, R> child = (Map<String, R>) entry.getValue();
                final List<Comparator> comparators = child.containsKey(EvaluationContext.COMPARATORS)
                    ? (List<Comparator>) child.remove(EvaluationContext.COMPARATORS)
                    : Collections.singletonList(Map.Entry.comparingByKey(Comparator.naturalOrder()));
                Comparator comparator = comparators.get(0);
                if (comparators.size() > 1)
                {
                    comparator = comparator.thenComparing(comparators.get(comparators.size() - 1));
                }
                final List<Map.Entry<String, R>> entries = new ArrayList<>(child.entrySet());
                entries.sort(comparator);
                final Map<String, R> result = new LinkedHashMap<>();
                for (final Map.Entry<String, R> childEntry : entries)
                {
                    result.put(childEntry.getKey(), childEntry.getValue());
                }
                entry.setValue((R) result);
                sort((Map<String, R>) entry.getValue());
            }
        }
        final List<Map.Entry<String, R>> entries = new ArrayList<>(map.entrySet());
        entries.sort(Map.Entry.comparingByKey());
        final Map<String, R> result = new LinkedHashMap<>();
        for (final Map.Entry<String, R> childEntry : entries)
        {
            result.put(childEntry.getKey(), childEntry.getValue());
        }
        return result;
    }
}
