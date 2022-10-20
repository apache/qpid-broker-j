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
package org.apache.qpid.server.query.engine.parsing.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.query.engine.evaluator.SelectEvaluator;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.AbstractComparisonExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.InExpression;
import org.apache.qpid.server.query.engine.parsing.expression.set.AbstractSetExpression;
import org.apache.qpid.server.query.engine.parsing.expression.set.EmptySetExpression;
import org.apache.qpid.server.query.engine.parsing.factory.LogicExpressionFactory;

/**
 * Contains information about a select expression, which may contain SELECT clause, FROM clause, WHERE clause, GROUP BY
 * clause and HAVING clause.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class SelectExpression<T, R> extends AbstractSetExpression<T, R>
{
    /**
     * List of items in the SELECT clause
     */
    private final List<ProjectionExpression<T, R>> _projections = new ArrayList<>();

    /**
     * List of items in the GROUP BY clause
     */
    private final List<ProjectionExpression<T, R>> _groupBy = new ArrayList<>();

    /**
     * Ordinal used to count items
     */
    private final AtomicInteger _ordinal = new AtomicInteger(0);

    /**
     * Flag used to determine if SELECT * was used
     */
    private boolean _selectAll = false;

    /**
     * FROM clause
     */
    private FromExpression<T, Stream<?>, ConfiguredObject<?>> _from;

    /**
     * WHERE clause
     */
    private ExpressionNode<T, R> _where;

    /**
     * HAVING clause
     */
    private HavingExpression<T, R> _having;

    /**
     * Default constructor sets default distinct property value (false)
     */
    public SelectExpression()
    {
        super(false);
    }

    /**
     * Constructor sets distinct property value
     *
     * @param distinct Distinct flag
     */
    public SelectExpression(final boolean distinct)
    {
        super(distinct);
    }

    /**
     * Evaluates select expression and returns evaluation result
     *
     * @param value Object to handle (is ignored)
     *
     * @return Evaluation result
     */
    @Override
    @SuppressWarnings("unchecked")
    public R apply(final T value)
    {
        if (ctx().contains(getAlias()))
        {
            return ctx().get(getAlias());
        }

        final Stream<Map<String, R>> result = new SelectEvaluator().evaluate(this);

        if (getParent() == null)
        {
            return (R) result;
        }

        if (getParent() instanceof ProjectionExpression)
        {
            return extractSingleResult(result);
        }

        if (getParent() instanceof InExpression)
        {
            final R collectionResult = extractCollectionResult(result);
            ctx().put(getAlias(), collectionResult);
            return collectionResult;
        }

        if (getParent() instanceof AbstractComparisonExpression)
        {
            final R singleResult = extractSingleResult(result);
            ctx().put(getAlias(), singleResult);
            return singleResult;
        }

        return (R) result;
    }

    /**
     * Extracts resulting entity from a subquery
     *
     * @param stream Stream returned from a subquery
     *
     * @return Resulting entity
     */
    @SuppressWarnings("unchecked")
    private R extractSingleResult(final Stream<Map<String, R>> stream)
    {
        final List<Map<String, R>> result = stream.collect(Collectors.toList());
        if (result.isEmpty())
        {
            return (R) new EmptySetExpression<>();
        }
        if (result.size() > 1)
        {
            throw QueryParsingException.of(Errors.VALIDATION.SUBQUERY_RETURNS_MULTIPLE_ROWS, String.valueOf(this), result.size());
        }
        final Map<String, R> aggregationMap = result.get(0);
        if (aggregationMap.keySet().size() > 1)
        {
            throw QueryParsingException.of(Errors.VALIDATION.SUBQUERY_RETURNS_MULTIPLE_VALUES, String.valueOf(this), String.valueOf(aggregationMap.keySet()));
        }
        final String firstKey = aggregationMap.entrySet().iterator().next().getKey();
        return aggregationMap.get(firstKey);
    }

    /**
     * Extracts resulting collection of entities from a subquery
     *
     * @param stream Stream returned from a subquery
     *
     * @return Resulting collection of entities
     */
    @SuppressWarnings("unchecked")
    private R extractCollectionResult(final Stream<Map<String, R>> stream)
    {
        final List<Map<String, R>> result = stream.collect(Collectors.toList());
        final List<Object> list = new ArrayList<>();
        for (final Map<?, ?> projection : result)
        {
            if (projection != null)
            {
                final Object firstKey = projection.entrySet().iterator().next().getKey();
                list.add(projection.get(firstKey));
            }
        }
        return (R) list;
    }

    @Override()
    @SuppressWarnings({"findbugs:EI_EXPOSE_REP", "java:S2384"})
    // sonar: mutable list of projections is returned intentionally, it may be modified externally
    public List<ProjectionExpression<T, R>> getProjections()
    {
        return _projections;
    }

    public boolean isDistinct()
    {
        return _distinct;
    }

    public boolean isSelectAll()
    {
        return _selectAll;
    }

    public int getOrdinal()
    {
        return _ordinal.incrementAndGet();
    }

    @SuppressWarnings({"java:S1452", "findbugs:EI_EXPOSE_REP"})
    // sonar: wildcard types are returned intentionally: it's unknown which type will be returned
    // sonar: from object is returned intentionally
    public FromExpression<T, Stream<?>, ConfiguredObject<?>> getFrom()
    {
        return _from;
    }

    public Predicate<T> getWhere()
    {
        return _where == null ? null : LogicExpressionFactory.toPredicate(_where);
    }

    public List<ProjectionExpression<T, R>> getGroupBy()
    {
        return new ArrayList<>(_groupBy);
    }

    @SuppressWarnings("findbugs:EI_EXPOSE_REP")
    // sonar: having object is returned intentionally
    public HavingExpression<T, R> getHaving()
    {
        return _having;
    }

    public void selectAll()
    {
        _selectAll = true;
    }

    public void selectItem(final ProjectionExpression<T, R> expression)
    {
        this._projections.add(expression);
    }

    public void distinct(boolean distinct)
    {
        this._distinct = distinct;
    }

    public void from(final String domain, final String alias)
    {
        if (_from != null)
        {
            throw QueryValidationException.of(Errors.VALIDATION.MULTIPLE_DOMAINS_NOT_SUPPORTED);
        }
        _from = new FromExpression<>(domain, alias);
    }

    public void join()
    {
        throw QueryValidationException.of(Errors.VALIDATION.JOINS_NOT_SUPPORTED);
    }

    @SuppressWarnings("findbugs:EI_EXPOSE_REP2")
    // sonar: mutable where object is stored intentionally
    public void where(final ExpressionNode<T, R> predicate)
    {
        this._where = predicate;
    }

    public void having(final ExpressionNode<T, R> predicate)
    {
        this._having = new HavingExpression<>(this, predicate);
    }

    public void groupBy(final ProjectionExpression<T, R> expression)
    {
        if (expression.isOrdinal())
        {
            final int ordinal = (Integer) expression.apply(null) - 1;
            if (ordinal < 0 || ordinal >= _projections.size())
            {
                throw QueryParsingException.of(Errors.VALIDATION.GROUP_BY_ORDINAL_INVALID);
            }
            this._groupBy.add(_projections.get(ordinal));
        }
        else
        {
            this._groupBy.add(expression);
        }
    }

    public void resetOrdinal()
    {
        _ordinal.set(0);
    }

    public boolean hasAggregationItems()
    {
        return _projections.stream().anyMatch(ProjectionExpression::containsAggregation);
    }

    public List<ProjectionExpression<T, R>> getAggregationItems()
    {
        return _projections.stream()
            .filter(ProjectionExpression::containsAggregation)
            .collect(Collectors.toList());
    }

    @Override
    public String getAlias()
    {
        return "select "
            + (_distinct ? " distinct " : "")
            + (_selectAll ? " * " : "")
            + _projections.stream().map(AbstractExpressionNode::toString).collect(Collectors.joining(", "))
            + (_from == null ? "" : " from " + _from.getAlias())
            + (_where == null ? "" : " where " + _where.getAlias())
            + _groupBy.stream().map(AbstractExpressionNode::getAlias).collect(Collectors.joining(", "))
            + (_having == null ? "" : _having.getAlias());
    }

    @Override()
    public String toString()
    {
        return getAlias();
    }
}
