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
package org.apache.qpid.server.query.engine.parsing.expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.AbstractAggregationExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;

/**
 * Abstract expression node containing base functionality
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public abstract class AbstractExpressionNode<T, R> implements ExpressionNode<T, R>
{
    /**
     * Expression metadata
     */
    protected final Metadata<T> _metadata;

    /**
     * Expression parent
     */
    private ExpressionNode<T, ?> _parent;

    /**
     * Expression children
     */
    private final List<ExpressionNode<T, ?>> _children;

    /**
     * Default constructor creates an empty children list and checks current query depth against the settings
     */
    public AbstractExpressionNode()
    {
        _metadata = new Metadata<>();
        _children = new ArrayList<>();
        if (ctx().isBuilding())
        {
            ctx().incrementDepth();
        }
        final int depth = ctx().getDepth();
        final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
        if (!ctx().isExecuting() && depth >= querySettings.getMaxQueryDepth())
        {
            throw QueryParsingException.of(Errors.VALIDATION.MAX_QUERY_DEPTH_REACHED, ctx().getDepth());
        }
    }

    /**
     * Constructor stores expression alias
     *
     * @param alias Expression alias
     */
    public AbstractExpressionNode(final String alias)
    {
        this();
        _metadata.setAlias(alias);
    }

    /**
     * Constructor stores child expression supplied
     *
     * @param first Fist child
     */
    public AbstractExpressionNode(final ExpressionNode<T, ?> first)
    {
        this(Collections.singletonList(first));
    }

    /**
     * Constructor stores alias and child expression supplied
     *
     * @param alias Expression alias
     * @param first Fist child
     */
    public AbstractExpressionNode(final String alias, final ExpressionNode<T, ?> first)
    {
        this(alias, Collections.singletonList(first));
    }

    /**
     * Constructor stores the supplied child expressions
     *
     * @param first Fist child
     * @param second Second child
     */
    public AbstractExpressionNode(final ExpressionNode<T, ?> first, final ExpressionNode<T, ?> second)
    {
        this(Arrays.asList(first, second));
    }

    /**
     * Constructor stores alias and children expressions supplied
     *
     * @param alias Expression alias
     * @param first Fist child
     * @param second Second child
     */
    public AbstractExpressionNode(final String alias, final ExpressionNode<T, ?> first, final ExpressionNode<T, ?> second)
    {
        this(alias, Arrays.asList(first, second));
    }

    /**
     * Constructor stores the supplied child expressions
     *
     * @param first Fist child
     * @param second Second child
     * @param third Third child
     */
    public AbstractExpressionNode(
        final ExpressionNode<T, ?> first,
        final ExpressionNode<T, ?> second,
        final ExpressionNode<T, ?> third
    )
    {
        this(Arrays.asList(first, second, third));
    }

    /**
     * Constructor stores the supplied child expressions
     *
     * @param children List of child expressions
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public AbstractExpressionNode(final List<ExpressionNode<T, ?>> children)
    {
        this();
        if (children != null)
        {
            for (final ExpressionNode<T, ?> child : children)
            {
                _children.add(child);
                child.setParent(this);
                if (child instanceof AbstractAggregationExpression)
                {
                    _metadata.addAggregation((AbstractAggregationExpression) child);
                }
                if (child.containsAggregation())
                {
                    child.getAggregations().forEach(_metadata::addAggregation);
                }
                _metadata.setAccessor(child.isAccessor());
            }
        }
    }

    /**
     * Constructor stores alias and children expressions supplied
     *
     * @param alias Expression alias
     * @param children List of child expressions
     */
    public AbstractExpressionNode(final String alias, final List<ExpressionNode<T, ?>> children)
    {
        this(children);
        _metadata.setAlias(alias);
    }

    /**
     * Returns current evaluation context
     *
     * @return Evaluation context
     */
    protected final EvaluationContext ctx()
    {
        return EvaluationContextHolder.getEvaluationContext();
    }

    /**
     * Returns expression parent
     *
     * @return Expression parent
     */
    @Override()
    @SuppressWarnings("unchecked")
    public <Y> ExpressionNode<T, Y> getParent()
    {
        return (ExpressionNode<T, Y>) _parent;
    }

    /**
     * Setter for expression parent
     *
     * @param parent Expression parent
     */
    @Override()
    public void setParent(ExpressionNode<T, ?> parent)
    {
        _parent = parent;
    }

    /**
     * Retrieves child expression by index
     *
     * @param index Child index
     *
     * @param <X> Child input type
     * @param <Y> Child return type
     *
     * @return Expression child or null
     */
    @SuppressWarnings("unchecked")
    protected <X, Y> ExpressionNode<X, Y> getChild(int index)
    {
        return _children.size() - 1 < index ? null : (ExpressionNode<X, Y>) _children.get(index);
    }

    /**
     * Retrieves child expression by index and evaluates it against the value provided
     *
     * @param index Child expression index
     * @param value Object to handle
     *
     * @param <X> Child input type
     * @param <Y> Child return type
     *
     * @return Evaluation result
     */
    protected <X, Y> Y evaluateChild(int index, X value)
    {
        final ExpressionNode<X, Y> child = getChild(index);
        if (child == null)
        {
            throw QueryEvaluationException.of(Errors.EVALUATION.CHILD_EXPRESSION_NOT_FOUND, getAlias(), index);
        }
        return child.apply(value);
    }

    /**
     * Return expression children
     *
     * @return List of child expressions
     */
    @Override()
    @SuppressWarnings("unchecked")
    public <Y> List<ExpressionNode<T, Y>> getChildren()
    {
        return _children.stream().map(item -> (ExpressionNode<T, Y>) item).collect(Collectors.toList());
    }

    /**
     * Determines whether expression contains aggregation nodes or not
     *
     * @return True or false
     */
    @Override()
    public boolean containsAggregation()
    {
        return _metadata.isAggregation();
    }

    /**
     * Determines whether expression is an accessor or not
     *
     * @return True or false
     */
    @Override()
    public boolean isAccessor()
    {
        return _metadata.isAccessor();
    }

    /**
     * Determines whether expression can be evaluated instantly or not
     *
     * @return True or false
     */
    @Override()
    public boolean isInstantlyEvaluable()
    {
        return getChildren().stream().allMatch(ConstantExpression.class::isInstance);
    }

    /**
     * Returns expression alias
     *
     * @return Expression alias
     */
    @Override()
    public String getAlias()
    {
        return _metadata.getAlias();
    }

    /**
     * Setter for alias
     *
     * @param alias Expression alias
     */
    @Override()
    public void setAlias(String alias)
    {
        _metadata.setAlias(alias);
    }

    /**
     * Returns expression children being aggregation nodes
     *
     * @param <Y> Return type
     *
     * @return List of aggregation expressions
     */
    @Override()
    @SuppressWarnings("unchecked")
    public <Y> List<AbstractAggregationExpression<T, Y>> getAggregations()
    {
        return _metadata.getAggregations().stream()
            .map(expression -> (AbstractAggregationExpression<T, Y>)expression)
            .collect(Collectors.toList());
    }

    @Override()
    public String toString()
    {
        return getAlias();
    }
}
