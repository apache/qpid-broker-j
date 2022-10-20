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
package org.apache.qpid.server.query.engine.parsing.expression.set;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.query.ProjectionExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Abstract class containing base functionality for set expressions
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public abstract class AbstractSetExpression<T, R> extends AbstractExpressionNode<T, R> implements SetExpression<T, R>
{
    /**
     * Distinct flag
     */
    protected boolean _distinct;

    /**
     * Constructor stores distinct flag
     *
     * @param distinct Distinct flag
     */
    public AbstractSetExpression(final boolean distinct)
    {
        super();
        _distinct = distinct;
    }

    /**
     * Constructor initializes children expression list and validates their compatibility
     *
     * @param distinct Distinct flag
     * @param left Left expression
     * @param right Right expression
     */
    public AbstractSetExpression(
        final boolean distinct,
        final ExpressionNode<T, R> left,
        final ExpressionNode<T, R> right
    )
    {
        super(left, right);
        _distinct = distinct;

        final SetExpression<T, R> leftExpression = (SetExpression<T, R>) left;
        final SetExpression<T, R> rightExpression = (SetExpression<T, R>) right;

        final List<ProjectionExpression<T, R>> leftProjections = leftExpression.getProjections();
        final List<ProjectionExpression<T, R>> rightProjections = rightExpression.getProjections();

        if (leftExpression.getProjections().size() != rightProjections.size())
        {
            throw QueryParsingException.of(
                Errors.SET.PRODUCTS_HAVE_DIFFERENT_LENGTH,
                getClass().getSimpleName().toLowerCase(Locale.US).replace("expression", "")
            );
        }

        for (int i = 0; i < leftProjections.size(); i ++)
        {
            final ProjectionExpression<T,R> leftProjection = leftProjections.get(i);
            final ProjectionExpression<T,R> rightProjection = rightProjections.get(i);
            rightProjection.setAlias(leftProjection.getAlias());
        }
    }

    /**
     * Returns list of projections
     *
     * @return List of projections
     */
    @Override()
    public List<ProjectionExpression<T, R>> getProjections()
    {
        final SetExpression<T, R> left = (SetExpression<T, R>) getChild(0);
        return left.getProjections();
    }

    /**
     * Returns list of select expressions
     *
     * @return List of select expressions
     */
    @Override()
    public List<SelectExpression<T,R>> getSelections()
    {
        final List<SelectExpression<T, R>> result = new ArrayList<>();
        getChildSelections(this, result);
        return result;
    }

    /**
     * Recursively goes through the set expression and retrieves child selections
     *
     * @param setExpression Expression to retrieve child selections from
     * @param result List to store child selections
     */
    @SuppressWarnings("unchecked")
    private void getChildSelections(final SetExpression<T, R> setExpression, final List<SelectExpression<T, R>> result)
    {
        if (setExpression instanceof SelectExpression)
        {
            result.add((SelectExpression<T, R>) setExpression);
            return;
        }
        for (final ExpressionNode<T, ?> child : setExpression.getChildren())
        {
            if (child instanceof SetExpression)
            {
                getChildSelections((SetExpression<T, R>) child, result);
            }
        }
    }
}
