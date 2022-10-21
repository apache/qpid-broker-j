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
package org.apache.qpid.server.query.engine.validation;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.query.ProjectionExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Validates select expression structure according to SQL rules
 */
public class SelectExpressionValidator
{
    /**
     * Validates select expression structure according to SQL rules
     *
     * @param selectExpression Select expression to be validated
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     */
    public <T, R> void validate(final SelectExpression<T, R> selectExpression)
    {
        Objects.requireNonNull(selectExpression, Errors.VALIDATION.SELECT_EXPRESSION_NULL);

        final boolean hasAccessors = selectExpression.getProjections().stream().anyMatch(AbstractExpressionNode::isAccessor);
        final boolean hasAggregations = selectExpression.getProjections().stream().anyMatch(AbstractExpressionNode::containsAggregation);
        final boolean hasGrouping = !selectExpression.getGroupBy().isEmpty();
        final boolean hasHaving = selectExpression.getHaving() != null;

        if (selectExpression.getFrom() == null)
        {
            if (selectExpression.getProjections().isEmpty())
            {
                throw QueryValidationException.of(Errors.VALIDATION.MISSING_EXPRESSION);
            }
            if (hasAccessors)
            {
                throw QueryValidationException.of(Errors.VALIDATION.KEYWORD_FROM_NOT_FOUND);
            }
        }

        if (hasAggregations)
        {
            final List<String> projectionAliases = selectExpression.getProjections().stream()
                .filter(item -> !item.containsAggregation()).map(ProjectionExpression::getAlias)
                .collect(Collectors.toList());
            final List<String> groupByAliases = selectExpression.getGroupBy().stream().map(ProjectionExpression::getAlias)
                .collect(Collectors.toList());
            projectionAliases.removeAll(groupByAliases);
            if (!projectionAliases.isEmpty())
            {
                throw QueryValidationException.of(Errors.VALIDATION.NOT_A_SINGLE_GROUP_EXPRESSION, projectionAliases);
            }
        }

        if (hasHaving && !(hasAggregations || hasGrouping))
        {
            throw QueryValidationException.of(Errors.VALIDATION.HAVING_WITHOUT_AGGREGATION);
        }
    }
}
