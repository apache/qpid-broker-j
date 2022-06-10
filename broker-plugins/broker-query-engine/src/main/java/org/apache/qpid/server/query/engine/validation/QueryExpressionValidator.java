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

import java.util.Objects;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.expression.set.SetExpression;
import org.apache.qpid.server.query.engine.parsing.query.QueryExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Validates query structure in accordance to SQL rules
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class QueryExpressionValidator
{
    /**
     * Validator for select expression
     */
    private final SelectExpressionValidator _selectExpressionValidator = new SelectExpressionValidator();

    /**
     * Validates query structure in accordance to SQL rules
     *
     * @param query Query to be validated
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     */
    public <T, R> void validate(final QueryExpression<T, R> query)
    {
        Objects.requireNonNull(query, Errors.VALIDATION.QUERY_EXPRESSION_NULL);

        final SetExpression<T, R> setExpression =  query.getSelect();
        if (setExpression == null)
        {
            throw QueryValidationException.of(Errors.VALIDATION.MISSING_EXPRESSION);
        }
        if (setExpression instanceof SelectExpression)
        {
            final SelectExpression<T, R> selectExpression = (SelectExpression<T, R>) setExpression;
            _selectExpressionValidator.validate(selectExpression);
        }
    }
}
