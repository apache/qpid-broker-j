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

import java.util.Objects;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.query.ProjectionExpression;

/**
 * Factory creating projection expressions
 */
public final class ProjectionExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private ProjectionExpressionFactory()
    {

    }

    /**
     * Creates a ProjectionExpression instance
     *
     * @param expression Wrapped expression
     * @param alias Projections alias
     * @param ordinal Projections ordinal
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return ProjectionExpression instance
     */
    public static <T, R> ProjectionExpression<T, R> projection(
        final ExpressionNode<T, R> expression,
        final String alias,
        final int ordinal
    )
    {
        Objects.requireNonNull(expression, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        if (ordinal < 1)
        {
            throw QueryValidationException.of(Errors.VALIDATION.INVALID_ORDINAL);
        }
        return new ProjectionExpression<>(alias, expression, ordinal);
    }
}
