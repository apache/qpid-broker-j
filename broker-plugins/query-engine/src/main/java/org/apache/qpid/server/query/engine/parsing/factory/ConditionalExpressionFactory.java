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

import java.util.List;
import java.util.Objects;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.conditional.CaseExpression;

/**
 * Factory creating conditional expressions
 */
public final class ConditionalExpressionFactory
{
    /**
     * Shouldn't be instantiated directly
     */
    private ConditionalExpressionFactory()
    {

    }

    /**
     * Creates CASE expression
     *
     * @param conditions List of conditions
     * @param outcomes List of outcomes
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return CaseExpression instance
     */
    public static <T, R> CaseExpression<T, R> caseExpression(
        final List<ExpressionNode<T,R>> conditions,
        final List<ExpressionNode<T,R>> outcomes
    )
    {
        Objects.requireNonNull(conditions, Errors.VALIDATION.CHILD_EXPRESSIONS_NULL);
        Objects.requireNonNull(outcomes, Errors.VALIDATION.CHILD_EXPRESSIONS_NULL);
        return new CaseExpression<>(conditions, outcomes);
    }
}
