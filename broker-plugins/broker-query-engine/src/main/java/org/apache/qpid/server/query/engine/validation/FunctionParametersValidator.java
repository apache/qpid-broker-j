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

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;

/**
 * Validates function parameters
 */
public final class FunctionParametersValidator
{
    /**
     * Shouldn't be instantiated directly
     */
    private FunctionParametersValidator() {}

    /**
     * Validates argument list size
     *
     * @param size Expected size of argument list
     * @param args List of arguments
     * @param expression Parent expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     */
    public static <T, R> void requireParameters(
        final int size,
        final List<ExpressionNode<T, ?>> args,
        final AbstractFunctionExpression<T, R> expression
    )
    {
        final String errorMessage = size > 1 ? Errors.FUNCTION.REQUIRE_PARAMETERS : Errors.FUNCTION.REQUIRE_PARAMETER;
        if (args == null || args.isEmpty())
        {
            throw QueryParsingException.of(errorMessage, expression.getFunctionName(), size);
        }
        if (args.size() != size)
        {
            throw QueryParsingException.of(errorMessage, expression.getFunctionName(), size);
        }
    }

    /**
     * Validates that argument list exceeds a minimal expected size
     *
     * @param size Expected minimal size of argument list
     * @param args List of arguments
     * @param expression Parent expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     */
    public static <T, R> void requireMinParameters(
        final int size,
        final List<ExpressionNode<T, ?>> args,
        final AbstractFunctionExpression<T, R> expression
    )
    {
        final String errorMessage = size > 1 ? Errors.FUNCTION.REQUIRE_MIN_PARAMETERS : Errors.FUNCTION.REQUIRE_MIN_PARAMETER;
        if (args == null || args.isEmpty())
        {
            throw QueryParsingException.of(errorMessage, expression.getFunctionName(), size);
        }
        if (args.size() < size)
        {
            throw QueryParsingException.of(errorMessage, expression.getFunctionName(), size);
        }
    }

    /**
     * Validates that argument list doesn't exceed a maximal expected size
     *
     * @param size Expected maximal size of argument list
     * @param args List of arguments
     * @param expression Parent expression
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     */
    public static <T, R> void requireMaxParameters(
        final int size,
        final List<ExpressionNode<T, ?>> args,
        final AbstractFunctionExpression<T, R> expression
    )
    {
        if (args == null || args.isEmpty())
        {
            throw QueryParsingException.of(Errors.FUNCTION.REQUIRE_MAX_PARAMETERS, expression.getFunctionName(), size);
        }
        if (args.size() > size)
        {
            throw QueryParsingException.of(Errors.FUNCTION.REQUIRE_MAX_PARAMETERS, expression.getFunctionName(), size);
        }
    }
}
