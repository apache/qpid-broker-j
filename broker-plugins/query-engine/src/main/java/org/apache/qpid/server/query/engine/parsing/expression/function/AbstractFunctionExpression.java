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
package org.apache.qpid.server.query.engine.parsing.expression.function;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * Parent for function expression classes
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public abstract class AbstractFunctionExpression<T, R> extends AbstractExpressionNode<T, R>
{
    /**
     * Function name
     */
    protected final String _functionName;

    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public AbstractFunctionExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        _functionName = alias.substring(0, alias.indexOf('(')).toUpperCase(Locale.US);
    }

    /**
     * Getter for function name
     *
     * @return Function name
     */
    public String getFunctionName()
    {
        return _functionName;
    }

    /**
     * Evaluates child expression against the value supplied and validates the result against the validator
     *
     * @param index Child expression index
     * @param value Value used to evaluate child expression
     * @param typePredicate Predicate used for result validation
     * @param <X> Value type
     * @param <Y> Result type
     *
     * @return Result of child expression evaluation
     */
    protected <X, Y> Y evaluateChild(final int index, final X value, final FunctionParameterTypePredicate<Y> typePredicate)
    {
        final ExpressionNode<X, Y> child = getChild(index);
        final Y result = child.apply(value);
        if (!typePredicate.test(result))
        {
            throw QueryEvaluationException.invalidFunctionParameter(_functionName, result);
        }
        return result;
    }

    /**
     * Evaluates expression using value supplied and performs validation checks of result. Returns result if checks
     * succeeded, throws an exception otherwise.
     *
     * @param index Child expression index to evaluate
     * @param value Value to pass to expression
     * @param type Expected result type
     * @param message Exception message
     *
     * @param <X> Input parameter type
     * @param <Y> Return parameter type
     *
     * @return Evaluated result
     */
    public <X, Y> Y getRequiredParameter(final int index, final X value, final Class<Y> type, final String message)
    {
        final ExpressionNode<X, Y> child = getChild(index);
        final Y result = child.apply(value);
        if (result == null || !type.isAssignableFrom(result.getClass()))
        {
            throw QueryEvaluationException.of(message, _functionName, index + 1);
        }
        return result;
    }

    /**
     * Evaluates nullable expression using value supplied and performs validation checks of result.
     * Returns an empty Optional when expression is null, otherwise returns evaluated result wrapped into Optional.
     *
     * @param index Child expression index to evaluate
     * @param value Value to pass to expression
     * @param type Expected result type
     * @param message Exception message
     *
     * @param <X> Input parameter type
     * @param <Y> Return parameter type
     *
     * @return Optional result
     */
    public <X, Y> Optional<Y> getOptionalParameter(
        final int index,
        final X value,
        final Class<Y> type,
        final String message
    )
    {
        final ExpressionNode<X, Y> child = getChild(index);
        if (child == null)
        {
            return Optional.empty();
        }
        final Y result = getRequiredParameter(index, value, type, message);
        return Optional.of(result);
    }

    /**
     * Evaluates nullable constant expression. Returns an empty Optional when expression is null, otherwise
     * returns evaluated result wrapped into Optional.
     *
     * @param index Child expression index to evaluate
     * @param type Expected result type
     * @param message Exception message
     *
     * @param <X> Input parameter type
     * @param <Y> Return parameter type
     *
     * @return Optional result
     */
    public <X, Y> Optional<Y> getOptionalConstantParameter(final int index, final Class<Y> type, final String message)
    {
        final ExpressionNode<X, Y> child = getChild(index);
        if (child == null)
        {
            return Optional.empty();
        }
        final Y result = child.apply(null);
        if (result == null)
        {
            return Optional.empty();
        }
        if (!type.isAssignableFrom(result.getClass()))
        {
            throw QueryEvaluationException.of(message, _functionName, index + 1);
        }
        return Optional.of(result);
    }
}
