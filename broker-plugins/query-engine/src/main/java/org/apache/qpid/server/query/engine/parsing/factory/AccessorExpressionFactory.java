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
import org.apache.qpid.server.query.engine.parsing.expression.accessor.ChainedObjectAccessor;
import org.apache.qpid.server.query.engine.parsing.expression.accessor.DelegatingCollectionAccessorExpression;
import org.apache.qpid.server.query.engine.parsing.expression.accessor.DelegatingObjectAccessor;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.StringLiteralExpression;

/**
 * Factory creating accessor expressions
 */
public final class AccessorExpressionFactory
{

    /**
     * Shouldn't be instantiated directly
     */
    private AccessorExpressionFactory()
    {

    }

    /**
     * Creates delegating accessor
     *
     * @param alias Expression alias
     * @param property Entity property name
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return DelegatingObjectAccessor instance
     */
    public static <T, R> ExpressionNode<T, R> delegating(final String alias, final String property)
    {
        Objects.requireNonNull(property, Errors.VALIDATION.PROPERTY_NAME_NULL);
        return new DelegatingObjectAccessor<>(alias, property);
    }

    /**
     * Creates chained accessor
     *
     * @param alias Expression alias
     * @param first First expression in the chain
     * @param args Subsequent expression in the chain
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Chained accessor instance
     */
    public static <T, R> ExpressionNode<T, R> chained(
        final String alias,
        final ExpressionNode<T, R> first,
        final List<ExpressionNode<R, ?>> args
    )
    {
        Objects.requireNonNull(first, Errors.VALIDATION.CHILD_EXPRESSION_NULL);
        if (args == null || args.isEmpty())
        {
            if (first instanceof StringLiteralExpression)
            {
                return new DelegatingObjectAccessor<>(alias, (String) first.apply(null));
            }
            if (first instanceof DelegatingObjectAccessor)
            {
                return first;
            }
        }
        return new ChainedObjectAccessor<>(alias, first, args);
    }

    /**
     * Creates collection accessor
     *
     * @param alias Expression alias
     * @param property Entity property name
     * @param index Collection index
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Collection accessor
     */
    public static <T, R> ExpressionNode<T, R> collection(
        final String alias,
        final String property,
        final Expression<T, R> index
    )
    {
        Objects.requireNonNull(property, Errors.VALIDATION.PROPERTY_NAME_NULL);
        Objects.requireNonNull(index, Errors.VALIDATION.INDEX_NULL);
        return new DelegatingCollectionAccessorExpression<>(alias, property, index);
    }
}
