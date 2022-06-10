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
package org.apache.qpid.server.query.engine.parsing.expression.accessor;

import java.util.Collection;
import java.util.Map;

import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;

/**
 * Delegating collection accessor retrieves values either from indexed collections or from maps
 *
 * @param <T> Input type parameter
 * @param <R> Return type parameter
 *
 * @param <INDEX> Index type
 * @param <COLLECTION> Collection type
 * @param <MAP> Map type
 */
// sonar complains about underscores in variable names
@SuppressWarnings({"java:S116", "java:S119"})
public class DelegatingCollectionAccessorExpression<T, R, INDEX, COLLECTION extends Collection<R>, MAP extends Map<String, R>>
    extends AbstractExpressionNode<T, R>
{
    /**
     * Property name
     */
    private final String _property;

    /**
     * Property index
     */
    private final Expression<T, INDEX> _index;

    /**
     * Constructor stores field values
     *
     * @param alias Expression alias
     * @param property Property name
     * @param index Property index
     */
    public DelegatingCollectionAccessorExpression(
        final String alias,
        final String property,
        final Expression<T, INDEX> index
    )
    {
        super(alias);
        _property = property;
        _index = index;
    }

    /**
     * Evaluates expression using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    public R apply(final T value)
    {
        final INDEX pointer = _index.apply(value);
        if (pointer instanceof Number)
        {
            final DelegatingObjectAccessor<T, COLLECTION> accessor = new DelegatingObjectAccessor<>(getAlias(), _property);
            final Collection<R> collection = accessor.apply(value);
            final int index = ((Number) pointer).intValue();
            return new CollectionObjectAccessor<R>(index).apply(collection);
        }
        if (pointer instanceof String)
        {
            final DelegatingObjectAccessor<T, MAP> accessor = new DelegatingObjectAccessor<>(getAlias(), _property);
            final Map<String, R> map = accessor.apply(value);
            final String key = (String) pointer;
            return new MapObjectAccessor<R>(key).apply(map);
        }
        throw QueryEvaluationException.fieldNotFound(_property);
    }
}
