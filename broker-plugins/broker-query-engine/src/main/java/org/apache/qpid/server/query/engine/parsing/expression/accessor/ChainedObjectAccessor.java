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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;

/**
 * Chained object accessor retrieves values from expressions having form parent.child.grandchild
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
public class ChainedObjectAccessor<T, R> extends AbstractExpressionNode<T, R>
{
    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param first First argument in the chain
     * @param args Subsequent arguments in the chain
     */
    public ChainedObjectAccessor(
        final String alias,
        final ExpressionNode<T, R> first,
        final List<ExpressionNode<R, ?>> args
    )
    {
        super(alias, first, ConstantExpression.of(args));
        _metadata.setAlias(first.getAlias() + (args.isEmpty() ? "" : "."  + args.stream().map(ExpressionNode::getAlias).collect(Collectors.joining("."))));
    }

    /**
     * Evaluates expression using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Evaluation result
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public R apply(final T value)
    {
        final R child = evaluateChild(0, value);
        final List<ExpressionNode<R, ?>> args = evaluateChild(1, null);
        return args.isEmpty()
            ? child
           : (R) new ChainedObjectAccessor(getAlias(), args.get(0), args.subList(1, args.size())).apply(child);
    }
}