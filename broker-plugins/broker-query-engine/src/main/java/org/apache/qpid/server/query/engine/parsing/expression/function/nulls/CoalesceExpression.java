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
package org.apache.qpid.server.query.engine.parsing.expression.function.nulls;

import java.util.List;
import java.util.Objects;

import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;

/**
 * The COALESCE() function returns the first non-null value in a list.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
public class CoalesceExpression<T, R> extends AbstractFunctionExpression<T, R>
{
    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public CoalesceExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireMinParameters(1, args,this);
    }

    /**
     * Performs coalesce operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting value
     */
    @Override
    @SuppressWarnings("unchecked")
    public R apply(final T value)
    {
        return (R) getChildren().stream().map(child -> child.apply(value)).filter(Objects::nonNull).findFirst().orElse(null);
    }
}
