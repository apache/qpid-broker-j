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
package org.apache.qpid.server.query.engine.parsing.expression.comparison;

import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;

/**
 * Comparison IS NULL operation. Evaluates left expression NULL value.
 *
 * @param <T> Input parameter type
 */
public class IsNullExpression<T> extends AbstractComparisonExpression<T, Boolean>
{
    /**
     * Constructor initializes children expression list
     *
     * @param left Left child expression
     */
    public IsNullExpression(final ExpressionNode<T, ?> left)
    {
        super(left);
    }

    /**
     * Performs IS NULL comparison using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Boolean result of value evaluation
     */
    @Override
    public Boolean apply(final T value)
    {
        return evaluateChild(0, value) == null;
    }

    /**
     * Returns expression alias
     *
     * @return Expression alias
     */
    @Override
    public String getAlias()
    {
        return _metadata.getAlias();
    }
}
