/*
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
 */

package org.apache.qpid.server.filter;

public class OrderByExpression implements Expression
{
    public enum Order
    { ASC, DESC }

    private final Expression _expression;
    private final Order _order;

    public OrderByExpression(Expression expression)
    {
        this(expression, Order.ASC);
    }

    public OrderByExpression(Expression expression, Order order)
    {
        _expression = expression;
        _order = order;
    }

    @Override
    public Object evaluate(final Object object)
    {
        return _expression.evaluate(object);
    }

    public Order getOrder()
    {
        return _order;
    }

    public boolean isColumnIndex()
    {
        return (_expression instanceof ConstantExpression && ((ConstantExpression)_expression).getValue() instanceof Number);
    }

    public int getColumnIndex()
    {
        return ((Number)((ConstantExpression)_expression).getValue()).intValue();
    }
}
