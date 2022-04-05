
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
package org.apache.qpid.server.query.engine.parsing.expression;

import java.util.List;

import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.AbstractAggregationExpression;

/**
 * Node of which expression tree consist of
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
public interface ExpressionNode<T, R> extends Expression<T, R>
{
    /**
     * Determines whether expression contains aggregation nodes or not
     *
     * @return True or false
     */
    boolean containsAggregation();

    /**
     * Determines whether expression is an accessor or not
     *
     * @return True or false
     */
    boolean isAccessor();

    /**
     * Determines whether expression can be evaluated instantly or not
     *
     * @return True or false
     */
    boolean isInstantlyEvaluable();

    /**
     * Returns expression alias
     *
     * @return Expression alias
     */
    String getAlias();

    /**
     * Setter for alias
     *
     * @param alias Expression alias
     */
    void setAlias(String alias);

    /**
     * Return expression parent
     *
     * @return Expression parent
     */
    <Y> ExpressionNode<T, Y> getParent();

    /**
     * Setter for parent
     *
     * @param parent Expression parent
     */
    void setParent(ExpressionNode<T, ?> parent);

    /**
     * Returns expression children
     *
     * @return List of child expressions
     */
    <Y> List<ExpressionNode<T, Y>> getChildren();

    /**
     * Returns expression children being aggregation nodes
     *
     * @param <Y> Return type
     *
     * @return List of aggregation expressions
     */
    <Y> List<AbstractAggregationExpression<T, Y>> getAggregations();
}
