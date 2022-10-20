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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.parsing.expression.function.aggregation.AbstractAggregationExpression;

/**
 * Expression metadata
 *
 * @param <T> Input parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class Metadata<T>
{
    /**
     * Expression alias
     */
    private String _alias;

    /**
     * Flag shows whether expression is an accessor or not
     */
    private boolean _accessor;

    /**
     * List of nested aggregation expressions
     */
    private final List<AbstractAggregationExpression<T, ?>> _aggregations = new ArrayList<>();

    public String getAlias()
    {
        return _alias;
    }

    public void setAlias(final String alias)
    {
        _alias = alias;
    }

    public boolean isAccessor()
    {
        return _accessor;
    }

    public void setAccessor(final boolean accessor)
    {
        _accessor = accessor;
    }

    public boolean isAggregation()
    {
        return !_aggregations.isEmpty();
    }

    public void addAggregation(final AbstractAggregationExpression<T, ?> aggregation)
    {
        _aggregations.add(aggregation);
    }

    @SuppressWarnings("unchecked")
    public <Y> List<AbstractAggregationExpression<T, Y>> getAggregations()
    {
        return _aggregations.stream().map(item -> (AbstractAggregationExpression<T, Y>) item).collect(Collectors.toList());
    }
}
