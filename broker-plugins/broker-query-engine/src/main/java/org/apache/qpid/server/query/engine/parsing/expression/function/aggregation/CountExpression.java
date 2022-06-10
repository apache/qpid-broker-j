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
package org.apache.qpid.server.query.engine.parsing.expression.function.aggregation;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.collector.CollectorType;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.literal.StringLiteralExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;

/**
 * The COUNT() function returns the number of items that matches a specified criterion.
 *
 * @param <T> Input parameter type
 */
public class CountExpression<T> extends AbstractAggregationExpression<T, Number>
{
    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public CountExpression(String alias, List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireMinParameters(1, args,this);
        FunctionParametersValidator.requireMaxParameters(2, args, this);
    }

    /**
     * Returns counting collector type
     *
     * @return CollectorType instance
     */
    @Override
    public CollectorType getCollectorType()
    {
        return CollectorType.COUNTING;
    }

    /**
     * Performs count operation using parameters and the value supplied
     *
     * @param items Collection to perform aggregation on
     *
     * @return Aggregated result
     */
    @Override
    public Number aggregate(final List<T> items)
    {
        Stream<?> stream;
        Expression<T, ?> expression;

        if (getChildren().size() == 1)
        {
            expression = getChild(0);
            stream = items.stream().map(expression);
        }
        else
        {
            final Expression<T, ?> first = getChild(0);
            if (!(first instanceof StringLiteralExpression
                && Objects.equals("distinct", String.valueOf(first.apply(null)).toLowerCase(Locale.US))))
            {
                throw QueryParsingException.of(Errors.AGGREGATION.DISTINCT_NOT_FOUND);
            }
            expression = getChild(1);
            stream = items.stream().map(expression);
            stream = stream.distinct();
        }
        return NumberConverter.narrow(stream.filter(Objects::nonNull).count());
    }
}
