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

import java.math.BigDecimal;
import java.util.List;
import java.util.OptionalDouble;

import org.apache.qpid.server.query.engine.parsing.collector.CollectorType;
import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * The AVG() function returns the average value of a numeric collection.
 *
 * @param <T> Input parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class AvgExpression<T> extends AbstractAggregationExpression<T, Double>
{
    /**
     * Argument type validator
     */
    private final FunctionParameterTypePredicate<Object> _typeValidator = FunctionParameterTypePredicate.builder()
        .allowNumbers()
        .build();

    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public AvgExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireParameters(1, args, this);
    }

    /**
     * Returns averaging collector type
     *
     * @return CollectorType instance
     */
    @Override
    public CollectorType getCollectorType()
    {
        return CollectorType.AVERAGING;
    }

    /**
     * Performs avg operation using parameters and the value supplied
     *
     * @param items Collection to perform aggregation on
     *
     * @return Aggregated result
     */
    @Override
    public Double aggregate(final List<T> items)
    {
        final List<?> args = getArguments(items, _typeValidator);
        final OptionalDouble optionalDouble = args.stream().mapToDouble(NumberConverter::toDouble).average();
        return optionalDouble.isPresent() ? NumberConverter.narrow(BigDecimal.valueOf(optionalDouble.getAsDouble())) : null;
    }
}
