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
package org.apache.qpid.server.query.engine.parsing.expression.function.numeric;

import java.math.BigDecimal;
import java.util.List;

import org.apache.qpid.server.query.engine.parsing.converter.NumberConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * ABS returns the absolute value of a number.
 *
 * @param <T> Input parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class AbsExpression<T> extends AbstractFunctionExpression<T, Number>
{
    /**
     * Argument type validator
     */
    private final FunctionParameterTypePredicate<Number> _typeValidator = FunctionParameterTypePredicate.<Number>builder()
        .allowNumbers()
        .build();

    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public AbsExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireParameters(1, args, this);
    }

    /**
     * Performs abs operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting number
     */
    @Override
    public Number apply(final T value)
    {
        final Number arg = evaluateChild(0, value, _typeValidator);
        return NumberConverter.narrow(new BigDecimal(arg.toString()).abs());
    }
}
