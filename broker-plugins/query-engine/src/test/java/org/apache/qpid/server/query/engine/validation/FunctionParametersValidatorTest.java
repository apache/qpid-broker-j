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
package org.apache.qpid.server.query.engine.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.factory.FunctionExpressionFactory;

/**
 * Tests designed to verify the {@link FunctionParametersValidator} functionality
 */
public class FunctionParametersValidatorTest
{
    @BeforeEach()
    public void setUp()
    {
        final EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public <T, R> void requireParametersSuccess()
    {
        final List<ExpressionNode<T, ?>> args = Collections.singletonList(ConstantExpression.of(0));
        final AbstractFunctionExpression<T, R> function = (AbstractFunctionExpression<T, R>)
            FunctionExpressionFactory.createFunction("abs(0)", "ABS", args);
        FunctionParametersValidator.requireParameters(1, args, function);
    }

    @Test()
    public <T, R> void requireZeroParameters()
    {
        try
        {
            final List<ExpressionNode<T, ?>> args = Collections.singletonList((ConstantExpression.of(0)));
            final AbstractFunctionExpression<T, R> function = (AbstractFunctionExpression<T, R>)
                FunctionExpressionFactory.createFunction("abs(0)", "ABS", args);
            FunctionParametersValidator.requireParameters(0, args, function);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'ABS' requires 0 parameter", e.getMessage());
        }
    }

    @Test()
    public <T, R> void requireMinMaxParametersSuccess()
    {
        final List<ExpressionNode<T, ?>> args = Collections.singletonList((ConstantExpression.of(0)));
        final AbstractFunctionExpression<T, R> function = (AbstractFunctionExpression<T, R>)
                FunctionExpressionFactory.createFunction("abs(0)", "ABS", args);
        FunctionParametersValidator.requireMinParameters(1, args, function);
        FunctionParametersValidator.requireMaxParameters(1, args, function);
    }
}
