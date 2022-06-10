package org.apache.qpid.server.query.engine.validation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.factory.FunctionExpressionFactory;

public class FunctionParametersValidatorTest
{
    @Before()
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
