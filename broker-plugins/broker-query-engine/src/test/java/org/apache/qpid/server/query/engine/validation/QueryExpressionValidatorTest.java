package org.apache.qpid.server.query.engine.validation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.query.QueryExpression;

public class QueryExpressionValidatorTest
{
    private final QueryExpressionValidator _validator = new QueryExpressionValidator();

    @Before()
    public void setUp()
    {
        final EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void nullQueryExpression()
    {
        try
        {
            _validator.validate(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.QUERY_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void emptyQueryExpression()
    {
        try
        {
            _validator.validate(new QueryExpression<>());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.MISSING_EXPRESSION, e.getMessage());
        }
    }
}
