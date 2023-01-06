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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.ExpressionParser;
import org.apache.qpid.server.query.engine.parsing.query.QueryExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Tests designed to verify the {@link SelectExpressionValidator} functionality
 */
public class SelectExpressionValidatorTest
{
    private final SelectExpressionValidator _validator = new SelectExpressionValidator();

    @BeforeEach()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void nullSelectExpression()
    {
        try
        {
            _validator.validate(null);
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.VALIDATION.SELECT_EXPRESSION_NULL, e.getMessage());
        }
    }

    @Test()
    public void emptySelectExpression()
    {
        try
        {
            _validator.validate(new SelectExpression<>());
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.MISSING_EXPRESSION, e.getMessage());
        }
    }

    @Test()
    public <T, R> void selectWithoutFrom()
    {
        try
        {
            final ExpressionParser<T, R> parser = new ExpressionParser<>();
            final QueryExpression<T, R> query = parser.parseQuery("select id");
            final SelectExpression<T, R> select = query.getSelect().getSelections().get(0);
            _validator.validate(select);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.KEYWORD_FROM_NOT_FOUND, e.getMessage());
        }
    }

    @Test()
    public <T, R> void missingGroupByItem()
    {
        try
        {
            final ExpressionParser<T, R> parser = new ExpressionParser<>();
            final QueryExpression<T, R> query = parser.parseQuery("select count(*), overflowPolicy from queue");
            final SelectExpression<T, R> select = query.getSelect().getSelections().get(0);
            _validator.validate(select);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Not a single-group group function: projections [overflowPolicy] should be included in GROUP BY clause", e.getMessage());
        }

        try
        {
            final ExpressionParser<T, R> parser = new ExpressionParser<>();
            final QueryExpression<T, R> query = parser.parseQuery("select count(*), overflowPolicy, expiryPolicy from queue group by overflowPolicy");
            final SelectExpression<T, R> select = query.getSelect().getSelections().get(0);
            _validator.validate(select);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Not a single-group group function: projections [expiryPolicy] should be included in GROUP BY clause", e.getMessage());
        }
    }

    @Test()
    public <T, R> void havingWithoutAggregation()
    {
        try
        {
            final ExpressionParser<T, R> parser = new ExpressionParser<>();
            final QueryExpression<T, R> query = parser.parseQuery("select * from queue having name='QUEUE_1'");
            final SelectExpression<T, R> select = query.getSelect().getSelections().get(0);
            _validator.validate(select);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.HAVING_WITHOUT_AGGREGATION, e.getMessage());
        }
    }
}
