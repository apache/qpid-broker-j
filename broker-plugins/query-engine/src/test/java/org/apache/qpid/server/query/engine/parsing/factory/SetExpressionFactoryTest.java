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
package org.apache.qpid.server.query.engine.parsing.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.set.IntersectExpression;
import org.apache.qpid.server.query.engine.parsing.expression.set.MinusExpression;
import org.apache.qpid.server.query.engine.parsing.expression.set.UnionExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Tests designed to verify the {@link SetExpressionFactory} functionality
 */
public class SetExpressionFactoryTest
{
    @BeforeEach()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public <T> void intersect()
    {
        final ExpressionNode<T, Stream<Map<String, Object>>> left = new SelectExpression<>();
        final ExpressionNode<T, Stream<Map<String, Object>>> right = new SelectExpression<>();
        final ExpressionNode<T, Stream<Map<String, Object>>> expression = SetExpressionFactory.intersect(true, left, right);
        assertEquals(IntersectExpression.class, expression.getClass());
    }

    @Test()
    public <T> void minus()
    {
        final ExpressionNode<T, Stream<Map<String, Object>>> left = new SelectExpression<>();
        final ExpressionNode<T, Stream<Map<String, Object>>> right = new SelectExpression<>();
        final ExpressionNode<T, Stream<Map<String, Object>>> expression = SetExpressionFactory.minus(true, left, right);
        assertEquals(MinusExpression.class, expression.getClass());
    }

    @Test()
    public <T> void union()
    {
        final ExpressionNode<T, Stream<Map<String, Object>>> left = new SelectExpression<>();
        final ExpressionNode<T, Stream<Map<String, Object>>> right = new SelectExpression<>();
        final ExpressionNode<T, Stream<Map<String, Object>>> expression = SetExpressionFactory.union(true, left, right);
        assertEquals(UnionExpression.class, expression.getClass());
    }
}
