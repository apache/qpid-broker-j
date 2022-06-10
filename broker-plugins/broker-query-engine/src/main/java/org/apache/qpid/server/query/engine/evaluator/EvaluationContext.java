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
package org.apache.qpid.server.query.engine.evaluator;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.server.query.engine.parsing.expression.Expression;
import org.apache.qpid.server.query.engine.parsing.query.QueryExpression;

/**
 * Holds temporary values needed during query evaluation
 */
@SuppressWarnings({"java:S116", "unchecked"})
public class EvaluationContext
{
    public static final String BROKER = "broker";

    public static final String STATISTICS = "statistics";

    public static final String QUERY_AGGREGATED_RESULT = "query.aggregated.result";

    public static final String QUERY_ALIASES = "query.aliases";

    public static final String QUERY_DATETIME_PATTERN_OVERRIDEN = "query.datetime.pattern.overriden";

    public static final String QUERY_DEPTH = "query.depth";

    public static final String QUERY_ORDERING = "query.ordering";

    public static final String QUERY_ITEMS_FOR_REMOVAL = "query.items.for.removal";

    public static final String QUERY_ORDER_ITEMS_FOR_REMOVAL = "query.order.items.for.removal";

    public static final String QUERY_SETTINGS = "query.settings";

    public static final String COMPARATORS = "comparators";

    private final Map<Object, Object> _values = new ConcurrentHashMap<>();

    private final Deque<QueryExpression<?, ?>> _expressions = new LinkedBlockingDeque<>();

    public EvaluationContext put(Object key, Object value)
    {
        _values.put(key, value);
        return this;
    }

    public <T> T get(Object key, Class<T> type)
    {
        return type.cast(_values.get(key));
    }

    public <T> T get(Object key)
    {
        return (T) _values.get(key);
    }

    public boolean contains(Object key)
    {
        return _values.containsKey(key);
    }

    public <T> T remove(String key)
    {
        return (T) _values.remove(key);
    }

    public void clear()
    {
        _values.clear();
    }

    public void incrementDepth()
    {
        final AtomicInteger depth = get(EvaluationContext.QUERY_DEPTH);
        depth.incrementAndGet();
    }

    public int getDepth()
    {
        final AtomicInteger depth = get(EvaluationContext.QUERY_DEPTH);
        return depth.get();
    }

    public void startBuilding()
    {
        _values.put(EvaluationPhase.class, EvaluationPhase.BUILDING);
    }

    public boolean isBuilding()
    {
        return EvaluationPhase.BUILDING.equals(_values.get(EvaluationPhase.class));
    }

    public boolean isExecuting()
    {
        return EvaluationPhase.EXECUTING.equals(_values.get(EvaluationPhase.class));
    }

    public <T,R> void putAlias(String alias, Expression<T,R> expression)
    {
        final Map<String, Expression<T,R>> aliases = (Map<String, Expression<T, R>>) _values.get(QUERY_ALIASES);
        aliases.put(alias, expression);
    }

    public boolean containsAlias(String alias)
    {
        return get(QUERY_ALIASES, Map.class).containsKey(alias);
    }

    public <T, R> Expression<T, R> removeAlias(String alias)
    {
        return (Expression<T, R>) get(QUERY_ALIASES, Map.class).remove(alias);
    }

    public <T, R> Expression<T, R> getAlias(String alias)
    {
        return (Expression<T, R>) get(QUERY_ALIASES, Map.class).get(alias);
    }

    public <T, R> void startExecution(QueryExpression<T, R> query)
    {
        _expressions.push(query);
        _values.put(EvaluationPhase.class, EvaluationPhase.EXECUTING);
        _values.put(QUERY_ALIASES, new HashMap<>());
    }

    public <T, R> QueryExpression<T, R> currentExecution()
    {
        return (QueryExpression<T, R>) _expressions.peek();
    }
}
