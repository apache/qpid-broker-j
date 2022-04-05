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
package org.apache.qpid.server.query.engine.parsing.expression.accessor;

import static org.apache.qpid.server.query.engine.evaluator.EvaluationContext.STATISTICS;

import java.util.Map;

import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;

/**
 * Map object accessor retrieves value from a map
 *
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class MapObjectAccessor<R> extends AbstractExpressionNode<Map<String, R>, R>
{
    /**
     * Object property name (map key)
     */
    private final String _property;

    /**
     * Constructor object property name
     *
     * @param property Object property name
     */
    public MapObjectAccessor(final String property)
    {
        super();
        _property = property;
    }

    /**
     * Evaluates expression using parameters and the value supplied
     *
     * @param map Map to handle
     *
     * @return Evaluation result
     */
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public R apply(final Map<String, R> map) {
        if ("*".equals(_property))
        {
            return (R) map;
        }
        if (map.containsKey(STATISTICS) && map.get(STATISTICS) instanceof Map && ((Map) map.get(STATISTICS)).containsKey(_property))
        {
            return (R) ((Map) map.get(STATISTICS)).get(_property);
        }
        if (!map.containsKey(_property))
        {
            throw QueryEvaluationException.fieldNotFound(_property);
        }
        return map.get(_property);
    }
}
