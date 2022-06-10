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
package org.apache.qpid.server.query.engine.parsing.expression.set;

import java.util.List;

import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.query.ProjectionExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Parent of select expression and set operations expressions
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
public interface SetExpression<T, R> extends ExpressionNode<T, R>
{
    /**
     * Returns list of projections
     *
     * @return List of projections
     */
    List<ProjectionExpression<T, R>> getProjections();

    /**
     * Returns list of select expressions
     *
     * @return List of select expressions
     */
    List<SelectExpression<T,R>> getSelections();
}
