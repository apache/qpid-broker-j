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
package org.apache.qpid.server.query.engine.parsing.expression.function.datetime;

import java.time.Instant;
import java.util.List;

import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;

/**
 * The CURRENT_TIMESTAMP() function returns current date and time.
 *
 * @param <T> Input parameter type
 */
public class CurrentTimestampExpression<T> extends AbstractFunctionExpression<T, Instant>
{
    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public CurrentTimestampExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        _metadata.setAlias(alias);
    }

    /**
     * Returns current datetime
     *
     * @param value Object to handle
     *
     * @return Current datetime
     */
    @Override
    public Instant apply(final T value)
    {
        return Instant.now();
    }
}
