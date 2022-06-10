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

import java.util.Collection;

import org.apache.qpid.server.query.engine.parsing.expression.AbstractExpressionNode;

/**
 * Collection object accessor retrieves values from a collection
 *
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class CollectionObjectAccessor<R> extends AbstractExpressionNode<Collection<R>, R>
{
    /**
     * Index
     */
    private final Integer _index;

    /**
     * Constructor stores index value
     *
     * @param index Collection index
     */
    public CollectionObjectAccessor(final Integer index)
    {
        super();
        _index = index;
    }

    /**
     * Evaluates expression using parameters and the value supplied
     *
     * @param items Collection to handle
     *
     * @return Evaluation result
     */
    @Override
    public R apply(final Collection<R> items)
    {
        return items == null ? null : items.stream().skip(_index).findFirst().orElse(null);
    }
}
