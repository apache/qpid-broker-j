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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Model / DTO class containing query evaluation results
 *
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class EvaluationResult<R>
{
    private List<Map<String, R>> _results;
    private Long _total;

    public EvaluationResult(final List<Map<String, R>> results, final Long total)
    {
        this._results = new ArrayList<>(results);
        this._total = total;
    }

    public List<Map<String, R>> getResults()
    {
        return Collections.unmodifiableList(_results);
    }

    public void setResults(final List<Map<String, R>> results)
    {
        this._results = new ArrayList<>(results);
    }

    public Long getTotal()
    {
        return _total;
    }

    public void setTotal(final Long total)
    {
        this._total = total;
    }
}
