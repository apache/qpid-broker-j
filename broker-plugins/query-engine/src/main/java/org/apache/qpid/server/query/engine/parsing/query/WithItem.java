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
package org.apache.qpid.server.query.engine.parsing.query;

import java.util.List;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.expression.set.SetExpression;

/**
 * Contains information about a single item in a WITH clause
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class WithItem<T, R>
{
    /**
     * Subquery name
     */
    private String _name;

    /**
     * Subquery
     */
    private QueryExpression<T, R> _query;

    /**
     * Constructor stores properties and validates subquery
     *
     * @param name Subquery name
     * @param aliases Subquery aliases (entity field names)
     * @param query Subquery
     */
    // sonar: mutable query object is stored intentionally
    @SuppressWarnings("findbugs:EI_EXPOSE_REP2")
    public WithItem(final String name, final List<String> aliases, final QueryExpression<T, R> query)
    {
        _name = name;
        _query = query;
        if (aliases != null && !aliases.isEmpty())
        {
            final SetExpression<T, R> setExpression = _query.getSelect();
            if (setExpression.getProjections().size() != aliases.size())
            {
                throw QueryParsingException.of(Errors.VALIDATION.WITH_ITEM_COMLUMNS_INVALID);
            }
            for (final SelectExpression<T, R> select : setExpression.getSelections())
            {
                final List<ProjectionExpression<T, R>> projections = select.getProjections();
                for (int i = 0; i < aliases.size(); i++)
                {
                    projections.get(i).setAlias(aliases.get(i));
                }
            }
        }
    }

    public String getName()
    {
        return _name;
    }

    public void setName(final String name)
    {
        _name = name;
    }

    // sonar: mutable query object is returned intentionally
    @SuppressWarnings("findbugs:EI_EXPOSE_REP")
    public QueryExpression<T, R> getQuery()
    {
        return _query;
    }

    // sonar: mutable query object is returned intentionally
    @SuppressWarnings("findbugs:EI_EXPOSE_REP2")
    public void setQuery(final QueryExpression<T, R> query)
    {
        _query = query;
    }
}
