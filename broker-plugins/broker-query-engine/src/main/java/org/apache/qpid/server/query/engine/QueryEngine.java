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
package org.apache.qpid.server.query.engine;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.query.engine.cache.MaxSizeHashMap;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.parsing.query.QueryExpression;

/**
 * Bridge class between broker configuration and query evaluator
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class QueryEngine
{
    /**
     * Broker instance
     */
    private final Broker<?> _broker;

    /**
     * Cache holding queries
     */
    private Map<String, QueryExpression<?, ?>> _queryCache;

    /**
     * Maximal allowed BigDecimal value.
     * Is needed to prevent heap memory consumption when calculating very large numbers.
     */
    private BigDecimal _maxBigDecimalValue = BigDecimal.valueOf(Double.MAX_VALUE).pow(4);

    /**
     * Maximal amount of queries allowed caching
     */
    private int _maxQueryCacheSize;

    /**
     * Maximal amount of query tree nodes allowed
     */
    private int _maxQueryDepth;

    /**
     * Zone id
     */
    private ZoneId _zoneId;

    /**
     * Constructor injects broker and retrieves default configuration values
     *
     * @param broker Broker instance
     */
    // mutable broker instance is stored intentionally
    @SuppressWarnings("findbugs:EI_EXPOSE_REP2")
    public QueryEngine(final Broker<?> broker)
    {
        Objects.requireNonNull(broker, "Broker instance not provided for querying");
        _broker = broker;
    }

    /**
     * Initializes query cache
     */
    public void initQueryCache()
    {
        _queryCache = _maxQueryCacheSize > 0 ? new MaxSizeHashMap<>(_maxQueryCacheSize) : null;
    }

    /**
     * Creates query evaluator
     *
     * @return QueryEvaluator instance
     */
    public QueryEvaluator createEvaluator()
    {
        final QuerySettings defaultQuerySettings = new QuerySettings();
        defaultQuerySettings.setMaxBigDecimalValue(_maxBigDecimalValue);
        defaultQuerySettings.setMaxQueryCacheSize(_maxQueryCacheSize);
        defaultQuerySettings.setMaxQueryDepth(_maxQueryDepth);
        defaultQuerySettings.setZoneId(_zoneId);
        return new QueryEvaluator(_queryCache, defaultQuerySettings, _broker);
    }

    public void setMaxBigDecimalValue(final BigDecimal maxBigDecimalValue)
    {
        _maxBigDecimalValue = maxBigDecimalValue;
    }

    public void setMaxQueryCacheSize(final int maxQueryCacheSize)
    {
        _maxQueryCacheSize = maxQueryCacheSize;
    }

    public void setMaxQueryDepth(final int maxQueryDepth)
    {
        _maxQueryDepth = maxQueryDepth;
    }

    public void setZoneId(final ZoneId zoneId)
    {
        _zoneId = zoneId;
    }

    public int getCacheSize()
    {
        return _queryCache == null ? 0 : _queryCache.size();
    }
}
