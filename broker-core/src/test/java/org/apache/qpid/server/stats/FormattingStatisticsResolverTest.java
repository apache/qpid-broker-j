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

package org.apache.qpid.server.stats;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.NumberFormat;
import java.util.Date;
import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.UnitTestBase;

public class FormattingStatisticsResolverTest extends UnitTestBase
{
    private static final String LARGEST_POSITIVE_VALUE_STAT_NAME = "largestPositiveValue";
    private static final String LARGER_POSITIVE_VALUE_STAT_NAME = "largerPositiveValue";
    private static final String POSITIVE_VALUE_STAT_NAME = "positiveValue";
    private static final String ZERO_VALUE_STAT_NAME = "zeroValue";
    private static final String NEGATIVE_VALUE_STAT_NAME = "negativeValue";
    private static final String SMALLER_NEGATIVE_VALUE_STAT_NAME = "smallerNegativeValue";
    private static final String SMALLEST_NEGATIVE_VALUE_STAT_NAME = "smallestNegativeValue";
    private static final String EPOCH_DATE_STAT_NAME = "epochDateStatName";

    private FormattingStatisticsResolver _resolver;

    @Before
    public void setUp() throws Exception
    {
        final ConfiguredObject<?> object = mock(ConfiguredObject.class);

        final Map<String, Object> statisticsMap = Maps.newHashMap();
        statisticsMap.put(LARGEST_POSITIVE_VALUE_STAT_NAME, (1024L * 1024L) + 1L );
        statisticsMap.put(LARGER_POSITIVE_VALUE_STAT_NAME, 1025L);
        statisticsMap.put(POSITIVE_VALUE_STAT_NAME, 10L);
        statisticsMap.put(NEGATIVE_VALUE_STAT_NAME, -1L);
        statisticsMap.put(SMALLER_NEGATIVE_VALUE_STAT_NAME, -1025L);
        statisticsMap.put(SMALLEST_NEGATIVE_VALUE_STAT_NAME, (-1024L * 1024L) - 1L );
        statisticsMap.put(ZERO_VALUE_STAT_NAME, 0L);
        statisticsMap.put(EPOCH_DATE_STAT_NAME, new Date(0L));

        when(object.getStatistics()).thenReturn(statisticsMap);
        _resolver = new FormattingStatisticsResolver(object);
    }

    @Test
    public void testNoFormatting() throws Exception
    {
        assertEquals("10", _resolver.resolve(POSITIVE_VALUE_STAT_NAME, null));
        assertEquals("0", _resolver.resolve(ZERO_VALUE_STAT_NAME, null));
        assertEquals("-1", _resolver.resolve(NEGATIVE_VALUE_STAT_NAME, null));
    }

    @Test
    public void testDuration() throws Exception
    {
        assertEquals("PT17M28.577S",
                            _resolver.resolve(LARGEST_POSITIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.DURATION, null));
        assertEquals("PT0S",
                            _resolver.resolve(ZERO_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.DURATION, null));
        assertEquals("-",
                            _resolver.resolve(NEGATIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.DURATION, null));
    }

    @Test
    public void testDateTime() throws Exception
    {
        assertEquals("1970-01-01T00:00:00Z",
                            _resolver.resolve(ZERO_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.DATETIME,
                                              null));
        assertEquals("1970-01-01T00:00:00Z",
                            _resolver.resolve(EPOCH_DATE_STAT_NAME + ":" + FormattingStatisticsResolver.DATETIME, null));
        assertEquals("-",
                            _resolver.resolve(NEGATIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.DATETIME,
                                              null));
    }

    @Test
    public void testIEC80000BinaryPrefixed() throws Exception
    {
        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setMinimumFractionDigits(1);
        String ONE_POINT_ZERO = formatter.format(1L);

        assertEquals(ONE_POINT_ZERO + " MiB",
                            _resolver.resolve(LARGEST_POSITIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.BYTEUNIT, null));
        assertEquals(ONE_POINT_ZERO + " KiB",
                            _resolver.resolve(LARGER_POSITIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.BYTEUNIT, null));
        assertEquals("10 B",
                            _resolver.resolve(POSITIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.BYTEUNIT, null));
        assertEquals("0 B",
                            _resolver.resolve(ZERO_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.BYTEUNIT, null));
        assertEquals("-1 B",
                            _resolver.resolve(NEGATIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.BYTEUNIT, null));
        assertEquals("-"+ ONE_POINT_ZERO + " KiB",
                            _resolver.resolve(SMALLER_NEGATIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.BYTEUNIT, null));
        assertEquals("-" + ONE_POINT_ZERO + " MiB",
                            _resolver.resolve(SMALLEST_NEGATIVE_VALUE_STAT_NAME + ":" + FormattingStatisticsResolver.BYTEUNIT, null));
    }

}
