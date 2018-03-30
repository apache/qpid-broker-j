/*
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
package org.apache.qpid.disttest.charting.chartbuilder;

import static org.mockito.Mockito.mock;

import org.junit.Assert;

import org.apache.qpid.disttest.charting.ChartType;
import org.apache.qpid.disttest.charting.seriesbuilder.SeriesBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ChartBuilderFactoryTest extends UnitTestBase
{
    private SeriesBuilder _seriesBuilder = mock(SeriesBuilder.class);

    @Test
    public void testLineChart()
    {
        ChartBuilder builder = ChartBuilderFactory.createChartBuilder(ChartType.LINE, _seriesBuilder);
        final boolean condition = builder instanceof LineChartBuilder;
        assertTrue(condition);
    }

    @Test
    public void testLineChart3D()
    {
        ChartBuilder builder = ChartBuilderFactory.createChartBuilder(ChartType.LINE3D, _seriesBuilder);
        final boolean condition = builder instanceof LineChart3DBuilder;
        assertTrue(condition);
    }

    @Test
    public void testBarChart()
    {
        ChartBuilder builder = ChartBuilderFactory.createChartBuilder(ChartType.BAR, _seriesBuilder);
        final boolean condition = builder instanceof BarChartBuilder;
        assertTrue(condition);
    }

    @Test
    public void testBarChart3D()
    {
        ChartBuilder builder = ChartBuilderFactory.createChartBuilder(ChartType.BAR3D, _seriesBuilder);
        final boolean condition = builder instanceof BarChart3DBuilder;
        assertTrue(condition);
    }

    @Test
    public void testXYLineChart()
    {
        ChartBuilder builder = ChartBuilderFactory.createChartBuilder(ChartType.XYLINE, _seriesBuilder);
        final boolean condition = builder instanceof XYLineChartBuilder;
        assertTrue(condition);
    }

    @Test
    public void testTimeSeriesLineChart()
    {
        ChartBuilder builder = ChartBuilderFactory.createChartBuilder(ChartType.TIMELINE, _seriesBuilder);
        final boolean condition = builder instanceof TimeSeriesLineChartBuilder;
        assertTrue(condition);
    }
}
