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
package org.apache.qpid.disttest.charting.definition;

import static org.apache.qpid.disttest.charting.definition.ChartingDefinitionCreator.CHART_DESCRIPTION_KEY;
import static org.apache.qpid.disttest.charting.definition.ChartingDefinitionCreator.CHART_SUBTITLE_KEY;
import static org.apache.qpid.disttest.charting.definition.ChartingDefinitionCreator.CHART_TITLE_KEY;
import static org.apache.qpid.disttest.charting.definition.ChartingDefinitionCreator.CHART_TYPE_KEY;
import static org.apache.qpid.disttest.charting.definition.ChartingDefinitionCreator.XAXIS_TITLE_KEY;
import static org.apache.qpid.disttest.charting.definition.ChartingDefinitionCreator.YAXIS_TITLE_KEY;
import static org.apache.qpid.disttest.charting.definition.SeriesDefinitionCreator.SERIES_STATEMENT_KEY_FORMAT;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;

import org.apache.qpid.disttest.charting.ChartType;
import org.apache.qpid.disttest.charting.ChartingException;

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

public class ChartingDefinitionCreatorTest extends UnitTestBase
{
    private static final String TEST_CHART_TITLE = "CHART_TITLE ${ChartingDefinitionSysProp}";
    private static final String TEST_CHART_SUBTITLE = "CHART_SUBTITLE";
    private static final String TEST_CHART_DESCRIPTION = "CHART_DESCRIPTION";
    private static final String TEST_XAXIS_TITLE = "XAXIS_TITLE";
    private static final String TEST_YAXIS_TITLE = "YAXIS_TITLE";
    private static final ChartType TEST_CHART_TYPE = ChartType.LINE;

    private static final String TEST_SERIES_SELECT_STATEMENT = "SERIES_SELECT_STATEMENT";

    private static final String SYSTEM_PROPERTY_NAME = "ChartingDefinitionSysProp";

    private final ChartingDefinitionCreator _chartingDefinitionLoader = new ChartingDefinitionCreator();
    private File _testTempDir;

    @Before
    public void setUp() throws Exception
    {
        _testTempDir = createTestTemporaryDirectory();
    }

    @Test
    public void testLoadTwoDefinitionsFromDirectory() throws Exception
    {
        createTestDefinitionWithin(_testTempDir);
        createTestDefinitionWithin(_testTempDir);

        List<ChartingDefinition> definitions = _chartingDefinitionLoader.createFromFileOrDirectory(_testTempDir.getAbsolutePath());

        assertEquals((long) 2, (long) definitions.size());
    }

    @Test
    public void testLoadOneDefinitionFromFile() throws Exception
    {
        File testDefFile = createTestDefinitionWithin(_testTempDir);

        List<ChartingDefinition> definitions = _chartingDefinitionLoader.createFromFileOrDirectory(testDefFile.getAbsolutePath());
        assertEquals((long) 1, (long) definitions.size());

        ChartingDefinition definition1 = definitions.get(0);
        assertEquals(TEST_CHART_TITLE, definition1.getChartTitle());
    }

    @Test
    public void testDefinitionsProperties() throws Exception
    {
        setTestSystemProperty(SYSTEM_PROPERTY_NAME, "propValue");
        File testDefFile = createTestDefinitionWithin(_testTempDir);

        List<ChartingDefinition> definitions = _chartingDefinitionLoader.createFromFileOrDirectory(testDefFile.getAbsolutePath());
        assertEquals((long) 1, (long) definitions.size());

        ChartingDefinition definition1 = definitions.get(0);
        assertEquals("CHART_TITLE propValue", definition1.getChartTitle());
        assertEquals(TEST_CHART_SUBTITLE, definition1.getChartSubtitle());
        assertEquals(TEST_CHART_DESCRIPTION, definition1.getChartDescription());
        assertEquals(TEST_XAXIS_TITLE, definition1.getXAxisTitle());
        assertEquals(TEST_YAXIS_TITLE, definition1.getYAxisTitle());
        assertEquals(TEST_CHART_TYPE, definition1.getChartType());

        String stemOnly = testDefFile.getName().replaceFirst("\\.chartdef", "");
        assertEquals(stemOnly, definition1.getChartStemName());

        final List<SeriesDefinition> seriesDefinitions = definition1.getSeriesDefinitions();
        assertEquals((long) 1, (long) seriesDefinitions.size());
        SeriesDefinition seriesDefinition = seriesDefinitions.get(0);
        assertEquals(TEST_SERIES_SELECT_STATEMENT, seriesDefinition.getSeriesStatement());
    }

    @Test
    public void testDefinitionFileNotFound() throws Exception
    {
        File notFound = new File(_testTempDir,"notfound.chartdef");
        assertFalse(notFound.exists());

        try
        {
            _chartingDefinitionLoader.createFromFileOrDirectory(notFound.getAbsolutePath());
            fail("Exception not thrown");
        }
        catch(ChartingException ce)
        {
            // PASS
        }
    }

    private File createTestDefinitionWithin(File _testTempDir) throws Exception
    {
        final String testDefFileName = "test." + System.nanoTime() +  ".chartdef";
        File chartDef = new File(_testTempDir, testDefFileName);
        chartDef.createNewFile();

        Properties props = new Properties();
        props.setProperty(CHART_TYPE_KEY, TEST_CHART_TYPE.name());
        props.setProperty(CHART_TITLE_KEY, TEST_CHART_TITLE);
        props.setProperty(CHART_SUBTITLE_KEY, TEST_CHART_SUBTITLE);
        props.setProperty(CHART_DESCRIPTION_KEY, TEST_CHART_DESCRIPTION);
        props.setProperty(XAXIS_TITLE_KEY, TEST_XAXIS_TITLE);
        props.setProperty(YAXIS_TITLE_KEY, TEST_YAXIS_TITLE);

        props.setProperty(String.format(SERIES_STATEMENT_KEY_FORMAT, 1), TEST_SERIES_SELECT_STATEMENT);

        final FileWriter writer = new FileWriter(chartDef);
        try
        {
            props.store(writer, "Test chart definition file");
        }
        finally
        {
            writer.close();
        }

        return chartDef;
    }

    private File createTestTemporaryDirectory() throws Exception
    {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "testdef" + System.nanoTime());
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();
        return tmpDir;
    }
}
