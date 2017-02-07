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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.qpid.disttest.charting.ChartType;
import org.apache.qpid.disttest.charting.ChartingException;
import org.apache.qpid.server.util.Strings;

public class ChartingDefinitionCreator
{
    public static final String CHARTDEF_FILE_EXTENSION = ".chartdef";

    public static final String CHART_TYPE_KEY = "chartType";
    public static final String CHART_TITLE_KEY = "chartTitle";
    public static final String CHART_SUBTITLE_KEY = "chartSubtitle";
    public static final String CHART_DESCRIPTION_KEY = "chartDescription";
    public static final String XAXIS_TITLE_KEY = "xAxisTitle";
    public static final String YAXIS_TITLE_KEY = "yAxisTitle";
    public static final String Y_AXIS_LOWER_BOUND_KEY = "yAxisLowerBound";
    public static final String Y_AXIS_UPPER_BOUND_KEY = "yAxisUpperBound";

    private SeriesDefinitionCreator _seriesDefinitionCreator = new SeriesDefinitionCreator();

    public List<ChartingDefinition> createFromFileOrDirectory(String chartingDefinitionFileOrDirectory)
    {
        List<ChartingDefinition> definitions = new ArrayList<ChartingDefinition>();

        File fileOrDirectory = new File(chartingDefinitionFileOrDirectory);
        if (fileOrDirectory.isDirectory())
        {
            File[] matchingFiles = fileOrDirectory.listFiles(new CHARTDEF_FILE_FILTER());

            for (File file : matchingFiles)
            {
                final ChartingDefinition chartDefinition = createChartDefinition(file);
                definitions.add(chartDefinition);
            }
        }
        else
        {
            final ChartingDefinition chartDefinition = createChartDefinition(fileOrDirectory);
            definitions.add(chartDefinition);
        }

        return definitions;
    }

    private ChartingDefinition createChartDefinition(File file)
    {
        Reader reader = null;
        try
        {
            reader = new BufferedReader(new FileReader(file));
            Properties props = new Properties();
            props.load(reader);

            final String chartStemName = getStemNameFrom(file);

            final ChartType chartType = ChartType.valueOf(Strings.expand(props.getProperty(CHART_TYPE_KEY), false, Strings.SYSTEM_RESOLVER));
            final String chartTitle = Strings.expand(props.getProperty(CHART_TITLE_KEY), false, Strings.SYSTEM_RESOLVER);
            final String chartSubtitle = Strings.expand(props.getProperty(CHART_SUBTITLE_KEY), false, Strings.SYSTEM_RESOLVER);
            final String chartDescription = Strings.expand(props.getProperty(CHART_DESCRIPTION_KEY), false, Strings.SYSTEM_RESOLVER);
            final String xAxisTitle = Strings.expand(props.getProperty(XAXIS_TITLE_KEY), false, Strings.SYSTEM_RESOLVER);
            final String yAxisTitle = Strings.expand(props.getProperty(YAXIS_TITLE_KEY), false, Strings.SYSTEM_RESOLVER);
            String yAxisLowerBoundStr = Strings.expand(props.getProperty(Y_AXIS_LOWER_BOUND_KEY), false, Strings.SYSTEM_RESOLVER);
            String yAxisUpperBoundStr = Strings.expand(props.getProperty(Y_AXIS_UPPER_BOUND_KEY), false, Strings.SYSTEM_RESOLVER);
            final Integer yAxisLowerBound =  yAxisLowerBoundStr == null ? null : Integer.valueOf(yAxisLowerBoundStr);
            final Integer yAxisUpperBound = yAxisUpperBoundStr == null ? null : Integer.valueOf(yAxisUpperBoundStr);

            final List<SeriesDefinition> seriesDefinitions = createSeriesDefinitions(props);

            final ChartingDefinition chartDefinition = new ChartingDefinition(chartStemName,
                                                                              chartType,
                                                                              chartTitle,
                                                                              chartSubtitle,
                                                                              chartDescription,
                                                                              xAxisTitle, yAxisTitle,
                                                                              yAxisLowerBound, yAxisUpperBound,
                                                                              seriesDefinitions);
            return chartDefinition;
        }
        catch (IOException e)
        {
            throw new ChartingException("Unable to open file " + file, e);
        }
        finally
        {
            if (reader != null)
            {
                try
                {
                    reader.close();
                }
                catch (IOException e)
                {
                    throw new ChartingException(e);
                }
            }
        }

    }

    private String getStemNameFrom(File file)
    {
        final String nameWithExtension = file.getName();
        final String nameWithoutExtensionPart = nameWithExtension.replaceAll("\\.[^\\.]*$", "");
        return nameWithoutExtensionPart;
    }

    private List<SeriesDefinition> createSeriesDefinitions(Properties props)
    {
        return _seriesDefinitionCreator.createFromProperties(props);
    }

    private final class CHARTDEF_FILE_FILTER implements FileFilter
    {
        @Override
        public boolean accept(File pathname)
        {
            return pathname.isFile() && pathname.getName().endsWith(CHARTDEF_FILE_EXTENSION);
        }
    }
}
