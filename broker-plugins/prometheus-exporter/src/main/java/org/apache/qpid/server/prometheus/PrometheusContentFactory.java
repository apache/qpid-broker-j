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

package org.apache.qpid.server.prometheus;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import io.prometheus.client.exporter.common.TextFormat;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.plugin.ContentFactory;
import org.apache.qpid.server.plugin.PluggableService;

@PluggableService
public class PrometheusContentFactory implements ContentFactory
{
    static final String INCLUDE_DISABLED = "includeDisabled";
    static final String INCLUDE_METRIC = "name[]";
    static final String INCLUDE_DISABLED_CONTEXT_VARIABLE = "qpid.metrics.includeDisabled";

    @Override
    public Content createContent(final ConfiguredObject<?> object, final Map<String, String[]> filter)
    {
        final String[] includeDisabledValues = filter.get(INCLUDE_DISABLED);
        boolean includeDisabled = includeDisabledValues!= null && includeDisabledValues.length == 1 && Boolean.parseBoolean(includeDisabledValues[0]);
        if (!includeDisabled)
        {
            Boolean val = object.getContextValue(Boolean.class, INCLUDE_DISABLED_CONTEXT_VARIABLE);
            if (val != null)
            {
                includeDisabled = val;
            }
        }

        final String[] includedMetricNames = filter.get(INCLUDE_METRIC);

        final IncludeMetricPredicate metricIncludeFilter =
                new IncludeMetricPredicate(includedMetricNames == null || includedMetricNames.length == 0
                                                   ? Collections.emptySet()
                                                   : new HashSet<>(Arrays.asList(includedMetricNames)));
        final QpidCollector qpidCollector = new QpidCollector(object,
                                                              new IncludeDisabledStatisticPredicate(includeDisabled),
                                                              metricIncludeFilter);

        return new Content()
        {
            @Override
            public void write(final OutputStream outputStream) throws IOException
            {
                try (final Writer writer = new OutputStreamWriter(outputStream))
                {
                    TextFormat.write004(writer, Collections.enumeration(qpidCollector.collect()));
                    writer.flush();
                }
            }

            @Override
            public void release()
            {

            }

            @SuppressWarnings("unused")
            @RestContentHeader("Content-Type")
            public String getContentType()
            {
                return TextFormat.CONTENT_TYPE_004;
            }

        };

    }

    @Override
    public String getType()
    {
        return "metrics";
    }
}
