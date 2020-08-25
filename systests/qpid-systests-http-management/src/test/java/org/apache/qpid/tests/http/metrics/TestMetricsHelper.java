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

package org.apache.qpid.tests.http.metrics;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

class TestMetricsHelper
{
    static final String QUEUE_NAME = "foo";

    static void assertMetricsInclusion(final String metricsString,
                                       final String[] metricNames,
                                       final boolean inclusionFlag)
    {
        for (String expected : metricNames)
        {
            assertThat(metricsString.contains(expected), equalTo(inclusionFlag));
        }
    }

    static void assertMetricsInclusion(final String metricsString,
                                       final Pattern[] expectedMetricPattens,
                                       final boolean inclusionFlag)
    {
        for (Pattern expected : expectedMetricPattens)
        {
            assertThat(expected.matcher(metricsString).find(), equalTo(inclusionFlag));
        }
    }

    static Pattern createQueueMetricPattern(final String metricName)
    {
        return Pattern.compile(String.format("%s\\s*\\{.*name\\s*=\\s*\"%s\"\\s*,.*\\}\\s*0\\.0",
                                             metricName,
                                             QUEUE_NAME));
    }

    static void assertVirtualHostHierarchyMetrics(final byte[] metricsBytes) throws IOException
    {
        final Predicate<String> unexpectedMetricPredicate = line -> !(line.startsWith("qpid_virtual_host")
                                                                      || line.startsWith("qpid_queue")
                                                                      || line.startsWith("qpid_exchange"));
        getMetricLines(metricsBytes).stream().filter(unexpectedMetricPredicate)
                                    .findFirst().ifPresent(found -> fail("Unexpected metric: " + found));
    }

    static Collection<String> getMetricLines(final byte[] metricsBytes) throws IOException
    {
        final List<String> results = new ArrayList<>();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metricsBytes))))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (!(line.startsWith("#") || line.isEmpty()))
                {
                    results.add(line);
                }
            }
        }
        return results;
    }
}
