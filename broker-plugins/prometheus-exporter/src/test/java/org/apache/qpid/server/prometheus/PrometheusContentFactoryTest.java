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

import static org.apache.qpid.server.prometheus.PrometheusContentFactory.INCLUDE_DISABLED;
import static org.apache.qpid.server.prometheus.PrometheusContentFactory.INCLUDE_METRIC;
import static org.apache.qpid.server.prometheus.PrometheusContentFactory.INCLUDE_DISABLED_CONTEXT_VARIABLE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.testmodels.hierarchy.TestCar;
import org.apache.qpid.server.model.testmodels.hierarchy.TestKitCarImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestModel;

public class PrometheusContentFactoryTest
{
    public static final String QPID_TEST_CAR_MILEAGE_COUNT = "qpid_test_car_mileage_count";
    public static final String QPID_TEST_CAR_AGE = "qpid_test_car_age";
    public static final String PROMETHEUS_COMMENT = "#";
    private ConfiguredObject _root;
    private PrometheusContentFactory _prometheusContentFactory;

    @Before
    public void setUp()
    {
        final Model model = TestModel.getInstance();
        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, "MyPrometheusCar");
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        @SuppressWarnings("unchecked") final TestCar<?> car =
                model.getObjectFactory().create(TestCar.class, carAttributes, null);
        _root = car;
        _prometheusContentFactory = new PrometheusContentFactory();
    }

    @Test
    public void testCreateContent() throws Exception
    {
        final Content content = _prometheusContentFactory.createContent(_root, Collections.emptyMap());
        assertThat(content, is(notNullValue()));
        Collection<String> metrics;
        try (final ByteArrayOutputStream output = new ByteArrayOutputStream())
        {
            content.write(output);
            metrics = getMetricLines(output.toByteArray());
        }
        assertThat(metrics, is(notNullValue()));

        assertThat(metrics.size(), is(equalTo(1)));
        String metric = metrics.iterator().next();
        assertThat(metric, startsWith(QPID_TEST_CAR_MILEAGE_COUNT));
    }

    @Test
    public void testCreateContentIncludeDisabled() throws Exception
    {
        final Content content = _prometheusContentFactory.createContent(_root, Collections.singletonMap(INCLUDE_DISABLED, new String[]{"true"}));
        assertThat(content, is(notNullValue()));
        Collection<String> metrics;
        try (final ByteArrayOutputStream output = new ByteArrayOutputStream())
        {
            content.write(output);
            metrics = getMetricLines(output.toByteArray());
        }
        assertThat(metrics, is(notNullValue()));

        assertThat(metrics.size(), is(equalTo(2)));
        Map<String, String> metricsMap = convertMetricsToMap(metrics);
        assertThat(metricsMap.containsKey(QPID_TEST_CAR_MILEAGE_COUNT), equalTo(Boolean.TRUE));
        assertThat(metricsMap.containsKey(QPID_TEST_CAR_AGE), equalTo(Boolean.TRUE));
    }

    @Test
    public void testCreateContentIncludeDisabledUsingContextVariable() throws Exception
    {
        _root.setContextVariable(INCLUDE_DISABLED_CONTEXT_VARIABLE, "true");
        final Content content = _prometheusContentFactory.createContent(_root, Collections.emptyMap());
        assertThat(content, is(notNullValue()));
        Collection<String> metrics;
        try (final ByteArrayOutputStream output = new ByteArrayOutputStream())
        {
            content.write(output);
            metrics = getMetricLines(output.toByteArray());
        }
        assertThat(metrics, is(notNullValue()));
        assertThat(metrics.size(), is(equalTo(2)));
        Map<String, String> metricsMap = convertMetricsToMap(metrics);
        assertThat(metricsMap.containsKey(QPID_TEST_CAR_MILEAGE_COUNT), equalTo(Boolean.TRUE));
        assertThat(metricsMap.containsKey(QPID_TEST_CAR_AGE), equalTo(Boolean.TRUE));
    }

    @Test
    public void testCreateContentIncludeName() throws Exception
    {
        final Map<String, String[]> filter = new HashMap<>();
        filter.put(INCLUDE_DISABLED, new String[]{"true"});
        filter.put(INCLUDE_METRIC, new String[]{QPID_TEST_CAR_AGE});
        final Content content = _prometheusContentFactory.createContent(_root, filter);
        assertThat(content, is(notNullValue()));
        Collection<String> metrics;
        try (final ByteArrayOutputStream output = new ByteArrayOutputStream())
        {
            content.write(output);
            metrics = getMetricLines(output.toByteArray());
        }
        assertThat(metrics, is(notNullValue()));

        assertThat(metrics.size(), is(equalTo(1)));
        String metric = metrics.iterator().next();
        assertThat(metric, startsWith(QPID_TEST_CAR_AGE));
    }

    private static Collection<String> getMetricLines(final byte[] metricsBytes) throws IOException
    {
        final List<String> results = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metricsBytes))))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (!(line.startsWith(PROMETHEUS_COMMENT) || line.isEmpty()))
                {
                    results.add(line);
                }
            }
        }
        return results;
    }

    private Map<String, String> convertMetricsToMap(final Collection<String> metrics)
    {
        return metrics.stream().map(m -> m.split(" ")).collect(Collectors.toMap(m -> m[0], m -> m[1]));
    }

}
