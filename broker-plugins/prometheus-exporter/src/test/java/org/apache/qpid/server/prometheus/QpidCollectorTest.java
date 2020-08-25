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


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.prometheus.client.Collector;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectStatistic;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;
import org.apache.qpid.server.model.testmodels.hierarchy.TestAbstractEngineImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestCar;
import org.apache.qpid.server.model.testmodels.hierarchy.TestDigitalInstrumentPanelImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestElecEngineImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestEngine;
import org.apache.qpid.server.model.testmodels.hierarchy.TestInstrumentPanel;
import org.apache.qpid.server.model.testmodels.hierarchy.TestKitCarImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestModel;
import org.apache.qpid.server.model.testmodels.hierarchy.TestPetrolEngineImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestSensor;
import org.apache.qpid.server.model.testmodels.hierarchy.TestTemperatureSensorImpl;
import org.apache.qpid.test.utils.UnitTestBase;

public class QpidCollectorTest extends UnitTestBase
{
    private static final String CAR_NAME = "myCar";
    private static final String ELECTRIC_ENGINE_NAME = "myEngine";
    private static final String INSTRUMENT_PANEL_NAME = "instrumentPanel";
    private static final String PETROL_ENGINE_NAME = "myPetrolModel";
    private static final String SENSOR = "sensor";
    private static final int DESIRED_MILEAGE = 100;
    private static final String QPID_TEST_CAR_MILEAGE_COUNT = "qpid_test_car_mileage_count";
    private static final String QPID_TEST_ENGINE_TEMPERATURE_TOTAL = "qpid_test_engine_temperature_total";
    private static final String QPID_TEST_SENSOR_ALERT_COUNT = "qpid_test_sensor_alert_count";
    private static final String QPID_TEST_CAR_AGE_COUNT = "qpid_test_car_age";

    private TestCar<?> _root;
    private QpidCollector _qpidCollector;
    private static final StatisticUnit[] UNITS = new StatisticUnit[]{
            StatisticUnit.BYTES,
            StatisticUnit.MESSAGES,
            StatisticUnit.COUNT,
            StatisticUnit.ABSOLUTE_TIME,
            StatisticUnit.TIME_DURATION};
    private static final String[] UNIT_SUFFIXES = new String[]{"_bytes", "_messages", "", "", ""};

    @Before
    public void setUp()
    {
        final Model model = TestModel.getInstance();
        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, CAR_NAME);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        @SuppressWarnings("unchecked") final TestCar<?> car =
                model.getObjectFactory().create(TestCar.class, carAttributes, null);
        _root = car;
        _qpidCollector = new QpidCollector(_root, new IncludeDisabledStatisticPredicate(false), s->true);
    }

    @Test
    public void testCollectForHierarchyOfTwoObjects()
    {
        createTestEngine(ELECTRIC_ENGINE_NAME, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        _root.move(DESIRED_MILEAGE);

        final List<Collector.MetricFamilySamples> metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames = {QPID_TEST_CAR_MILEAGE_COUNT, QPID_TEST_ENGINE_TEMPERATURE_TOTAL};
        final Map<String, Collector.MetricFamilySamples> metricsMap =
                convertMetricFamilySamplesIntoMapAndAssert(metrics, expectedFamilyNames);

        final Collector.MetricFamilySamples carMetricFamilySamples = metricsMap.get(QPID_TEST_CAR_MILEAGE_COUNT);
        assertMetricFamilySamplesSize(carMetricFamilySamples, 1);
        final Collector.MetricFamilySamples.Sample carSample = carMetricFamilySamples.samples.get(0);
        assertThat(carSample.value, closeTo(DESIRED_MILEAGE, 0.01));
        assertThat(carSample.labelNames.size(), is(equalTo(0)));

        final Collector.MetricFamilySamples engineMetricFamilySamples = metricsMap.get(
                QPID_TEST_ENGINE_TEMPERATURE_TOTAL);
        assertMetricFamilySamplesSize(engineMetricFamilySamples, 1);
        final Collector.MetricFamilySamples.Sample engineSample = engineMetricFamilySamples.samples.get(0);
        assertThat(engineSample.labelNames, is(equalTo(Collections.singletonList("name"))));
        assertThat(engineSample.labelValues, is(equalTo(Collections.singletonList(ELECTRIC_ENGINE_NAME))));
        assertThat(engineSample.value, Matchers.closeTo(TestAbstractEngineImpl.TEST_TEMPERATURE, 0.01));
    }

    @Test
    public void testCollectForHierarchyOfThreeObjects()
    {
        final TestInstrumentPanel instrumentPanel = getTestInstrumentPanel();
        createTestSensor(instrumentPanel);

        final List<Collector.MetricFamilySamples> metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames =
                {QPID_TEST_CAR_MILEAGE_COUNT, QPID_TEST_SENSOR_ALERT_COUNT};
        final Map<String, Collector.MetricFamilySamples> metricsMap =
                convertMetricFamilySamplesIntoMapAndAssert(metrics, expectedFamilyNames);

        final Collector.MetricFamilySamples carMetricFamilySamples = metricsMap.get(QPID_TEST_CAR_MILEAGE_COUNT);
        assertMetricFamilySamplesSize(carMetricFamilySamples, 1);
        final Collector.MetricFamilySamples.Sample carSample = carMetricFamilySamples.samples.get(0);
        assertThat(carSample.labelNames.size(), is(equalTo(0)));
        assertThat(carSample.labelValues.size(), is(equalTo(0)));
        assertThat(carSample.value, closeTo(0, 0.01));

        final Collector.MetricFamilySamples sensorlMetricFamilySamples =
                metricsMap.get(QPID_TEST_SENSOR_ALERT_COUNT);
        assertMetricFamilySamplesSize(sensorlMetricFamilySamples, 1);
        final Collector.MetricFamilySamples.Sample sensorSample = sensorlMetricFamilySamples.samples.get(0);
        assertThat(sensorSample.labelNames, is(equalTo(Arrays.asList("name", "test_instrument_panel_name"))));
        assertThat(sensorSample.labelValues, is(equalTo(Arrays.asList(SENSOR, INSTRUMENT_PANEL_NAME))));
    }

    @Test
    public void testCollectForSiblingObjects()
    {
        createTestEngine(ELECTRIC_ENGINE_NAME, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        createTestEngine(PETROL_ENGINE_NAME, TestPetrolEngineImpl.TEST_PETROL_ENGINE_TYPE);

        final List<Collector.MetricFamilySamples> metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames = {QPID_TEST_CAR_MILEAGE_COUNT, QPID_TEST_ENGINE_TEMPERATURE_TOTAL};
        final Map<String, Collector.MetricFamilySamples> metricsMap =
                convertMetricFamilySamplesIntoMapAndAssert(metrics, expectedFamilyNames);

        final Collector.MetricFamilySamples carMetricFamilySamples = metricsMap.get(QPID_TEST_CAR_MILEAGE_COUNT);
        assertMetricFamilySamplesSize(carMetricFamilySamples, 1);
        final Collector.MetricFamilySamples.Sample carSample = carMetricFamilySamples.samples.get(0);
        assertThat(carSample.labelNames.size(), is(equalTo(0)));
        assertThat(carSample.labelValues.size(), is(equalTo(0)));

        final Collector.MetricFamilySamples engineMetricFamilySamples = metricsMap.get(
                QPID_TEST_ENGINE_TEMPERATURE_TOTAL);
        assertMetricFamilySamplesSize(engineMetricFamilySamples, 2);
        final String[] engineNames = {PETROL_ENGINE_NAME, ELECTRIC_ENGINE_NAME};
        for (String engineName : engineNames)
        {
            final Collector.MetricFamilySamples.Sample sample =
                    findSampleByLabelValue(engineMetricFamilySamples, engineName);
            assertThat(sample.labelNames, is(equalTo(Collections.singletonList("name"))));
            assertThat(sample.labelValues, is(equalTo(Collections.singletonList(engineName))));
            assertThat(sample.value, Matchers.closeTo(TestAbstractEngineImpl.TEST_TEMPERATURE, 0.01));
        }
    }

    @Test
    public void testCollectWithFilter(){
        createTestEngine(ELECTRIC_ENGINE_NAME, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        _root.move(DESIRED_MILEAGE);

        _qpidCollector = new QpidCollector(_root,
                                           new IncludeDisabledStatisticPredicate(true),
                                           new IncludeMetricPredicate(Collections.singleton(QPID_TEST_CAR_AGE_COUNT)));
        final List<Collector.MetricFamilySamples> metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames = {QPID_TEST_CAR_AGE_COUNT};
        final Map<String, Collector.MetricFamilySamples> metricsMap =
                convertMetricFamilySamplesIntoMapAndAssert(metrics, expectedFamilyNames);


        final Collector.MetricFamilySamples engineMetricFamilySamples = metricsMap.get(QPID_TEST_CAR_AGE_COUNT);
        assertMetricFamilySamplesSize(engineMetricFamilySamples, 1);
        final Collector.MetricFamilySamples.Sample engineSample = engineMetricFamilySamples.samples.get(0);
        assertThat(engineSample.labelNames, is(equalTo(Collections.emptyList())));
        assertThat(engineSample.labelValues, is(equalTo(Collections.emptyList())));
        assertThat(engineSample.value, Matchers.closeTo(0.0, 0.01));

    }


    private Collector.MetricFamilySamples.Sample findSampleByLabelValue(final Collector.MetricFamilySamples metricFamilySamples,
                                                                        final String nameLabelValue)
    {
        final List<Collector.MetricFamilySamples.Sample> found = metricFamilySamples.samples
                .stream()
                .filter(s -> s.labelValues != null
                             && s.labelValues.size() > 0
                             && nameLabelValue.equals(s.labelValues.get(0)))
                .collect(Collectors.toList());
        assertThat(found.size(), is(equalTo(1)));
        return found.get(0);
    }

    private void createTestEngine(final String engineName, final String engineType)
    {
        final Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);
        engineAttributes.put(ConfiguredObject.TYPE, engineType);
        _root.createChild(TestEngine.class, engineAttributes);
    }

    private void createTestSensor(final TestInstrumentPanel instrumentPanel)
    {
        final Map<String, Object> sensorAttributes = new HashMap<>();
        sensorAttributes.put(ConfiguredObject.NAME, SENSOR);
        sensorAttributes.put(ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        instrumentPanel.createChild(TestSensor.class, sensorAttributes);
    }

    private TestInstrumentPanel getTestInstrumentPanel()
    {
        final Map<String, Object> instrumentPanelAttributes = new HashMap<>();
        instrumentPanelAttributes.put(ConfiguredObject.NAME, INSTRUMENT_PANEL_NAME);
        instrumentPanelAttributes.put(ConfiguredObject.TYPE,
                                      TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        return _root.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);
    }

    private Map<String, Collector.MetricFamilySamples> convertMetricFamilySamplesIntoMap(List<Collector.MetricFamilySamples> metricFamilySamples)
    {
        Map<String, Collector.MetricFamilySamples> result = new HashMap<>();
        for (Collector.MetricFamilySamples metricFamilySample : metricFamilySamples)
        {
            final String name = metricFamilySample.name;

            if (result.put(name, metricFamilySample) != null)
            {
                fail(String.format("Duplicate family name : %s", name));
            }
        }
        return result;
    }

    private void assertMetricFamilySamples(final Collector.MetricFamilySamples metricFamilySamples)
    {
        assertThat(metricFamilySamples, is(notNullValue()));
        assertThat(metricFamilySamples.samples, is(notNullValue()));

        for (final Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples)
        {
            assertThat(sample, is(notNullValue()));
            assertThat(sample.name, is(equalTo(metricFamilySamples.name)));
        }
    }


    private void assertMetricFamilySamplesSize(final Collector.MetricFamilySamples metricFamilySamples,
                                               final int expectedSamplesSize)
    {
        assertThat(metricFamilySamples.samples.size(), equalTo(expectedSamplesSize));
    }

    private Map<String, Collector.MetricFamilySamples> convertMetricFamilySamplesIntoMapAndAssert(final List<Collector.MetricFamilySamples> metrics,
                                                                                                  final String[] expectedFamilyNames)
    {
        assertThat(metrics.size(), equalTo(expectedFamilyNames.length));
        final Map<String, Collector.MetricFamilySamples> metricsMap = convertMetricFamilySamplesIntoMap(metrics);

        for (String expectedFamily : expectedFamilyNames)
        {
            assertMetricFamilySamples(metricsMap.get(expectedFamily));
        }
        return metricsMap;
    }

    @Test
    public void testToSnakeCase()
    {
        assertThat(QpidCollector.toSnakeCase("carEngineOilChanges"), is(equalTo("car_engine_oil_changes")));
    }

    @Test
    public void getFamilyNameForCumulativeStatistic()
    {
        for (int i = 0; i < UNITS.length; i++)
        {
            final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
            when(statistics.getUnits()).thenReturn(UNITS[i]);
            when(statistics.getStatisticType()).thenReturn(StatisticType.CUMULATIVE);
            when(statistics.getName()).thenReturn("diagnosticData");
            final String familyName = QpidCollector.getFamilyName(TestCar.class, statistics);
            final String expectedName =
                    String.format("qpid_test_car_diagnostic_data%s%s", UNIT_SUFFIXES[i], getSuffix(UNITS[i],QpidCollector.COUNT_SUFFIX));
            assertThat(String.format("unexpected metric name for units %s", UNITS[i]),
                       familyName,
                       is(equalTo(expectedName)));
        }
    }

    @Test
    public void getFamilyNameForCumulativeStatisticContainingCountInName()
    {
        final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
        when(statistics.getUnits()).thenReturn(StatisticUnit.BYTES);
        when(statistics.getStatisticType()).thenReturn(StatisticType.CUMULATIVE);
        when(statistics.getName()).thenReturn("CountOfDiagnosticData");
        final String familyName = QpidCollector.getFamilyName(TestCar.class, statistics);
        assertThat(familyName, is(equalTo("qpid_test_car_count_of_diagnostic_data")));
    }

    @Test
    public void getFamilyNameForPointInTimeStatistic()
    {
        for (int i = 0; i < UNITS.length; i++)
        {
            final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
            when(statistics.getUnits()).thenReturn(UNITS[i]);
            when(statistics.getStatisticType()).thenReturn(StatisticType.POINT_IN_TIME);
            when(statistics.getName()).thenReturn("diagnosticData");
            final String familyName = QpidCollector.getFamilyName(TestCar.class, statistics);

            final String expectedName =
                    String.format("qpid_test_car_diagnostic_data%s%s", UNIT_SUFFIXES[i],  getSuffix(UNITS[i],QpidCollector.TOTAL_SUFFIX));
            assertThat(String.format("unexpected metric name for units %s", UNITS[i]),
                       familyName,
                       is(equalTo(expectedName)));
        }
    }

    String getSuffix(final StatisticUnit unit,final String requiredSuffix)
    {
        String suffix = "_" + requiredSuffix;
        if(unit.equals(StatisticUnit.ABSOLUTE_TIME) || unit.equals(StatisticUnit.TIME_DURATION)){
            suffix = "";
        }
        return suffix;
    }
}
