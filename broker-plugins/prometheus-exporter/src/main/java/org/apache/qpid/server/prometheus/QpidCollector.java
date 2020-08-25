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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectStatistic;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;

public class QpidCollector extends Collector
{
    private final static MetricFamilySamples IGNORED = new MetricFamilySamples(null, null, null, null);
    static final String COUNT_SUFFIX = "count";
    static final String TOTAL_SUFFIX = "total";
    private final Predicate<ConfiguredObjectStatistic<?,?>> _includeStatisticFilter;
    private final Predicate<String> _includeMetricFilter;
    private ConfiguredObject<?> _root;
    private Model _model;


    QpidCollector(final ConfiguredObject<?> root,
                  final Predicate<ConfiguredObjectStatistic<?,?>> includeStatisticFilter,
                  final Predicate<String> includeMetricFilter)
    {
        _root = root;
        _model = _root.getModel();
        _includeStatisticFilter = includeStatisticFilter;
        _includeMetricFilter = includeMetricFilter;
    }

    @Override
    public List<MetricFamilySamples> collect()
    {
        final List<MetricFamilySamples> metricFamilySamples = new ArrayList<>();
        addObjectMetrics(_root, Collections.emptyList(), new HashMap<>(), metricFamilySamples);
        addChildrenMetrics(metricFamilySamples, _root, Collections.singletonList("name"));
        return metricFamilySamples;
    }

    private void addObjectMetrics(final ConfiguredObject<?> object,
                                  final List<String> labelNames,
                                  final Map<String, MetricFamilySamples> metricFamilyMap,
                                  final List<MetricFamilySamples> metricFamilySamples)
    {
        final Map<String, Object> statsMap = object.getStatistics();
        for (final Map.Entry<String, Object> entry : statsMap.entrySet())
        {
            MetricFamilySamples family = metricFamilyMap.get(entry.getKey());
            if (family == null)
            {
                family = createMetricFamilySamples(entry.getKey(), object, labelNames);
                metricFamilyMap.put(entry.getKey(), family);
                if (family != IGNORED)
                {
                    metricFamilySamples.add(family);
                }
            }
            if (family != IGNORED)
            {
                final List<String> labelsValues = buildLabelValues(object);
                final double doubleValue = toDoubleValue(entry.getValue());
                family.samples.add(new MetricFamilySamples.Sample(family.name, labelNames, labelsValues, doubleValue));
            }
        }
    }

    private MetricFamilySamples createMetricFamilySamples(final String statisticName,
                                                          final ConfiguredObject<?> object,
                                                          final List<String> labelNames)
    {
        final ConfiguredObjectStatistic<?, ?> configuredObjectStatistic =
                findConfiguredObjectStatistic(statisticName, object.getTypeClass());
        if (configuredObjectStatistic == null || !_includeStatisticFilter.test(configuredObjectStatistic))
        {
            return IGNORED;
        }
        final StatisticType type = configuredObjectStatistic.getStatisticType();
        final String familyName = getFamilyName(object.getCategoryClass(), configuredObjectStatistic);

        if (!_includeMetricFilter.test(familyName))
        {
            return IGNORED;
        }

        if (type == StatisticType.CUMULATIVE)
        {
            return new CounterMetricFamily(familyName, configuredObjectStatistic.getDescription(), labelNames);
        }
        else
        {
            return new GaugeMetricFamily(familyName, configuredObjectStatistic.getDescription(), labelNames);
        }
    }

    private ConfiguredObjectStatistic<?, ?> findConfiguredObjectStatistic(final String statisticName,
                                                                          final Class<? extends ConfiguredObject> typeClass)
    {
        final Collection<ConfiguredObjectStatistic<?, ?>> statisticsDefinitions =
                _model.getTypeRegistry().getStatistics(typeClass);
        return statisticsDefinitions.stream()
                                    .filter(s -> statisticName.equals(s.getName()))
                                    .findFirst()
                                    .orElse(null);
    }

    private List<String> buildLabelValues(final ConfiguredObject<?> object)
    {
        final List<String> labelsValues = new ArrayList<>();
        ConfiguredObject o = object;
        while (o != null && o != _root)
        {
            labelsValues.add(o.getName());
            o = o.getParent();
        }
        return labelsValues;
    }

    private void addChildrenMetrics(final List<MetricFamilySamples> metricFamilySamples,
                                    final ConfiguredObject<?> object,
                                    final List<String> childLabelNames)
    {
        final Class<? extends ConfiguredObject> category = object.getCategoryClass();
        for (final Class<? extends ConfiguredObject> childClass : _model.getChildTypes(category))
        {
            final Collection<? extends ConfiguredObject> children = object.getChildren(childClass);
            if (children != null && !children.isEmpty())
            {
                final Map<String, MetricFamilySamples> childrenMetricFamilyMap = new HashMap<>();
                for (final ConfiguredObject<?> child : children)
                {
                    addObjectMetrics(child, childLabelNames, childrenMetricFamilyMap, metricFamilySamples);
                    final List<String> labelNames = new ArrayList<>(childLabelNames);
                    final String label = String.format("%s_name", toSnakeCase(childClass.getSimpleName()));
                    labelNames.add(label);
                    addChildrenMetrics(metricFamilySamples, child, labelNames);
                }
            }
        }
    }

    static String toSnakeCase(final String simpleName)
    {
        final StringBuilder sb = new StringBuilder();
        final char[] chars = simpleName.toCharArray();
        for (int i = 0; i < chars.length; i++)
        {
            final char ch = chars[i];
            if (Character.isUpperCase(ch))
            {
                if (i > 0)
                {
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(ch));
            }
            else
            {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    private double toDoubleValue(final Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).doubleValue();
        }
        return 0;
    }

    static String getFamilyName(final Class<? extends ConfiguredObject> categoryClass,
                                ConfiguredObjectStatistic<?, ?> statistics)
    {
        String metricName = statistics.getMetricName();
        if (metricName == null || metricName.isEmpty())
        {
            metricName = generateMetricName(statistics);
        }

        return String.format("qpid_%s_%s",
                             toSnakeCase(categoryClass.getSimpleName()),
                             metricName);
    }

    private static String generateMetricName(final ConfiguredObjectStatistic<?, ?> statistics)
    {
        String metricName = toSnakeCase(statistics.getName());
        String suffix;
        switch (statistics.getStatisticType())
        {
            case CUMULATIVE:
                suffix = generateMetricSuffix(statistics, COUNT_SUFFIX, metricName);
                break;
            case POINT_IN_TIME:
                suffix = generateMetricSuffix(statistics, TOTAL_SUFFIX, metricName);
                break;
            default:
                suffix = "";
        }

        return metricName + suffix;
    }

    private static String generateMetricSuffix(final ConfiguredObjectStatistic<?, ?> statistics,
                                               final String typeSuffix,
                                               final String metricName)
    {
        String suffix = "";
        if (!statistics.getName().toLowerCase().contains(typeSuffix)
            && statistics.getUnits() != StatisticUnit.ABSOLUTE_TIME
            && statistics.getUnits() != StatisticUnit.TIME_DURATION)
        {
            if (statistics.getUnits() == StatisticUnit.MESSAGES || statistics.getUnits() == StatisticUnit.BYTES)
            {
                final String units = statistics.getUnits().toString() + "s";
                if (!metricName.contains(units))
                {
                    suffix = "_" + units;
                }
            }
            suffix = suffix + "_" + typeSuffix;
        }
        return suffix;
    }
}
