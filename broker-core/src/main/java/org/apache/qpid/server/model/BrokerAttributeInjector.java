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
package org.apache.qpid.server.model;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.plugin.ConfiguredObjectAttributeInjector;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.util.ParameterizedTypes;

@PluggableService
public class BrokerAttributeInjector implements ConfiguredObjectAttributeInjector
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerAttributeInjector.class);

    private final InjectedAttributeOrStatistic.TypeValidator _typeValidator = Broker.class::isAssignableFrom;

    private final Class<?> _hotSpotDiagnosticMXBeanClass;
    private final PlatformManagedObject _hotSpotDiagnosticMXBean;
    private final Class<?> _operatingSystemMXBeanClass;
    private final OperatingSystemMXBean _operatingSystemMXBean;

    public BrokerAttributeInjector()
    {
        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        Class<?> hotSpotDiagnosticMXBeanClass = null;
        PlatformManagedObject hotSpotDiagnosticMXBean = null;
        try
        {
            hotSpotDiagnosticMXBeanClass =
                    Class.forName("com.sun.management.HotSpotDiagnosticMXBean", true, systemClassLoader);
            hotSpotDiagnosticMXBean =
                    ManagementFactory.getPlatformMXBean((Class<? extends PlatformManagedObject>) hotSpotDiagnosticMXBeanClass);

        }
        catch (IllegalArgumentException | ClassNotFoundException e)
        {
            LOGGER.debug("Cannot find com.sun.management.HotSpotDiagnosticMXBean MXBean: " + e);
        }

        _hotSpotDiagnosticMXBeanClass = hotSpotDiagnosticMXBeanClass;
        _hotSpotDiagnosticMXBean = hotSpotDiagnosticMXBean;


        Class<?> operatingSystemMXBeanClass = null;
        PlatformManagedObject operatingSystemMXBean = null;
        try
        {
            operatingSystemMXBeanClass =
                    Class.forName("com.sun.management.OperatingSystemMXBean", true, systemClassLoader);
            operatingSystemMXBean =
                    ManagementFactory.getPlatformMXBean((Class<? extends PlatformManagedObject>) operatingSystemMXBeanClass);

        }
        catch (IllegalArgumentException | ClassNotFoundException e)
        {
            LOGGER.debug("com.sun.management.OperatingSystemMXBean MXBean: " + e);
        }

        _operatingSystemMXBeanClass = operatingSystemMXBeanClass;
        _operatingSystemMXBean = (OperatingSystemMXBean) operatingSystemMXBean;

    }

    @Override
    public InjectedAttributeStatisticOrOperation.TypeValidator getTypeValidator()
    {
        return _typeValidator;
    }

    @Override
    public Collection<ConfiguredObjectInjectedAttribute<?, ?>> getInjectedAttributes()
    {
        List<ConfiguredObjectInjectedAttribute<?, ?>> attributes = new ArrayList<>();
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeans)
        {

            String poolName = memoryPoolMXBean.getName().replace(" ", "");
            String attributeName = "jvmMemoryMaximum" + poolName;
            try
            {
                Method getMemoryPoolMaximum = BrokerAttributeInjector.class.getDeclaredMethod("getMemoryPoolMaximum",
                                                                                              Broker.class,
                                                                                              MemoryPoolMXBean.class);

                final ConfiguredObjectInjectedAttribute<?, ?> injectedStatistic =
                        new ConfiguredDerivedInjectedAttribute<>(attributeName,
                                                                 getMemoryPoolMaximum,
                                                                 new Object[]{memoryPoolMXBean},
                                                                 false,
                                                                 false,
                                                                 "",
                                                                 false,
                                                                 "",
                                                                 "Maximum size of memory pool " + memoryPoolMXBean.getName(),
                                                                 _typeValidator);
                attributes.add(injectedStatistic);
            }
            catch (NoSuchMethodException e)
            {
                LOGGER.warn("Failed to inject attribute '{}'", attributeName, e);
            }
        }
        return attributes;
    }

    @Override
    public Collection<ConfiguredObjectInjectedStatistic<?, ?>> getInjectedStatistics()
    {
        List<ConfiguredObjectInjectedStatistic<?, ?>> statistics = new ArrayList<>();
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeans)
        {
            String poolName = memoryPoolMXBean.getName().replace(" ", "");
            String statisticName = "jvmMemoryUsed" + poolName;
            try
            {
                Method getMemoryPoolUsed = BrokerAttributeInjector.class.getDeclaredMethod("getMemoryPoolUsed",
                                                                                           Broker.class,
                                                                                           MemoryPoolMXBean.class);

                final ConfiguredObjectInjectedStatistic<?, ?> injectedStatistic =
                        new ConfiguredObjectInjectedStatistic<>(statisticName,
                                                                getMemoryPoolUsed,
                                                                new Object[]{memoryPoolMXBean},
                                                                "Usage of memory in pool " + memoryPoolMXBean.getName(),
                                                                _typeValidator,
                                                                StatisticUnit.BYTES,
                                                                StatisticType.POINT_IN_TIME,
                                                                memoryPoolMXBean.getName() + " Memory Used");
                statistics.add(injectedStatistic);
            }
            catch (NoSuchMethodException e)
            {
                LOGGER.warn("Failed to inject statistic '{}'", statisticName, e);
            }
        }

        for (GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans())
        {
            String gcName = garbageCollectorMXBean.getName().replace(" ", "");
            String jvmGCCollectionTimeStatisticName = "jvmGCCollectionTime" + gcName;
            try
            {
                Method getGCCollectionTime = BrokerAttributeInjector.class.getDeclaredMethod("getGCCollectionTime",
                                                                                             Broker.class,
                                                                                             GarbageCollectorMXBean.class);
                final ConfiguredObjectInjectedStatistic<?, ?> injectedStatistic =
                        new ConfiguredObjectInjectedStatistic<>(jvmGCCollectionTimeStatisticName,
                                                                getGCCollectionTime,
                                                                new Object[]{garbageCollectorMXBean},
                                                                "Cumulative time in ms taken to perform collections for GC "
                                                                + garbageCollectorMXBean.getName(),
                                                                _typeValidator,
                                                                StatisticUnit.COUNT,
                                                                StatisticType.CUMULATIVE,
                                                                garbageCollectorMXBean.getName()
                                                                + " GC Collection Time");
                statistics.add(injectedStatistic);
            }
            catch (NoSuchMethodException e)
            {
                LOGGER.warn("Failed to inject statistic '{}'", jvmGCCollectionTimeStatisticName, e);
            }
            String jvmGCCollectionCountStatisticName = "jvmGCCollectionCount" + gcName;
            try
            {
                Method getGCCollectionCount = BrokerAttributeInjector.class.getDeclaredMethod("getGCCollectionCount",
                                                                                              Broker.class,
                                                                                              GarbageCollectorMXBean.class);
                final ConfiguredObjectInjectedStatistic<?, ?> injectedStatistic =
                        new ConfiguredObjectInjectedStatistic<>(jvmGCCollectionCountStatisticName,
                                                                getGCCollectionCount,
                                                                new Object[]{garbageCollectorMXBean},
                                                                "Cumulative number of collections for GC "
                                                                + garbageCollectorMXBean.getName(),
                                                                _typeValidator,
                                                                StatisticUnit.COUNT,
                                                                StatisticType.CUMULATIVE,
                                                                garbageCollectorMXBean.getName()
                                                                + " GC Collection Count");
                statistics.add(injectedStatistic);
            }
            catch (NoSuchMethodException e)
            {
                LOGGER.warn("Failed to inject statistic '{}'", jvmGCCollectionCountStatisticName, e);
            }
        }

        if (_operatingSystemMXBean != null)
        {
            try
            {
                Method method = _operatingSystemMXBeanClass.getDeclaredMethod("getProcessCpuTime");

                ToLongFunction<Broker> supplier = broker -> {
                    try
                    {
                        final Object returnValue = method.invoke(_operatingSystemMXBean);

                        if (returnValue instanceof Number)
                        {
                            return ((Number) returnValue).longValue();
                        }
                    }
                    catch (IllegalAccessException | InvocationTargetException e)
                    {
                        LOGGER.warn("Unable to get cumulative process CPU time");
                    }
                    return -1L;
                };

                Method getLongValue = BrokerAttributeInjector.class.getDeclaredMethod("getLongValue",
                                                                                      Broker.class,
                                                                                      ToLongFunction.class);

                final ConfiguredObjectInjectedStatistic<?, ?> injectedStatistic =
                        new ConfiguredObjectInjectedStatistic<>("processCpuTime",
                                                                getLongValue,
                                                                new Object[]{supplier},
                                                                "Cumulative process CPU time",
                                                                _typeValidator,
                                                                StatisticUnit.TIME_DURATION,
                                                                StatisticType.CUMULATIVE,
                                                                _operatingSystemMXBeanClass.getName()
                                                                + " Process CPU Time");
                statistics.add(injectedStatistic);

            }
            catch (NoSuchMethodException | SecurityException e)
            {
                LOGGER.warn("Failed to inject statistic 'getProcessCpuTime'");
                LOGGER.debug("Exception:",e);
            }

            try
            {
                Method method = _operatingSystemMXBeanClass.getDeclaredMethod("getProcessCpuLoad");
                method.setAccessible(true);
                Function<Broker, BigDecimal> supplier = broker -> {
                    try
                    {
                        final Object returnValue = method.invoke(_operatingSystemMXBean);

                        if (returnValue instanceof Number)
                        {
                            return BigDecimal.valueOf(((Number) returnValue).doubleValue()).setScale(4,
                                                                                                     RoundingMode.HALF_UP);
                        }
                    }
                    catch (IllegalAccessException | InvocationTargetException e)
                    {
                        LOGGER.warn("Unable to get current process CPU load", e);
                    }
                    return BigDecimal.valueOf(-1L);
                };

                Method getBigDecimalValue = BrokerAttributeInjector.class.getDeclaredMethod("getBigDecimalValue",
                                                                                            Broker.class,
                                                                                            Function.class);

                final ConfiguredObjectInjectedStatistic<?, ?> injectedStatistic =
                        new ConfiguredObjectInjectedStatistic<>("processCpuLoad",
                                                                getBigDecimalValue,
                                                                new Object[]{supplier},
                                                                "Current process CPU load",
                                                                _typeValidator,
                                                                StatisticUnit.COUNT,
                                                                StatisticType.POINT_IN_TIME,
                                                                _operatingSystemMXBean.getName()
                                                                + " Process CPU Load");
                statistics.add(injectedStatistic);

            }
            catch (NoSuchMethodException e)
            {
                LOGGER.warn("Failed to inject statistic 'getProcessCpuLoad'");
                LOGGER.debug("Exception:",e);
            }
        }
        return statistics;
    }

    @Override
    public Collection<ConfiguredObjectInjectedOperation<?>> getInjectedOperations()
    {
        List<ConfiguredObjectInjectedOperation<?>> operations = new ArrayList<>();

        if (_hotSpotDiagnosticMXBean != null)
        {
            try
            {
                operations.add(injectSetJVMOptions());
            }
            catch (NoSuchMethodException e)
            {
                LOGGER.warn("Failed to inject operation setJVMOptions", e);
            }

            try
            {
                operations.add(injectDumpHeap());
            }
            catch (NoSuchMethodException e)
            {
                LOGGER.warn("Failed to inject operation dumpHeap", e);
            }
        }

        return operations;
    }

    private ConfiguredObjectInjectedOperation<?> injectDumpHeap() throws NoSuchMethodException
    {
            Method heapDumpMethod =
                    _hotSpotDiagnosticMXBeanClass.getDeclaredMethod("dumpHeap", String.class, boolean.class);

            Method method = BrokerAttributeInjector.class.getDeclaredMethod("dumpHeap",
                                                                            Broker.class,
                                                                            PlatformManagedObject.class,
                                                                            Method.class,
                                                                            String.class,
                                                                            boolean.class);

            final OperationParameter[] params = new OperationParameter[2];

            params[0] = new OperationParameterFromInjection("outputFile",
                                                            String.class,
                                                            String.class,
                                                            "",
                                                            "the system-dependent filename",
                                                            new String[0], true);
            params[1] = new OperationParameterFromInjection("live",
                                                            boolean.class,
                                                            boolean.class,
                                                            "true",
                                                            "if true dump only live objects i.e. objects that are reachable from others",
                                                            new String[]{Boolean.TRUE.toString(), Boolean.FALSE.toString()},
                                                            false);
            ConfiguredObjectInjectedOperation<?> setVMOptionOperation = new ConfiguredObjectInjectedOperation(
                    "dumpHeap",
                    "Dumps the heap to the outputFile file in the same format as the hprof heap dump.",
                    true,
                    false,
                    "",
                    params,
                    method,
                    new Object[]{_hotSpotDiagnosticMXBean, heapDumpMethod},
                    _typeValidator);
            return setVMOptionOperation;
    }

    private ConfiguredObjectInjectedOperation<?> injectSetJVMOptions() throws NoSuchMethodException
    {

            Method setVMOptionMethod =
                    _hotSpotDiagnosticMXBeanClass.getDeclaredMethod("setVMOption", String.class, String.class);

            Method method = BrokerAttributeInjector.class.getDeclaredMethod("setJVMOptions",
                                                                            Broker.class,
                                                                            PlatformManagedObject.class,
                                                                            Method.class,
                                                                            Map.class);

            final OperationParameter[] params =
                    new OperationParameter[]{new OperationParameterFromInjection("options",
                                                                                 Map.class,
                                                                                 ParameterizedTypes.MAP_OF_STRING_STRING,
                                                                                 "",
                                                                                 "JVM options map",
                                                                                 new String[0], true)};
            ConfiguredObjectInjectedOperation<?> setVMOptionOperation = new ConfiguredObjectInjectedOperation(
                    "setJVMOptions",
                    "Sets given JVM options",
                    true,
                    false,
                    "",
                    params,
                    method,
                    new Object[]{_hotSpotDiagnosticMXBean, setVMOptionMethod},
                    _typeValidator);
            return setVMOptionOperation;
    }

    @Override
    public String getType()
    {
        return "Broker";
    }

    public static long getMemoryPoolUsed(Broker<?> broker, MemoryPoolMXBean memoryPoolMXBean)
    {
        return memoryPoolMXBean.getUsage().getUsed();
    }

    public static long getMemoryPoolMaximum(Broker<?> broker, MemoryPoolMXBean memoryPoolMXBean)
    {
        return memoryPoolMXBean.getUsage().getMax();
    }

    public static long getGCCollectionTime(Broker<?> broker, GarbageCollectorMXBean garbageCollectorMXBean)
    {
        return garbageCollectorMXBean.getCollectionTime();
    }

    public static long getGCCollectionCount(Broker<?> broker, GarbageCollectorMXBean garbageCollectorMXBean)
    {
        return garbageCollectorMXBean.getCollectionCount();
    }

    public static void setJVMOptions(Broker<?> broker,
                                     PlatformManagedObject hotSpotDiagnosticMXBean,
                                     Method setVMOption,
                                     Map<String, String> options)
    {
        broker.getEventLogger().message(BrokerMessages.OPERATION("setJVMOptions"));
        StringBuilder exceptionMessages = new StringBuilder();
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            try
            {
                setVMOption.invoke(hotSpotDiagnosticMXBean, entry.getKey(), entry.getValue());
            }
            catch (IllegalAccessException e)
            {
                LOGGER.warn("Cannot access setVMOption " + setVMOption);
            }
            catch (InvocationTargetException e)
            {
                if (exceptionMessages.length() > 0)
                {
                    exceptionMessages.append(";");
                }
                exceptionMessages.append(e.getTargetException().toString());
                LOGGER.warn("Cannot set option {} to {} due to {}",
                            entry.getKey(),
                            entry.getValue(),
                            e.getTargetException().toString());
            }

            if (exceptionMessages.length() > 0)
            {
                throw new IllegalConfigurationException("Exception(s) occurred whilst setting JVM options : " +
                                                        exceptionMessages.toString());
            }
        }
    }

    private static long getLongValue(Broker broker, ToLongFunction<Broker> supplier)
    {
        return supplier.applyAsLong(broker);
    }

    private static BigDecimal getBigDecimalValue(Broker broker, Function<Broker, BigDecimal> supplier)
    {
        return supplier.apply(broker);
    }


    public static void dumpHeap(Broker<?> broker,
                                PlatformManagedObject hotSpotDiagnosticMXBean,
                                Method dumpHeapMethod,
                                String outputFile,
                                boolean live)
    {
        broker.getEventLogger().message(BrokerMessages.OPERATION("dumpHeap"));
        try
        {
            dumpHeapMethod.invoke(hotSpotDiagnosticMXBean, outputFile, live);
        }
        catch (IllegalAccessException e)
        {
            LOGGER.warn("Cannot access dumpHeap " + dumpHeapMethod);
        }
        catch (InvocationTargetException e)
        {
            String causeAsString = e.getTargetException().toString();
            LOGGER.warn("Cannot collect heap dump into {} (with parameter 'live' set to '{}') due to {}",
                        outputFile,
                        live,
                        causeAsString);
            throw new IllegalConfigurationException("Unexpected exception on collecting heap dump : "
                                                    + causeAsString,
                                                    e.getTargetException());
        }
    }
}
