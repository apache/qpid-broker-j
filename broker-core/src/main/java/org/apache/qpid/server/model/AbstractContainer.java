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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.plugin.ConfigurationSecretEncrypterFactory;
import org.apache.qpid.server.plugin.PluggableFactoryLoader;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.util.HousekeepingExecutor;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.SystemUtils;

public abstract class AbstractContainer<X extends AbstractContainer<X>> extends AbstractConfiguredObject<X> implements Container<X>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractContainer.class);

    private final BufferPoolMXBean _bufferPoolMXBean;
    private final List<String> _jvmArguments;
    private final long _maximumHeapSize = Runtime.getRuntime().maxMemory();
    private final long _maximumDirectMemorySize = getMaxDirectMemorySize();
    protected SystemConfig<?> _parent;
    protected EventLogger _eventLogger;

    @ManagedAttributeField(beforeSet = "preEncrypterProviderSet", afterSet = "postEncrypterProviderSet")
    private String _confidentialConfigurationEncryptionProvider;

    protected HousekeepingExecutor _houseKeepingTaskExecutor;

    @ManagedAttributeField
    private int _housekeepingThreadCount;

    private String _preConfidentialConfigurationEncryptionProvider;

    AbstractContainer(
            final Map<String, Object> attributes,
            SystemConfig parent)
    {
        super(parent, attributes);
        _parent = parent;
        _eventLogger = parent.getEventLogger();
        _jvmArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        BufferPoolMXBean bufferPoolMXBean = null;
        List<BufferPoolMXBean> bufferPoolMXBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for(BufferPoolMXBean mBean : bufferPoolMXBeans)
        {
            if (mBean.getName().equals("direct"))
            {
                bufferPoolMXBean = mBean;
                break;
            }
        }
        _bufferPoolMXBean = bufferPoolMXBean;
        if(attributes.get(CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER) != null )
        {
            _confidentialConfigurationEncryptionProvider =
                    String.valueOf(attributes.get(CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER));
        }

    }

    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();
        if (_confidentialConfigurationEncryptionProvider != null)
        {
            updateEncrypter(_confidentialConfigurationEncryptionProvider);
        }
    }

    @SuppressWarnings("unused")
    public static Collection<String> getAvailableConfigurationEncrypters()
    {
        return (new QpidServiceLoader()).getInstancesByType(ConfigurationSecretEncrypterFactory.class).keySet();
    }

    static long getMaxDirectMemorySize()
    {
        long maxMemory = 0;
        try
        {
            ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
            Class<?> hotSpotDiagnosticMXBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean", true, systemClassLoader);
            Class<?> vmOptionClass = Class.forName("com.sun.management.VMOption", true, systemClassLoader);

            Object hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean((Class<? extends PlatformManagedObject>)hotSpotDiagnosticMXBeanClass);
            Method getVMOption = hotSpotDiagnosticMXBeanClass.getDeclaredMethod("getVMOption", String.class);
            Object vmOption = getVMOption.invoke(hotSpotDiagnosticMXBean, "MaxDirectMemorySize");
            Method getValue = vmOptionClass.getDeclaredMethod("getValue");
            String maxDirectMemoryAsString = (String)getValue.invoke(vmOption);
            maxMemory = Long.parseLong(maxDirectMemoryAsString);
        }
        catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | IllegalArgumentException e)
        {
            LOGGER.debug("Cannot determine direct memory max size using com.sun.management.HotSpotDiagnosticMXBean: " + e.getMessage());
        }
        catch (InvocationTargetException e)
        {
            throw new ServerScopedRuntimeException("Unexpected exception in evaluation of MaxDirectMemorySize with HotSpotDiagnosticMXBean", e.getTargetException());
        }

        if (maxMemory == 0)
        {
            Pattern
                    maxDirectMemorySizeArgumentPattern = Pattern.compile("^\\s*-XX:MaxDirectMemorySize\\s*=\\s*(\\d+)\\s*([KkMmGgTt]?)\\s*$");
            RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
            List<String> inputArguments = RuntimemxBean.getInputArguments();
            boolean argumentFound = false;
            for (String argument : inputArguments)
            {
                Matcher matcher = maxDirectMemorySizeArgumentPattern.matcher(argument);
                if (matcher.matches())
                {
                    argumentFound = true;
                    maxMemory = Long.parseLong(matcher.group(1));
                    String unit = matcher.group(2);
                    char unitChar = "".equals(unit) ? 0 : unit.charAt(0);
                    switch (unitChar)
                    {
                        case 'k':
                        case 'K':
                            maxMemory *= 1024l;
                            break;
                        case 'm':
                        case 'M':
                            maxMemory *= 1024l * 1024l;
                            break;
                        case 'g':
                        case 'G':
                            maxMemory *= 1024l * 1024l * 1024l;
                            break;
                        case 't':
                        case 'T':
                            maxMemory *= 1024l * 1024l * 1024l * 1024l;
                            break;
                        case 0:
                            // noop
                            break;
                        default:
                            throw new IllegalStateException("Unexpected unit character in MaxDirectMemorySize argument : " + argument);
                    }
                    // do not break; continue. Oracle and IBM JVMs use the last value when argument is specified multiple times
                }
            }

            if (maxMemory == 0)
            {
                if (argumentFound)
                {
                    throw new IllegalArgumentException("Qpid Broker cannot operate with 0 direct memory. Please, set JVM argument MaxDirectMemorySize to non-zero value");
                }
                else
                {
                    maxMemory = Runtime.getRuntime().maxMemory();
                }
            }
        }

        return maxMemory;
    }

    private void updateEncrypter(final String encryptionProviderType)
    {
        if(encryptionProviderType != null && !"".equals(encryptionProviderType.trim()))
        {
            PluggableFactoryLoader<ConfigurationSecretEncrypterFactory> factoryLoader =
                    new PluggableFactoryLoader<>(ConfigurationSecretEncrypterFactory.class);
            ConfigurationSecretEncrypterFactory factory = factoryLoader.get(encryptionProviderType);
            if (factory == null)
            {
                throw new IllegalConfigurationException("Unknown Configuration Secret Encryption method "
                                                        + encryptionProviderType);
            }
            setEncrypter(factory.createEncrypter(this));
        }
        else
        {
            setEncrypter(null);
        }
    }

    public String getBuildVersion()
    {
        return CommonProperties.getBuildVersion();
    }

    public String getOperatingSystem()
    {
        return SystemUtils.getOSString();
    }

    public String getPlatform()
    {
        return System.getProperty("java.vendor") + " "
                      + System.getProperty("java.runtime.version", System.getProperty("java.version"));
    }

    public String getProcessPid()
    {
        return SystemUtils.getProcessPid();
    }

    public String getProductVersion()
    {
        return CommonProperties.getReleaseVersion();
    }

    public int getNumberOfCores()
    {
        return Runtime.getRuntime().availableProcessors();
    }

    public String getConfidentialConfigurationEncryptionProvider()
    {
        return _confidentialConfigurationEncryptionProvider;
    }

    public int getHousekeepingThreadCount()
    {
        return _housekeepingThreadCount;
    }

    public String getModelVersion()
    {
        return BrokerModel.MODEL_VERSION;
    }

    @Override
    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    @Override
    public void setEventLogger(final EventLogger eventLogger)
    {
        _eventLogger = eventLogger;
    }

    @SuppressWarnings("unused")
    private void preEncrypterProviderSet()
    {
        _preConfidentialConfigurationEncryptionProvider = _confidentialConfigurationEncryptionProvider;
    }

    @SuppressWarnings("unused")
    private void postEncrypterProviderSet()
    {
        if (!Objects.equals(_preConfidentialConfigurationEncryptionProvider,
                            _confidentialConfigurationEncryptionProvider))
        {
            updateEncrypter(_confidentialConfigurationEncryptionProvider);
            forceUpdateAllSecureAttributes();
        }
    }

    public int getNumberOfLiveThreads()
    {
        return ManagementFactory.getThreadMXBean().getThreadCount();
    }

    public long getMaximumHeapMemorySize()
    {
        return _maximumHeapSize;
    }

    public long getUsedHeapMemorySize()
    {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    public long getMaximumDirectMemorySize()
    {
        return _maximumDirectMemorySize;
    }

    public long getUsedDirectMemorySize()
    {
        if (_bufferPoolMXBean == null)
        {
            return -1;
        }
        return _bufferPoolMXBean.getMemoryUsed();
    }

    public long getDirectMemoryTotalCapacity()
    {
        if (_bufferPoolMXBean == null)
        {
            return -1;
        }
        return _bufferPoolMXBean.getTotalCapacity();
    }

    public int getNumberOfObjectsPendingFinalization()
    {
        return ManagementFactory.getMemoryMXBean().getObjectPendingFinalizationCount();
    }

    public List<String> getJvmArguments()
    {
        return _jvmArguments;
    }

    public void performGC()
    {
        getEventLogger().message(BrokerMessages.OPERATION("performGC"));
        System.gc();
    }

    public Content getThreadStackTraces(boolean appendToLog)
    {
        getEventLogger().message(BrokerMessages.OPERATION("getThreadStackTraces"));
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        StringBuilder threadDump = new StringBuilder();
        threadDump.append(String.format("Full thread dump captured %s", Instant.now())).append(System.lineSeparator());

        for (ThreadInfo threadInfo : threadInfos)
        {
            threadDump.append(getThreadStackTraces(threadInfo));
        }
        long[] deadLocks = threadMXBean.findDeadlockedThreads();
        if (deadLocks != null && deadLocks.length > 0)
        {
            ThreadInfo[] deadlockedThreads = threadMXBean.getThreadInfo(deadLocks);
            threadDump.append(System.lineSeparator()).append("Deadlock is detected!").append(System.lineSeparator());
            for (ThreadInfo threadInfo : deadlockedThreads)
            {
                threadDump.append(getThreadStackTraces(threadInfo));
            }
        }
        String threadStackTraces = threadDump.toString();
        if (appendToLog)
        {
            LOGGER.warn("Thread dump:{} {}", System.lineSeparator(), threadStackTraces);
        }
        return new ThreadStackContent(threadStackTraces);
    }

    public Content findThreadStackTraces(String threadNameFindExpression)
    {
        getEventLogger().message(BrokerMessages.OPERATION("findThreadStackTraces"));
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        StringBuilder threadDump = new StringBuilder();
        threadDump.append(String.format("Thread dump (names matching '%s') captured %s",
                                        threadNameFindExpression,
                                        Instant.now())).append(System.lineSeparator());

        Pattern pattern = threadNameFindExpression == null || threadNameFindExpression.equals("") ? null : Pattern.compile(
                threadNameFindExpression);
        for (ThreadInfo threadInfo : threadInfos)
        {
            if (pattern == null || pattern.matcher(threadInfo.getThreadName()).find())
            {
                threadDump.append(getThreadStackTraces(threadInfo));
            }
        }
        return new ThreadStackContent(threadDump.toString());
    }

    private String getThreadStackTraces(final ThreadInfo threadInfo)
    {
        String lineSeparator = System.lineSeparator();
        StringBuilder dump = new StringBuilder();
        dump.append("\"").append(threadInfo.getThreadName()).append("\"").append(" Id=")
            .append(threadInfo.getThreadId()).append( " ").append(threadInfo.getThreadState());
        if (threadInfo.getLockName() != null)
        {
            dump.append(" on ").append(threadInfo.getLockName());
        }
        if (threadInfo.getLockOwnerName() != null)
        {
            dump.append(" owned by \"").append(threadInfo.getLockOwnerName())
                .append("\" Id=").append(threadInfo.getLockOwnerId());
        }
        if (threadInfo.isSuspended())
        {
            dump.append(" (suspended)");
        }
        if (threadInfo.isInNative())
        {
            dump.append(" (in native)");
        }
        dump.append(lineSeparator);
        StackTraceElement[] stackTrace = threadInfo.getStackTrace();
        for (int i = 0; i < stackTrace.length; i++)
        {
            StackTraceElement stackTraceElement = stackTrace[i];
            dump.append("    at ").append(stackTraceElement.toString()).append(lineSeparator);

            LockInfo lockInfo = threadInfo.getLockInfo();
            if (i == 0 && lockInfo != null)
            {
                Thread.State threadState = threadInfo.getThreadState();
                switch (threadState)
                {
                    case BLOCKED:
                        dump.append("    -  blocked on ").append(lockInfo).append(lineSeparator);
                        break;
                    case WAITING:
                        dump.append("    -  waiting on ").append(lockInfo).append(lineSeparator);
                        break;
                    case TIMED_WAITING:
                        dump.append("    -  waiting on ").append(lockInfo).append(lineSeparator);
                        break;
                    default:
                }
            }

            for (MonitorInfo mi : threadInfo.getLockedMonitors())
            {
                if (mi.getLockedStackDepth() == i)
                {
                    dump.append("    -  locked ").append(mi).append(lineSeparator);
                }
            }
        }

        LockInfo[] locks = threadInfo.getLockedSynchronizers();
        if (locks.length > 0)
        {
            dump.append(lineSeparator).append("    Number of locked synchronizers = ").append(locks.length);
            dump.append(lineSeparator);
            for (LockInfo li : locks)
            {
                dump.append("    - " + li);
                dump.append(lineSeparator);
            }
        }
        dump.append(lineSeparator);
        return dump.toString();
    }

    public ScheduledFuture<?> scheduleHouseKeepingTask(long period, final TimeUnit unit, Runnable task)
    {
        return _houseKeepingTaskExecutor.scheduleAtFixedRate(task, period / 2, period, unit);
    }

    public ScheduledFuture<?> scheduleTask(long delay, final TimeUnit unit, Runnable task)
    {
        return _houseKeepingTaskExecutor.schedule(task, delay, unit);
    }

    public static class ThreadStackContent implements Content, CustomRestHeaders
    {
        private final String _threadStackTraces;

        public ThreadStackContent(final String threadStackTraces)
        {
            _threadStackTraces = threadStackTraces;
        }

        @Override
        public void write(final OutputStream outputStream) throws IOException
        {
            if (_threadStackTraces != null)
            {
                outputStream.write(_threadStackTraces.getBytes(Charset.forName("UTF-8")));
            }
        }

        @Override
        public void release()
        {
            // noop; nothing to release
        }

        @RestContentHeader("Content-Type")
        public String getContentType()
        {
            return "text/plain;charset=utf-8";
        }
    }
}
