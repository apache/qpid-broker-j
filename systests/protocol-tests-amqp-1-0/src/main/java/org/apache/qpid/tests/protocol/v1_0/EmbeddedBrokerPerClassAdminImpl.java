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

package org.apache.qpid.tests.protocol.v1_0;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.logging.logback.LogbackLoggingSystemLauncherListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.model.NotFoundException;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.store.MemoryConfigurationStore;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhostnode.JsonVirtualHostNode;

@PluggableService
public class EmbeddedBrokerPerClassAdminImpl implements BrokerAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedBrokerPerClassAdminImpl.class);
    private final Map<String, Integer> _ports = new HashMap<>();
    private SystemLauncher _systemLauncher;
    private Container<?> _broker;
    private VirtualHostNode<?> _currentVirtualHostNode;
    private String _currentWorkDirectory;

    @Override
    public void beforeTestClass(final Class testClass)
    {
        LOGGER.info("beforeTestClass " + testClass.getSimpleName());
        try
        {
            String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()));
            _currentWorkDirectory = Files.createTempDirectory(String.format("qpid-work-%s-%s-", timestamp, testClass.getSimpleName())).toString();

            Map<String,String> context = new HashMap<>();
            context.put("qpid.work_dir", _currentWorkDirectory);
            context.put("qpid.port.protocol_handshake_timeout", "1000000");

            Map<String,Object> systemConfigAttributes = new HashMap<>();
            systemConfigAttributes.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, "classpath:config-protocol-tests.json");
            systemConfigAttributes.put(ConfiguredObject.CONTEXT, context);
            systemConfigAttributes.put(ConfiguredObject.TYPE, System.getProperty("broker.config-store-type", "JSON"));

            if (Thread.getDefaultUncaughtExceptionHandler() == null)
            {
                Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
            }

            LOGGER.info("Starting internal broker (same JVM)");

            List<SystemLauncherListener> systemLauncherListeners = new ArrayList<>();
            systemLauncherListeners.add(new LogbackLoggingSystemLauncherListener());
            systemLauncherListeners.add(new ShutdownLoggingSystemLauncherListener());
            systemLauncherListeners.add(new PortExtractingLauncherListener());
            _systemLauncher = new SystemLauncher(systemLauncherListeners.toArray(new SystemLauncherListener[systemLauncherListeners.size()]));

            _systemLauncher.startup(systemConfigAttributes);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to start broker for test class", e);
        }
    }

    @Override
    public void beforeTestMethod(final Class testClass, final Method method)
    {
        LOGGER.info("beforeTestMethod " + testClass.getSimpleName() + "#" + method.getName());

        final String virtualHostNodeName = testClass.getSimpleName() + "_" + method.getName();
        final String storeType = System.getProperty("virtualhostnode.type");

        String storeDir = null;
        if (System.getProperty("profile", "").startsWith("java-dby-mem"))
        {
            storeDir = ":memory:";
        }
        else if (!MemoryConfigurationStore.TYPE.equals(storeType))
        {
            storeDir = "${qpid.work_dir}" + File.separator + virtualHostNodeName;
        }

        String blueprint = System.getProperty("virtualhostnode.context.blueprint");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHostNode.NAME, virtualHostNodeName);
        attributes.put(VirtualHostNode.TYPE, storeType);
        attributes.put(VirtualHostNode.CONTEXT, Collections.singletonMap("virtualhostBlueprint", blueprint));
        attributes.put(VirtualHostNode.DEFAULT_VIRTUAL_HOST_NODE, true);
        attributes.put(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, blueprint);
        if (storeDir != null)
        {
            attributes.put(JsonVirtualHostNode.STORE_PATH, storeDir);
        }

        _currentVirtualHostNode = _broker.createChild(VirtualHostNode.class, attributes);
    }

    @Override
    public void afterTestMethod(final Class testClass, final Method method)
    {
        LOGGER.info("afterTestMethod " + testClass.getSimpleName() + "#" + method.getName());
        if (Boolean.getBoolean("broker.clean.between.tests"))
        {
            _currentVirtualHostNode.delete();
        }
    }

    @Override
    public void afterTestClass(final Class testClass)
    {
        LOGGER.info("afterTestClass " + testClass.getSimpleName());
        _systemLauncher.shutdown();
        _ports.clear();
        if (Boolean.getBoolean("broker.clean.between.tests"))
        {
            FileUtils.delete(new File(_currentWorkDirectory), true);
        }
    }

    @Override
    public InetSocketAddress getBrokerAddress(final PortType portType)
    {
        Integer port = _ports.get(portType.name());
        if (port == null)
        {
            throw new IllegalStateException(String.format("Could not find port with name '%s' on the Broker", portType.name()));
        }
        return new InetSocketAddress(port);
    }

    @Override
    public void createQueue(final String queueName)
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, queueName);
        attributes.put(Queue.TYPE, "standard");
        _currentVirtualHostNode.getVirtualHost().createChild(Queue.class, attributes);
    }

    @Override
    public void deleteQueue(final String queueName)
    {
        getQueue(queueName).delete();
    }

    @Override
    public String getType()
    {
        return "EMBEDDED_BROKER_PER_CLASS";
    }

    private Queue getQueue(final String queueName)
    {
        Collection<Queue> queues = _currentVirtualHostNode.getVirtualHost().getChildren(Queue.class);
        for (Queue queue : queues)
        {
            if (queue.getName().equals(queueName))
            {
                return queue;
            }
        }
        throw new NotFoundException(String.format("Queue '%s' not found", queueName));
    }

    private class PortExtractingLauncherListener implements SystemLauncherListener
    {
        private SystemConfig<?> _systemConfig;

        @Override
        public void beforeStartup()
        {

        }

        @Override
        public void errorOnStartup(final RuntimeException e)
        {

        }

        @Override
        public void afterStartup()
        {

            if (_systemConfig == null)
            {
                throw new IllegalStateException("System config is required");
            }

            _broker = _systemConfig.getContainer();
            Collection<Port> ports = _broker.getChildren(Port.class);
            for (Port port : ports)
            {
                _ports.put(port.getName(), port.getBoundPort());
            }
        }

        @Override
        public void onContainerResolve(final SystemConfig<?> systemConfig)
        {
            _systemConfig = systemConfig;
        }

        @Override
        public void onContainerClose(final SystemConfig<?> systemConfig)
        {

        }

        @Override
        public void onShutdown(final int exitCode)
        {

        }

        @Override
        public void exceptionOnShutdown(final Exception e)
        {

        }
    }


    private static class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler
    {
        private final AtomicInteger _count = new AtomicInteger(0);

        @Override
        public void uncaughtException(final Thread t, final Throwable e)
        {
            System.err.print("Thread terminated due to uncaught exception");
            e.printStackTrace();

            LOGGER.error("Uncaught exception from thread {}", t.getName(), e);
            _count.getAndIncrement();
        }

        public int getAndResetCount()
        {
            int count;
            do
            {
                count = _count.get();
            }
            while (!_count.compareAndSet(count, 0));
            return count;
        }
    }

    private class ShutdownLoggingSystemLauncherListener extends SystemLauncherListener.DefaultSystemLauncherListener
    {
        @Override
        public void onShutdown(final int exitCode)
        {
            _systemLauncher = null;
        }

        @Override
        public void exceptionOnShutdown(final Exception e)
        {
            if (e instanceof IllegalStateException
                || e instanceof IllegalStateTransitionException)
            {
                System.out.println(
                        "IllegalStateException occurred on broker shutdown in test ");
            }
        }
    }

}
