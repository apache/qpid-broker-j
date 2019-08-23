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
package org.apache.qpid.systests.admin;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.systests.Utils.getAmqpManagementFacade;
import static org.apache.qpid.systests.Utils.getJmsProvider;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.NamingException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.util.SystemUtils;
import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.ConfigItem;

@SuppressWarnings("unused")
@PluggableService
public class SpawnBrokerAdmin implements BrokerAdmin, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SpawnBrokerAdmin.class);

    public static final String SYSTEST_PROPERTY_SPAWN_BROKER_STARTUP_TIME = "qpid.systests.broker_startup_time";
    private static final String SYSTEST_PROPERTY_VIRTUALHOSTNODE_TYPE = "virtualhostnode.type";
    private static final String SYSTEST_PROPERTY_VIRTUALHOST_BLUEPRINT = "virtualhostnode.context.blueprint";
    private static final String SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION = "qpid.initialConfigurationLocation";
    static final String SYSTEST_PROPERTY_BUILD_CLASSPATH_FILE = "qpid.systests.build.classpath.file";
    private static final String AMQP_QUEUE_TYPE = "org.apache.qpid.Queue";
    private static final String AMQP_NODE_TYPE = "org.apache.qpid.VirtualHostNode";
    private static final String AMQP_VIRTUAL_HOST_TYPE = "org.apache.qpid.VirtualHost";

    private static final AtomicLong COUNTER = new AtomicLong();

    private String _currentWorkDirectory;
    private ExecutorService _executorService;
    private Process _process;
    private Integer _pid;
    private List<ListeningPort> _ports;
    private boolean _isPersistentStore;
    private String _virtualHostNodeName;
    private final long _id = COUNTER.incrementAndGet();

    @Override
    public void beforeTestClass(final Class testClass)
    {
        startBroker(testClass);
    }

    @Override
    public void beforeTestMethod(final Class testClass, final Method method)
    {
        final String nodeType = System.getProperty(SYSTEST_PROPERTY_VIRTUALHOSTNODE_TYPE, "JSON");
        final String virtualHostNodeName = testClass.getSimpleName() + "_" + method.getName();
        final Map<String, Object> attributes = getNodeAttributes(virtualHostNodeName, nodeType);

        beforeTestMethod(virtualHostNodeName, nodeType, attributes);
    }

    @Override
    public void afterTestMethod(final Class testClass, final Method method)
    {
        if (_virtualHostNodeName != null)
        {
            deleteVirtualHostNode(_virtualHostNodeName);
        }
    }

    @Override
    public void afterTestClass(final Class testClass)
    {
        shutdownBroker();
    }

    @Override
    public InetSocketAddress getBrokerAddress(final PortType portType)
    {
        if (_ports == null)
        {
            throw new IllegalArgumentException("Port information not present");
        }
        Integer port = null;
        switch (portType)
        {
            case AMQP:
                for (ListeningPort p : _ports)
                {
                    if (p.getTransport().contains("TCP"))
                    {
                        port = p.getPort();
                        break;
                    }
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown port type '%s'", portType));
        }
        if (port == null)
        {
            throw new IllegalArgumentException(String.format("Cannot find port of type '%s'", portType));
        }
        return new InetSocketAddress(port);
    }

    @Override
    public void createQueue(final String queueName)
    {
        invokeManagementOperation(false, (amqpManagementFacade, session) -> {
            amqpManagementFacade.createEntityAndAssertResponse(queueName,
                                                               AMQP_QUEUE_TYPE,
                                                               Collections.emptyMap(),
                                                               session);
            return null;
        });
        invokeManagementOperation(false, (amqpManagementFacade, session) -> {
            // bind queue to direct exchange automatically
            Map<String, Object> arguments = new HashMap<>();
            arguments.put("destination", queueName);
            arguments.put("bindingKey", queueName);
            amqpManagementFacade.performOperationUsingAmqpManagement("amq.direct",
                                                                     "bind",
                                                                     session,
                                                                     "org.apache.qpid.DirectExchange",
                                                                     arguments);
            return null;
        });
    }

    @Override
    public void deleteQueue(final String queueName)
    {
        invokeManagementOperation(false, (amqpManagementFacade, session) -> {
            amqpManagementFacade.deleteEntityUsingAmqpManagement(queueName,
                                                                 session,
                                                                 AMQP_QUEUE_TYPE);
            return null;
        });
    }

    @Override
    public void putMessageOnQueue(final String queueName, final String... messages)
    {
        for (String content : messages)
        {
            final Map<String, Object> message = new HashMap<>();
            message.put("content", content);
            message.put("address", queueName);
            message.put("mimeType", "text/plain");

            invokeManagementOperation(false, (amqpManagementFacade, session) -> {
                amqpManagementFacade.performOperationUsingAmqpManagement(_virtualHostNodeName,
                                                                         "publishMessage",
                                                                         session,
                                                                         AMQP_VIRTUAL_HOST_TYPE,
                                                                         Collections.singletonMap("message",
                                                                                                  message));
                return null;
            });
        }
    }

    @Override
    public int getQueueDepthMessages(final String testQueueName)
    {
        return invokeManagementOperation(false, (amqpManagementFacade, session) -> {
            Map<String, Object> arguments = Collections.singletonMap("statistics",
                                                                     Collections.singletonList("queueDepthMessages"));
            Object statistics = amqpManagementFacade.performOperationUsingAmqpManagement(testQueueName,
                                                                                         "getStatistics",
                                                                                         session,
                                                                                         AMQP_QUEUE_TYPE,
                                                                                         arguments);

            @SuppressWarnings("unchecked")
            Map<String, Object> stats = (Map<String, Object>) statistics;
            return ((Number) stats.get("queueDepthMessages")).intValue();
        });
    }

    @Override
    public boolean supportsRestart()
    {
        return _isPersistentStore;
    }

    @Override
    public ListenableFuture<Void> restart()
    {
        stop();
        start();
        return Futures.immediateFuture(null);
    }

    @Override
    public boolean isAnonymousSupported()
    {
        return true;
    }

    @Override
    public boolean isSASLSupported()
    {
        return true;
    }

    @Override
    public boolean isSASLMechanismSupported(final String mechanismName)
    {
        return true;
    }

    @Override
    public boolean isWebSocketSupported()
    {
        return false;
    }

    @Override
    public boolean isQueueDepthSupported()
    {
        return true;
    }

    @Override
    public boolean isManagementSupported()
    {
        return true;
    }

    @Override
    public boolean isPutMessageOnQueueSupported()
    {
        return true;
    }

    @Override
    public boolean isDeleteQueueSupported()
    {
        return true;
    }

    @Override
    public String getValidUsername()
    {
        return "admin";
    }

    @Override
    public String getValidPassword()
    {
        return "admin";
    }

    @Override
    public String getKind()
    {
        return KIND_BROKER_J;
    }

    @Override
    public String getType()
    {
        return "SPAWN_BROKER_PER_CLASS";
    }

    @Override
    public void close()
    {
        shutdownBroker();
    }

    public void beforeTestMethod(final String virtualHostNodeName,
                                 final String nodeType,
                                 final Map<String, Object> attributes)
    {
        _isPersistentStore = !"Memory".equals(nodeType);
        _virtualHostNodeName = virtualHostNodeName;

        if (!attributes.containsKey("qpid-type"))
        {
            attributes.put("qpid-type", nodeType);
        }

        invokeManagementOperation(true, ((amqpManagementFacade, session) -> {
            amqpManagementFacade.createEntityAndAssertResponse(virtualHostNodeName,
                                                               AMQP_NODE_TYPE,
                                                               attributes,
                                                               session);
            return null;
        }));
    }

    public void start()
    {
        if (_virtualHostNodeName == null)
        {
            throw new BrokerAdminException("Virtual host is not created");
        }
        invokeManagementOperation(true, (amqpManagementFacade, session) -> {
            amqpManagementFacade.updateEntityUsingAmqpManagementAndReceiveResponse(_virtualHostNodeName,
                                                                                   AMQP_NODE_TYPE,
                                                                                   Collections.singletonMap(
                                                                                           "desiredState",
                                                                                           "ACTIVE"), session);
            return null;
        });
    }

    public void stop()
    {
        if (_virtualHostNodeName == null)
        {
            throw new BrokerAdminException("Virtual host is not created");
        }
        invokeManagementOperation(true, (amqpManagementFacade, session) -> {
            amqpManagementFacade.updateEntityUsingAmqpManagementAndReceiveResponse(_virtualHostNodeName,
                                                                                   AMQP_NODE_TYPE,
                                                                                   Collections.singletonMap(
                                                                                           "desiredState",
                                                                                           "STOPPED"),
                                                                                   session);
            return null;
        });
    }

    public Object awaitAttributeValue(long timeLimitMilliseconds,
                                      boolean isBrokerManagement,
                                      String name,
                                      String type,
                                      String attributeName,
                                      Object... attributeValue)
    {
        Object value;
        long limit = System.currentTimeMillis() + timeLimitMilliseconds;

        do
        {
            value = invokeManagementOperation(isBrokerManagement, ((amqpManagementFacade, session) -> {

                Map<String, Object> object = null;
                try
                {
                    object = amqpManagementFacade.readEntityUsingAmqpManagement(session, type, name, false);
                }
                catch (AmqpManagementFacade.OperationUnsuccessfulException e)
                {
                    if (e.getStatusCode() != 404)
                    {
                        throw e;
                    }
                }
                return object == null ? null : object.get(attributeName);
            }));

            final Object lookup = value;
            if (Arrays.stream(attributeValue).anyMatch(v -> v.equals(lookup)))
            {
                break;
            }
            else
            {
                try
                {
                    Thread.sleep(50);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        while (System.currentTimeMillis() < limit);
        return value;
    }

    public String getVirtualHostName()
    {
        return _virtualHostNodeName;
    }

    public void update(final boolean brokerManagement,
                       final String name,
                       final String type,
                       final Map<String, Object> attributes)
    {
        invokeManagementOperation(brokerManagement, (amqpManagementFacade, session) -> {
            amqpManagementFacade.updateEntityUsingAmqpManagement(name,
                                                                 session,
                                                                 type,
                                                                 attributes);
            return null;
        });
    }

    public Map<String, Object> getAttributes(final boolean brokerManagement, final String name, final String type)
    {
        return invokeManagementOperation(brokerManagement,
                                         (amqpManagementFacade, session) -> amqpManagementFacade.readEntityUsingAmqpManagement(
                                                 session,
                                                 type,
                                                 name,
                                                 false));
    }

    public String dumpThreads()
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            Process process = Runtime.getRuntime().exec("jstack " + _pid);
            InputStream is = process.getInputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1)
            {
                baos.write(buffer, 0, length);
            }
            return new String(baos.toByteArray());
        }
        catch (Exception e)
        {
            LOGGER.error("Error whilst collecting thread dump for " + _pid, e);
            return "";
        }
        finally
        {
            try
            {
                baos.close();
            }
            catch (IOException e)
            {
                // ignore
            }
        }
    }

    private Map<String, Object> getNodeAttributes(final String virtualHostNodeName, final String nodeType)
    {
        String storeDir;
        if (System.getProperty("profile", "").startsWith("java-dby-mem"))
        {
            storeDir = ":memory:";
        }
        else
        {
            storeDir = "${qpid.work_dir}" + File.separator + virtualHostNodeName;
        }
        Map<String, Object> attributes = new HashMap<>();
        String blueprint =
                System.getProperty(SYSTEST_PROPERTY_VIRTUALHOST_BLUEPRINT, "{\"type\":\"ProvidedStore\"}");
        LOGGER.debug("Creating Virtual host {} from blueprint: {}", virtualHostNodeName, blueprint);

        attributes.put("name", virtualHostNodeName);
        attributes.put("type", nodeType);
        attributes.put("qpid-type", nodeType);
        String contextAsString;
        try
        {
            contextAsString =
                    new ObjectMapper().writeValueAsString(Collections.singletonMap("virtualhostBlueprint",
                                                                                   blueprint));
        }
        catch (JsonProcessingException e)
        {
            throw new BrokerAdminException("Cannot create virtual host as context serialization failed", e);
        }
        attributes.put("context", contextAsString);
        attributes.put("defaultVirtualHostNode", true);
        attributes.put("virtualHostInitialConfiguration", blueprint);
        attributes.put("storePath", storeDir);
        return attributes;
    }

    private void deleteVirtualHostNode(final String virtualHostNodeName)
    {
        invokeManagementOperation(true,
                                  (amqpManagementFacade, session) -> {
                                      amqpManagementFacade.deleteEntityUsingAmqpManagement(virtualHostNodeName,
                                                                                           session,
                                                                                           AMQP_NODE_TYPE);
                                      _virtualHostNodeName = null;
                                      return null;
                                  });
    }

    private <T> T invokeManagementOperation(boolean isBrokerOperation, AmqpManagementOperation<T> operation)
    {
        try
        {
            InetSocketAddress brokerAddress = getBrokerAddress(BrokerAdmin.PortType.AMQP);

            final Connection connection = getJmsProvider().getConnectionBuilder()
                                                          .setVirtualHost(isBrokerOperation
                                                                                  ? "$management"
                                                                                  : getVirtualHostName())
                                                          .setClientId("admin-" + UUID.randomUUID().toString())
                                                          .setHost(brokerAddress.getHostName())
                                                          .setPort(brokerAddress.getPort())
                                                          .setFailover(false)
                                                          .build();
            try
            {
                connection.start();
                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try
                {
                    return operation.invoke(getAmqpManagementFacade(), session);
                }
                catch (AmqpManagementFacade.OperationUnsuccessfulException | JMSException e)
                {
                    throw new BrokerAdminException("Cannot perform operation", e);
                }
                finally
                {
                    session.close();
                }
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException | NamingException e)
        {
            throw new BrokerAdminException("Cannot create connection to broker", e);
        }
    }

    private void startBroker(final Class testClass)
    {
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()));
        boolean brokerStarted = false;
        try
        {
            _currentWorkDirectory =
                    Files.createTempDirectory(String.format("qpid-work-%d-%s-%s-",
                                                            _id,
                                                            testClass.getSimpleName(),
                                                            timestamp))
                         .toString();

            String readyLogPattern = "BRK-1004 : Qpid Broker Ready";

            LOGGER.debug("Spawning broker working folder: {}", _currentWorkDirectory);

            int startUpTime = Integer.getInteger(SYSTEST_PROPERTY_SPAWN_BROKER_STARTUP_TIME, 30000);

            LOGGER.debug("Spawning broker permitted start-up time: {}", startUpTime);

            ProcessBuilder processBuilder = createBrokerProcessBuilder(_currentWorkDirectory, testClass);
            processBuilder.redirectErrorStream(true);

            Map<String, String> processEnvironment = processBuilder.environment();
            processEnvironment.put("QPID_PNAME", String.format("-DPNAME=QPBRKR -DTNAME=\"%s\"", testClass.getName()));

            CountDownLatch readyLatch = new CountDownLatch(1);
            long startTime = System.currentTimeMillis();

            LOGGER.debug("Starting broker process");
            _process = processBuilder.start();

            BrokerSystemOutputHandler brokerSystemOutputHandler =
                    new BrokerSystemOutputHandler(_process.getInputStream(),
                                                  readyLatch
                    );

            _executorService = Executors.newFixedThreadPool(1, r -> {
                Thread t = new Thread(r, "SPAWN-" + _id);
                t.setDaemon(false);
                return t;
            });

            _executorService.submit(brokerSystemOutputHandler);
            if (!readyLatch.await(startUpTime, TimeUnit.MILLISECONDS))
            {
                LOGGER.warn("Spawned broker failed to become ready within {} ms. Ready line '{}'",
                            startUpTime, readyLogPattern);
                throw new BrokerAdminException(String.format(
                        "Broker failed to become ready within %d ms. Stop line : %s",
                        startUpTime,
                        readyLogPattern));
            }

            _pid = brokerSystemOutputHandler.getPID();
            _ports = brokerSystemOutputHandler.getAmqpPorts();

            if (_pid == -1)
            {
                throw new BrokerAdminException("Broker PID is not detected");
            }

            if (_ports.size() == 0)
            {
                throw new BrokerAdminException("Broker port is not detected");
            }

            try
            {
                int exit = _process.exitValue();
                LOGGER.info("broker aborted: {}", exit);
                throw new BrokerAdminException("broker aborted: " + exit);
            }
            catch (IllegalThreadStateException e)
            {
                // ignore
            }

            LOGGER.info("Broker was started successfully within {} milliseconds, broker PID {}",
                        System.currentTimeMillis() - startTime,
                        _pid);
            LOGGER.info("Broker ports: {}", _ports);
            brokerStarted = true;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception e)
        {
            throw new BrokerAdminException(String.format("Unexpected exception on broker startup: %s", e), e);
        }
        finally
        {
            if (!brokerStarted)
            {
                LOGGER.warn("Broker failed to start");
                if (_process != null)
                {
                    _process.destroy();
                    _process = null;
                }
                if (_executorService != null)
                {
                    _executorService.shutdown();
                    _executorService = null;
                }
                _ports = null;
                _pid = null;
            }
        }
    }


    private ProcessBuilder createBrokerProcessBuilder(String currentWorkDirectory, Class testClass) throws IOException
    {
        String initialConfiguration = System.getProperty(SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION);
        if (initialConfiguration == null)
        {
            throw new BrokerAdminException(
                    String.format("No initial configuration is found: JVM property '%s' is not set.",
                                  SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION));
        }

        File testInitialConfiguration = new File(currentWorkDirectory, "initial-configuration.json");
        if (!testInitialConfiguration.createNewFile())
        {
            throw new BrokerAdminException("Failed to create a file for a copy of initial configuration");
        }
        if (initialConfiguration.startsWith("classpath:"))
        {
            String config = initialConfiguration.substring("classpath:".length());
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(config);
                 OutputStream os = new FileOutputStream(testInitialConfiguration))
            {
                ByteStreams.copy(is, os);
            }
        }
        else
        {
            Files.copy(new File(initialConfiguration).toPath(), testInitialConfiguration.toPath());
        }

        String classpath;
        File file = new File(System.getProperty(SYSTEST_PROPERTY_BUILD_CLASSPATH_FILE));
        if (!file.exists())
        {
            throw new BrokerAdminException(String.format("Cannot find file with classpath: %s",
                                                         file.getAbsoluteFile()));
        }
        else
        {
            classpath = new String(Files.readAllBytes(file.toPath()), UTF_8);
        }

        final ConfigItem[] configItems = (ConfigItem[]) testClass.getAnnotationsByType(ConfigItem.class);

        List<String> jvmArguments = new ArrayList<>();
        jvmArguments.add("java");
        jvmArguments.add("-cp");
        jvmArguments.add(classpath);
        jvmArguments.add("-Djava.io.tmpdir=" + escape(System.getProperty("java.io.tmpdir")));
        jvmArguments.add("-Dlogback.configurationFile=default-broker-logback.xml");
        jvmArguments.add("-Dqpid.tests.mms.messagestore.persistence=true");
        jvmArguments.addAll(Arrays.stream(configItems)
                                  .filter(ConfigItem::jvm)
                                  .map(ci -> String.format("-D%s=%s", ci.name(), ci.value()))
                                  .collect(Collectors.toList()));
        jvmArguments.add("org.apache.qpid.server.Main");
        jvmArguments.add("--store-type");
        jvmArguments.add("JSON");
        jvmArguments.add("--initial-config-path");
        jvmArguments.add(escape(testInitialConfiguration.toString()));

        Map<String, String> context = new HashMap<>();
        context.put("qpid.work_dir", escape(currentWorkDirectory));
        context.put("qpid.port.protocol_handshake_timeout", "1000000");
        context.put("qpid.amqp_port", "0");

        System.getProperties()
              .stringPropertyNames()
              .stream()
              .filter(n -> n.startsWith("qpid."))
              .forEach(n -> context.put(n, System.getProperty(n)));

        context.putAll(Arrays.stream(configItems)
                             .filter(i -> !i.jvm())
                             .collect(Collectors.toMap(ConfigItem::name,
                                                       ConfigItem::value,
                                                       (name, value) -> value)));

        context.forEach((key, value) -> jvmArguments.addAll(Arrays.asList("-prop",
                                                                          String.format("%s=%s", key, value))));

        LOGGER.debug("Spawning broker JVM :", jvmArguments);
        String[] cmd = jvmArguments.toArray(new String[jvmArguments.size()]);

        LOGGER.debug("command line:" + String.join(" ", jvmArguments));
        return new ProcessBuilder(cmd);
    }

    private String escape(String value)
    {
        if (SystemUtils.isWindows() && value.contains("\"") && !value.startsWith("\""))
        {
            return "\"" + value.replaceAll("\"", "\"\"") + "\"";
        }
        else
        {
            return value;
        }
    }

    private void shutdownBroker()
    {
        try
        {
            if (SystemUtils.isWindows())
            {
                doWindowsKill();
            }

            if (_process != null)
            {
                LOGGER.info("Destroying broker process");
                _process.destroy();

                reapChildProcess();
            }
        }
        finally
        {
            if (_executorService != null)
            {
                _executorService.shutdown();
                _executorService = null;
            }
            if (_ports != null)
            {
                _ports.clear();
                _ports = null;
            }
            _pid = null;
            _process = null;
            if (_currentWorkDirectory != null && Boolean.getBoolean("broker.clean.between.tests"))
            {
                if (FileUtils.delete(new File(_currentWorkDirectory), true))
                {
                    _currentWorkDirectory = null;
                }
            }
        }
    }

    private void doWindowsKill()
    {
        try
        {

            Process p;
            p = Runtime.getRuntime().exec(new String[]{"taskkill", "/PID", Integer.toString(_pid), "/T", "/F"});
            consumeAllOutput(p);
        }
        catch (IOException e)
        {
            LOGGER.error("Error whilst killing process " + _pid, e);
        }
    }

    private static void consumeAllOutput(Process p) throws IOException
    {
        try (InputStreamReader inputStreamReader = new InputStreamReader(p.getInputStream()))
        {
            try (BufferedReader reader = new BufferedReader(inputStreamReader))
            {
                String line;
                while ((line = reader.readLine()) != null)
                {
                    LOGGER.debug("Consuming output: {}", line);
                }
            }
        }
    }

    private void reapChildProcess()
    {
        try
        {
            _process.waitFor();
            LOGGER.info("broker exited: " + _process.exitValue());
        }
        catch (InterruptedException e)
        {
            LOGGER.error("Interrupted whilst waiting for process shutdown");
            Thread.currentThread().interrupt();
        }
        finally
        {
            try
            {
                _process.getInputStream().close();
                _process.getErrorStream().close();
                _process.getOutputStream().close();
            }
            catch (IOException ignored)
            {
            }
        }
    }

    private final class BrokerSystemOutputHandler implements Runnable
    {
        private final Logger LOGGER = LoggerFactory.getLogger(BrokerSystemOutputHandler.class);

        private final BufferedReader _in;
        private final List<ListeningPort> _amqpPorts;
        private final Pattern _readyPattern;
        private final Pattern _stoppedPattern;
        private final Pattern _pidPattern;
        private final Pattern _amqpPortPattern;
        private final CountDownLatch _readyLatch;

        private volatile boolean _seenReady;
        private volatile int _pid;

        private BrokerSystemOutputHandler(InputStream in, CountDownLatch readyLatch)
        {
            _amqpPorts = new ArrayList<>();
            _seenReady = false;
            _in = new BufferedReader(new InputStreamReader(in));

            _readyPattern = Pattern.compile("BRK-1004 : Qpid Broker Ready");
            _stoppedPattern = Pattern.compile("BRK-1005 : Stopped");
            _amqpPortPattern = Pattern.compile("BRK-1002 : Starting : Listening on (\\w*) port ([0-9]+)");
            _pidPattern = Pattern.compile("BRK-1017 : Process : PID : ([0-9]+)");
            _readyLatch = readyLatch;
        }

        @Override
        public void run()
        {
            try
            {
                String line;
                while ((line = _in.readLine()) != null)
                {
                    LOGGER.info(line);

                    if (!_seenReady)
                    {
                        checkPortListeningLog(line, _amqpPortPattern, _amqpPorts);

                        Matcher pidMatcher = _pidPattern.matcher(line);
                        if (pidMatcher.find())
                        {
                            if (pidMatcher.groupCount() > 0)
                            {
                                _pid = Integer.parseInt(pidMatcher.group(1));
                            }
                        }

                        Matcher readyMatcher = _readyPattern.matcher(line);
                        if (readyMatcher.find())
                        {
                            _seenReady = true;
                            _readyLatch.countDown();
                        }
                    }

                    Matcher stopMatcher = _stoppedPattern.matcher(line);
                    if (stopMatcher.find())
                    {
                        break;
                    }

                    if (line.contains("Error:"))
                    {
                        break;
                    }
                }
            }
            catch (IOException e)
            {
                LOGGER.warn(e.getMessage()
                            + " : Broker stream from unexpectedly closed; last log lines written by Broker may be lost.");
            }
        }

        private void checkPortListeningLog(final String line,
                                           final Pattern portPattern,
                                           final List<ListeningPort> ports)
        {
            Matcher portMatcher = portPattern.matcher(line);
            if (portMatcher.find())
            {
                ports.add(new ListeningPort(portMatcher.group(1),
                                            Integer.parseInt(portMatcher.group(2))));
            }
        }

        int getPID()
        {
            return _pid;
        }

        List<ListeningPort> getAmqpPorts()
        {
            return _amqpPorts;
        }
    }

    private static class ListeningPort
    {
        private String _transport;
        private int _port;

        ListeningPort(final String transport, final int port)
        {
            _transport = transport;
            _port = port;
        }

        String getTransport()
        {
            return _transport;
        }

        int getPort()
        {
            return _port;
        }

        @Override
        public String toString()
        {
            return "ListeningPort{" +
                   ", _transport='" + _transport + '\'' +
                   ", _port=" + _port +
                   '}';
        }
    }

    private interface AmqpManagementOperation<T>
    {
        T invoke(AmqpManagementFacade amqpManagementFacade, Session session) throws JMSException;

        default <V> AmqpManagementOperation<V> andThen(AmqpManagementOperation<V> after)
        {
            Objects.requireNonNull(after);

            return (amqpManagementFacade, session) -> {
                invoke(amqpManagementFacade, session);
                return after.invoke(amqpManagementFacade, session);
            };
        }
    }
}
