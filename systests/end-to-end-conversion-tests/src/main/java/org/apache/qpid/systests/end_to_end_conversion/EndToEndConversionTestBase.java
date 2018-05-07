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

package org.apache.qpid.systests.end_to_end_conversion;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.end_to_end_conversion.client.AugumentConnectionUrl;
import org.apache.qpid.systests.end_to_end_conversion.client.Client;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientInstruction;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientResult;
import org.apache.qpid.systests.end_to_end_conversion.client.ConfigureDestination;
import org.apache.qpid.systests.end_to_end_conversion.client.ConfigureJndiContext;
import org.apache.qpid.systests.end_to_end_conversion.dependency_resolution.ClasspathQuery;
import org.apache.qpid.systests.end_to_end_conversion.utils.LoggingOutputStream;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;


public class EndToEndConversionTestBase extends BrokerAdminUsingTestBase
{
    public static final String TEMPORARY_QUEUE_JNDI_NAME = "<TEMPORARY>";
    public static final int CLIENT_SOCKET_TIMEOUT = 30000;
    private static final int SERVER_SOCKET_TIMEOUT = 30000;
    private static final Logger LOGGER = LoggerFactory.getLogger(EndToEndConversionTestBase.class);
    private static final Logger CLIENT_LOGGER = LoggerFactory.getLogger(Client.class);
    private ListeningExecutorService _executorService;

    @Before
    public void setupExecutor()
    {
        _executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    }

    @After
    public void teardownExecutor() throws InterruptedException
    {
        if (_executorService != null)
        {
            _executorService.shutdown();
            if (!_executorService.awaitTermination(10, TimeUnit.SECONDS))
            {
                _executorService.shutdownNow();
            }
            _executorService = null;
        }
    }

    @AfterClass
    public static void reportStats()
    {
        System.out.println("LQDEBUG: " + ClasspathQuery.getCacheStats());
    }

    protected ListenableFuture<ClientResult> runPublisher(final List<ClientInstruction> clientInstructions)
    {
        List<String> gavs = Arrays.asList(System.getProperty("qpid.systests.end_to_end_conversion.publisherGavs",
                                                             "org.apache.qpid:qpid-jms-client:LATEST")
                                                .split(","));
        List<String> additionalJavaArgs = Arrays.stream(System.getProperty(
                "qpid.systests.end_to_end_conversion.publisherAdditionalJavaArguments",
                "").split(" ")).filter(s -> !s.isEmpty()).collect(Collectors.toList());

        return _executorService.submit(() -> {
            Thread.currentThread().setName("Publisher");
            return runClient(gavs, additionalJavaArgs, clientInstructions);
        });
    }

    protected ListenableFuture<ClientResult> runSubscriber(final List<ClientInstruction> clientInstructions)
    {
        List<String> gavs = Arrays.asList(System.getProperty("qpid.systests.end_to_end_conversion.subscriberGavs",
                                                             "org.apache.qpid:qpid-client:LATEST,org.apache.geronimo.specs:geronimo-jms_1.1_spec:1.1.1")
                                                .split(","));

        List<String> additionalJavaArgs = Arrays.stream(System.getProperty(
                "qpid.systests.end_to_end_conversion.subscriberAdditionalJavaArguments",
                "-Dqpid.amqp.version=0-9-1").split(" ")).filter(s -> !s.isEmpty()).collect(Collectors.toList());

        return _executorService.submit(() -> {
            Thread.currentThread().setName("Subscriber");
            return runClient(gavs, additionalJavaArgs, clientInstructions);
        });
    }

    private List<ClientInstruction> amendClientInstructions(List<ClientInstruction> clientInstructions,
                                                            final boolean amqp0xClient)
    {
        if (clientInstructions.isEmpty())
        {
            LOGGER.warn("client instructions are empty!");
        }
        else
        {
            if (!(clientInstructions.get(0) instanceof ConfigureDestination))
            {
                LOGGER.warn(String.format("first client instructions should be a 'ConfigureDestination' but is '%s'!",
                                          clientInstructions.get(0).getClass().getSimpleName()));
            }
            if (clientInstructions.stream().filter(item -> item instanceof ConfigureJndiContext).count() != 0)
            {
                LOGGER.warn("Test should not set a 'ConfigureContext' client instruction!"
                            + " This is set by the base class.");
            }
        }

        List<AugumentConnectionUrl> configUrls = clientInstructions.stream()
                                                                   .filter(AugumentConnectionUrl.class::isInstance)
                                                                   .map(AugumentConnectionUrl.class::cast)
                                                                   .collect(Collectors.toList());

        final String contextFactory;
        final String connectionUrl;
        if (amqp0xClient)
        {
            contextFactory = getAmqp0xContextFactory();
            connectionUrl = getAmqp0xConnectionUrl(configUrls);
        }
        else
        {
            contextFactory = getAmqp10ContextFactory();
            connectionUrl = getAmqp10ConnectionUrl(configUrls);
        }

        clientInstructions = new ArrayList<>(clientInstructions);
        clientInstructions.removeAll(configUrls);

        ConfigureJndiContext jndiContext = new ConfigureJndiContext(contextFactory, connectionUrl);
        List<ClientInstruction> instructions = new ArrayList<>();
        instructions.add(jndiContext);
        instructions.addAll(clientInstructions);
        return instructions;
    }

    protected Protocol getPublisherProtocolVersion()
    {
        final String publisherGavs = System.getProperty("qpid.systests.end_to_end_conversion.publisherGavs",
                                                        "org.apache.qpid:qpid-jms-client:LATEST");
        final String additionalArgs =
                System.getProperty("qpid.systests.end_to_end_conversion.publisherAdditionalJavaArguments", "");
        return getClientProtocolVersion(publisherGavs, additionalArgs);
    }

    protected Protocol getSubscriberProtocolVersion()
    {
        final String subscriberGavs = System.getProperty("qpid.systests.end_to_end_conversion.subscriberGavs",
                                                        "org.apache.qpid:qpid-client:LATEST,org.apache.geronimo.specs:geronimo-jms_1.1_spec:1.1.1");
        final String additionalArgs = System.getProperty(
                "qpid.systests.end_to_end_conversion.subscriberAdditionalJavaArguments",
                "-Dqpid.amqp.version=0-9-1");
        return getClientProtocolVersion(subscriberGavs, additionalArgs);
    }

    private Protocol getClientProtocolVersion(final String publisherGavs, final String additionalArgs)
    {
        if (publisherGavs.contains("org.apache.qpid:qpid-jms-client"))
        {
            return Protocol.AMQP_1_0;
        }
        else
        {
            if (additionalArgs.contains("0-10"))
            {
                return Protocol.AMQP_0_10;
            }
            else if (additionalArgs.contains("0-9-1"))
            {
                return Protocol.AMQP_0_9_1;
            }
            else if (additionalArgs.contains("0-9"))
            {
                return Protocol.AMQP_0_9;
            }
            else if (additionalArgs.contains("0-8"))
            {
                return Protocol.AMQP_0_8;
            }
        }
        throw new RuntimeException(String.format("Unable to determine client protocol version. Addition args are : "
                                                 + "[%s]", additionalArgs));
    }

    private ClientResult runClient(final Collection<String> clientGavs,
                                   final List<String> additionalJavaArguments,
                                   final List<ClientInstruction> jmsInstructions)
    {
        final List<ClientInstruction> clientInstructions = amendClientInstructions(jmsInstructions,
                                                                                   isAmqp0xClient(clientGavs));
        final ClasspathQuery classpathQuery = new ClasspathQuery(Client.class, clientGavs);

        LOGGER.debug("starting server socket");
        try (final ServerSocket serverSocket = new ServerSocket(0))
        {
            serverSocket.setSoTimeout(SERVER_SOCKET_TIMEOUT);
            String classPath = classpathQuery.getClasspath();
            final List<String> arguments = Lists.newArrayList("java", "-showversion",
                                                              "-cp", classPath);
            arguments.addAll(additionalJavaArguments);
            arguments.add(classpathQuery.getClientClass().getName());
            arguments.add(String.valueOf(serverSocket.getLocalPort()));

            LOGGER.debug("starting client process with arguments: {}", arguments);
            ProcessBuilder processBuilder = new ProcessBuilder(arguments);
            processBuilder.environment().put("PN_TRACE_FRM", "true");
            Process p = processBuilder.start();
            try (final InputStream pInputStream = p.getInputStream();
                 final LoggingOutputStream loggingOutputStream = new LoggingOutputStream(CLIENT_LOGGER, Level.DEBUG))
            {
                final LoggingThread loggingThread = new LoggingThread(pInputStream, loggingOutputStream);
                loggingThread.start();
                LOGGER.debug("client process started listening on port {}", serverSocket.getLocalPort());

                try (final Socket clientSocket = serverSocket.accept();
                     final ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
                     final ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream()))
                {
                    LOGGER.debug("client process connected from port {}", clientSocket.getPort());
                    clientSocket.setSoTimeout(CLIENT_SOCKET_TIMEOUT);
                    outputStream.writeObject(clientInstructions);
                    final Object result = inputStream.readObject();
                    if (result instanceof ClientResult)
                    {
                        final ClientResult clientResult = (ClientResult) result;
                        if (clientResult.getException() != null)
                        {
                            throw clientResult.getException();
                        }
                        return clientResult;
                    }
                    else
                    {
                        throw new RuntimeException("did not receive client results");
                    }
                }
                finally
                {
                    loggingThread.flush();
                    p.waitFor();
                    loggingThread.flush();
                    loggingThread.stop();
                    LOGGER.debug("client process finished exit value: {}", p.exitValue());
                }
            }
        }
        catch (RuntimeException e)
        {
            LOGGER.debug("client process finished with exception: {}", e);
            throw e;
        }
        catch (Exception e)
        {
            LOGGER.error("client process finished with exception: {}", e);
            throw new RuntimeException(e);
        }
        finally
        {
            LOGGER.debug("execution of runClient finished!");
        }
    }

    private String getAmqp0xContextFactory()
    {
        return "org.apache.qpid.jndi.PropertiesFileInitialContextFactory";
    }

    private String getAmqp0xConnectionUrl(final List<AugumentConnectionUrl> configUrls)
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        int port = brokerAddress.getPort();
        String hostString = "localhost";

        if (!configUrls.isEmpty())
        {
            for (final AugumentConnectionUrl configUrl : configUrls)
            {
                if (!configUrl.getConnectionUrlConfig().isEmpty())
                {
                    throw new UnsupportedOperationException("Not implemented");
                }
            }
        }
        return String.format("amqp://clientid/?brokerlist='tcp://%s:%d'", hostString, port);
    }

    private String getAmqp10ContextFactory()
    {
        return "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
    }

    private String getAmqp10ConnectionUrl(final List<AugumentConnectionUrl> configUrls)
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        int port = brokerAddress.getPort();
        String hostString = "localhost";
        int connectTimeout = 30000;

        String additional = configUrls.stream()
                                      .map(i -> i.getConnectionUrlConfig().entrySet())
                                      .flatMap(Collection::stream)
                                      .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                                      .collect(Collectors.joining("&", "&", ""));

        return String.format("amqp://%s:%d?jms.connectTimeout=%d%s", hostString, port, connectTimeout, additional);
    }

    private boolean isAmqp0xClient(final Collection<String> gavs)
    {
        for (String gav : gavs)
        {
            if (gav.contains("qpid-client"))
            {
                return true;
            }
        }
        return false;
    }

    private static class LoggingThread
    {
        private static final int BUFFER_SIZE = 2048;
        private final InputStream _in;
        private final OutputStream _out;
        private final Thread _loggingThread;

        private volatile boolean _stop;

        public LoggingThread(final InputStream in, final OutputStream out)
        {
            _in = in;
            _out = out;
            _stop = false;
            _loggingThread = new Thread(this::run);
            _loggingThread.setName(Thread.currentThread().getName() + "-Client");
        }

        public void start()
        {
            _loggingThread.start();
        }

        public void stop()
        {
            _stop = true;
            _loggingThread.interrupt();
        }

        private void run()
        {
            try
            {
                final byte[] buffer = new byte[BUFFER_SIZE];
                while (!_stop)
                {
                    final int read = _in.read(buffer);
                    if (read == -1)
                    {
                        break;
                    }
                    _out.write(buffer, 0, read);
                }
                _out.flush();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        public void flush()
        {
            try
            {
                _loggingThread.join(CLIENT_SOCKET_TIMEOUT);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}
