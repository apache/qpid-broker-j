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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import org.apache.qpid.systests.end_to_end_conversion.client.Client;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientInstructions;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientResult;
import org.apache.qpid.systests.end_to_end_conversion.dependency_resolution.ClasspathQuery;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;


public class EndToEndConversionTestBase extends BrokerAdminUsingTestBase
{
    public static final String QUEUE_NAME = "queue";
    public static final int CLIENT_SOCKET_TIMEOUT = 30000;
    private static final Logger LOGGER = LoggerFactory.getLogger(EndToEndConversionTestBase.class);
    private static final Logger CLIENT_LOGGER = LoggerFactory.getLogger(Client.class);
    private static final int SERVER_SOCKET_TIMEOUT = 30000;
    private ListeningExecutorService _executorService =
            MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
    }

    @AfterClass
    public static void reportStats()
    {
        System.out.println("LQDEBUG: " + ClasspathQuery.getCacheStats());
    }

    protected ListenableFuture<?> runPublisher(final List<JmsInstructions> jmsInstructions)
    {
        List<String> gavs = Arrays.asList(System.getProperty("qpid.systests.end_to_end_conversion.publisherGavs",
                                                             "org.apache.qpid:qpid-jms-client:LATEST")
                                                .split(","));
        List<String> additionalJavaArgs = Arrays.stream(System.getProperty(
                "qpid.systests.end_to_end_conversion.publisherAdditionalJavaArguments",
                "").split(" ")).filter(s -> !s.isEmpty()).collect(Collectors.toList());

        return _executorService.submit(() -> {
            Thread.currentThread().setName("Publisher");
            runClient(gavs, additionalJavaArgs, jmsInstructions);
        });
    }

    protected ListenableFuture<?> runSubscriber(final List<JmsInstructions> jmsInstructions)
    {
        List<String> gavs = Arrays.asList(System.getProperty("qpid.systests.end_to_end_conversion.subscriberGavs",
                                                             "org.apache.qpid:qpid-client:LATEST,org.apache.geronimo.specs:geronimo-jms_1.1_spec:1.1.1")
                                                .split(","));

        List<String> additionalJavaArgs = Arrays.stream(System.getProperty(
                "qpid.systests.end_to_end_conversion.subscriberAdditionalJavaArguments",
                "-Dqpid.amqp.version=0-9-1").split(" ")).filter(s -> !s.isEmpty()).collect(Collectors.toList());

        return _executorService.submit(() -> {
            Thread.currentThread().setName("Subscriber");
            runClient(gavs, additionalJavaArgs, jmsInstructions);
        });
    }

    private ClientInstructions getClientInstructions(final List<JmsInstructions> jmsInstructions,
                                                     final boolean amqp0xClient)
    {
        ClientInstructions clientInstructions = new ClientInstructions();
        clientInstructions.setJmsInstructions(jmsInstructions);
        if (amqp0xClient)
        {
            clientInstructions.setContextFactory(getAmqp0xContextFactory());
            clientInstructions.setConnectionUrl(getAmqp0xConnectionUrl());
        }
        else
        {
            clientInstructions.setContextFactory(getAmqp10ContextFactory());
            clientInstructions.setConnectionUrl(getAmqp10ConnectionUrl());
        }
        clientInstructions.setQueueName(QUEUE_NAME);
        return clientInstructions;
    }

    private void runClient(final Collection<String> clientGavs,
                           final List<String> additionalJavaArguments,
                           final List<JmsInstructions> jmsInstructions)
    {
        final ClientInstructions clientInstructions = getClientInstructions(jmsInstructions, isAmqp0xClient(clientGavs));
        final ClasspathQuery classpathQuery = new ClasspathQuery(Client.class, clientGavs);

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
            Process p = processBuilder.start();
            try (final InputStream pInputStream = p.getInputStream();
                 final LoggingOutputStream loggingOutputStream = new LoggingOutputStream(CLIENT_LOGGER, Level.DEBUG))
            {
                final LoggingThread loggingThread = new LoggingThread(pInputStream, loggingOutputStream);
                loggingThread.start();
                LOGGER.debug("client process {} started", serverSocket.getLocalPort());

                try (final Socket clientSocket = serverSocket.accept();
                     final ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
                     final ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream()))
                {
                    LOGGER.debug("client process {} connected from port {}",
                                 clientSocket.getLocalPort(),
                                 clientSocket.getPort());
                    clientSocket.setSoTimeout(CLIENT_SOCKET_TIMEOUT);
                    outputStream.writeObject(clientInstructions);
                    final Object result = inputStream.readObject();
                    if (result instanceof ClientResult)
                    {
                        final ClientResult publisherResult = (ClientResult) result;
                        if (publisherResult.getException() != null)
                        {
                            throw publisherResult.getException();
                        }
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
                }
            }

            LOGGER.debug("client process {} finished exit value: {}", serverSocket.getLocalPort(), p.exitValue());
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
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

    private String getAmqp0xConnectionUrl()
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        int port = brokerAddress.getPort();
        String hostString = "localhost";
        return String.format("amqp://clientid/?brokerlist='tcp://%s:%d'", hostString, port);
    }

    private String getAmqp10ContextFactory()
    {
        return "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
    }

    private String getAmqp10ConnectionUrl()
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        int port = brokerAddress.getPort();
        String hostString = "localhost";
        int connectTimeout = 30000;
        return String.format("amqp://%s:%d?jms.connectTimeout=%d", hostString, port, connectTimeout);
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
