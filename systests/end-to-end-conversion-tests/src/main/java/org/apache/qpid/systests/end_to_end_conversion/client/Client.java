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

package org.apache.qpid.systests.end_to_end_conversion.client;

import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.qpid.systests.end_to_end_conversion.EndToEndConversionTestBase;
import org.apache.qpid.systests.end_to_end_conversion.JmsInstructions;

public class Client
{
    private static final long RECEIVE_TIMEOUT = 30000L;

    public static void main(String... args)
    {
        new Client().start(args);
    }

    private void start(String... args)
    {
        System.out.println(String.format("Client started with classpath: %s", System.getProperty("java.class.path")));
        System.out.println(String.format("Client started with args: %s", Arrays.asList(args)));

        int controllerPort = Integer.parseInt(args[0]);

        try (final Socket socket = new Socket("localhost", controllerPort);
             final ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
             final ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());)
        {
            System.out.println(String.format("Connected to controller %d -> %d", socket.getLocalPort(), socket.getPort()));
            socket.setSoTimeout(EndToEndConversionTestBase.CLIENT_SOCKET_TIMEOUT);
            try
            {
                final Object o = inputStream.readObject();
                final ClientInstructions instructions;
                if (o instanceof ClientInstructions)
                {
                    instructions = (ClientInstructions) o;
                }
                else
                {
                    throw new RuntimeException("Did not receive ClientInstructions");
                }
                System.out.println(String.format("Received instructions : %s", instructions.toString()));

                String contextFactory = instructions.getContextFactory();
                String connectionUrl = instructions.getConnectionUrl();
                String queueName = instructions.getQueueName();

                Connection connection = null;
                try
                {
                    Hashtable<Object, Object> env = new Hashtable<>();
                    env.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
                    env.put("connectionfactory.myFactoryLookup", connectionUrl);
                    env.put("queue.myQueueLookup", queueName);

                    javax.naming.Context context = new InitialContext(env);

                    ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
                    Destination queue = (Destination) context.lookup("myQueueLookup");

                    System.out.println(String.format("Connecting to broker: %s", connectionUrl));
                    connection = factory.createConnection();

                    handleInstructions(connection, queue, instructions.getJmsInstructions());
                }
                finally
                {
                    if (connection != null)
                    {
                        connection.close();
                    }
                }
                System.out.println("Finished successfully");
                objectOutputStream.writeObject(new ClientResult());
            }
            catch (Exception e)
            {
                System.out.println("Encountered exception: " + e.getMessage());
                try (OutputStream baos = new ByteArrayOutputStream();
                     ObjectOutputStream oos = new ObjectOutputStream(baos))
                {
                    oos.writeObject(e);
                    objectOutputStream.writeObject(new ClientResult(e));
                }
                catch (NotSerializableException nse)
                {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    final RuntimeException serializableException = new RuntimeException(
                            "Client failed with non-serializable exception",
                            new Exception(sw.toString()));
                    objectOutputStream.writeObject(new ClientResult(serializableException));
                }
            }
        }
        catch (Exception e)
        {
            System.out.println("Encountered exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleInstructions(final Connection connection,
                                    final Destination queue,
                                    final List<JmsInstructions> jmsInstructions) throws Exception
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            for (JmsInstructions jmsInstruction : jmsInstructions)
            {
                System.out.println(String.format("Process instruction: %s", jmsInstruction));
                if (jmsInstruction instanceof JmsInstructions.PublishMessage)
                {
                    publishMessage(session, queue, jmsInstruction.getMessageDescription());
                }
                else if (jmsInstruction instanceof JmsInstructions.ReceiveMessage)
                {
                    connection.start();
                    receiveMessage(session, queue, jmsInstruction.getMessageDescription());
                }
                else if (jmsInstruction instanceof JmsInstructions.ReplyToMessage)
                {
                    throw new RuntimeException("ReplyTo is not implemented, yet.");
                }
                else
                {
                    throw new RuntimeException(String.format("Unknown jmsInstruction class: '%s'",
                                                             jmsInstruction.getClass().getName()));
                }
            }
        }
        finally
        {
            session.close();
        }
    }

    private void receiveMessage(final Session session,
                                final Destination queue,
                                final JmsInstructions.MessageDescription messageDescription) throws Exception
    {
        MessageConsumer consumer = session.createConsumer(queue);

        final Message message = consumer.receive(RECEIVE_TIMEOUT);
        MessageVerifier.verifyMessage(messageDescription, message);
        System.out.println(String.format("Received message: %s", message));
    }


    private void publishMessage(final Session session,
                                final Destination queue,
                                final JmsInstructions.MessageDescription messageDescription)
            throws Exception
    {
        MessageProducer messageProducer = session.createProducer(queue);
        try
        {
            Message message = MessageCreator.fromMessageDescription(session, messageDescription);
            messageProducer.send(message,
                                 messageDescription.getHeader(JmsInstructions.MessageDescription.MessageHeader.DELIVERY_MODE,
                                                              DeliveryMode.NON_PERSISTENT),
                                 messageDescription.getHeader(JmsInstructions.MessageDescription.MessageHeader.PRIORITY,
                                                              Message.DEFAULT_PRIORITY),
                                 messageDescription.getHeader(JmsInstructions.MessageDescription.MessageHeader.EXPIRATION,
                                                              Message.DEFAULT_TIME_TO_LIVE));
            System.out.println(String.format("Sent message: %s", message));
        }
        finally
        {
            messageProducer.close();
        }
    }
}
