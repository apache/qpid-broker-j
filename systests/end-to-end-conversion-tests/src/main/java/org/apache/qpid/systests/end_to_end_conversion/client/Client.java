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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.qpid.systests.end_to_end_conversion.EndToEndConversionTestBase;

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
            System.out.println(String.format("Connected to controller %d -> %d",
                                             socket.getLocalPort(),
                                             socket.getPort()));
            socket.setSoTimeout(EndToEndConversionTestBase.CLIENT_SOCKET_TIMEOUT);
            try
            {
                final Object o = inputStream.readObject();
                final List<ClientInstruction> instructions;

                if (o instanceof List && ((List<?>) o).stream().allMatch(item -> item instanceof ClientInstruction))
                {
                    instructions = (List<ClientInstruction>) o;
                }
                else
                {
                    throw new RuntimeException("Did not receive ClientInstructions");
                }
                System.out.println(String.format("Received instructions : %s", instructions.toString()));

                List<ClientMessage> clientMessages = new ArrayList<>();
                if (!instructions.isEmpty())
                {
                    String connectionUrl = null;
                    javax.naming.Context context = null;
                    Hashtable<Object, Object> env = new Hashtable<>();
                    for (int i = 0; i < instructions.size(); i++)
                    {
                        final ClientInstruction instruction = instructions.get(i);
                        if (instruction instanceof ConfigureJndiContext)
                        {
                            env.put(Context.INITIAL_CONTEXT_FACTORY,
                                    ((ConfigureJndiContext) instruction).getContextFactory());
                            connectionUrl = ((ConfigureJndiContext) instruction).getConnectionUrl();
                            env.put("connectionfactory.myFactoryLookup", connectionUrl);
                        }
                        else if (instruction instanceof ConfigureDestination)
                        {
                            env.putAll(((ConfigureDestination) instruction).getDestinations());
                        }
                        else
                        {
                            context = new InitialContext(env);
                            ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
                            System.out.println(String.format("Connecting to broker: %s", connectionUrl));
                            Connection connection = factory.createConnection();
                            try
                            {
                                connection.start();
                                List<ClientMessage> messages = handleInstructions(context,
                                                                                  connection,
                                                                                  instructions.subList(i,
                                                                                                       instructions
                                                                                                               .size()));
                                clientMessages.addAll(messages);
                            }
                            finally
                            {
                                connection.close();
                            }
                            break;
                        }
                    }
                }
                System.out.println("Finished successfully");
                objectOutputStream.writeObject(new ClientResult(clientMessages));
            }
            catch (VerificationException e)
            {
                final VerificationException serializableException = new VerificationException(stringifyStacktrace(e));
                objectOutputStream.writeObject(new ClientResult(serializableException));
            }
            catch (Exception e)
            {
                final String stringifiedStacktrace = stringifyStacktrace(e);
                System.out.println(stringifiedStacktrace);
                final RuntimeException serializableException =
                        new RuntimeException("Client failed with exception", new Exception(stringifiedStacktrace));
                objectOutputStream.writeObject(new ClientResult(serializableException));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            e.printStackTrace(System.out);
        }
        catch (Error e)
        {
            e.printStackTrace();
            e.printStackTrace(System.out);
            throw e;
        }
    }

    private String stringifyStacktrace(final Throwable e)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    private List<ClientMessage> handleInstructions(final Context context,
                                                   final Connection connection,
                                                   final List<ClientInstruction> instructions) throws Exception
    {
        List<ClientMessage> clientMessages = new ArrayList<>(instructions.size());
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            for (ClientInstruction instruction : instructions)
            {
                System.out.println(String.format("Process instruction: %s", instruction));
                final ClientMessage clientMessage;
                if (instruction instanceof MessagingInstruction.PublishMessage)
                {
                    final MessagingInstruction.PublishMessage publishInstruction =
                            (MessagingInstruction.PublishMessage) instruction;
                    clientMessage = publishMessage(context, session, publishInstruction);
                }
                else if (instruction instanceof MessagingInstruction.ReceiveMessage)
                {
                    final MessagingInstruction.ReceiveMessage receiveInstruction =
                            (MessagingInstruction.ReceiveMessage) instruction;
                    final Destination destination =
                            (Destination) context.lookup(receiveInstruction.getDestinationJndiName());
                    final MessageDescription messageDescription = receiveInstruction.getMessageDescription();
                    clientMessage = receiveMessage(session, destination, messageDescription);
                }
                else
                {
                    throw new RuntimeException(String.format("Unknown jmsInstruction class: '%s'",
                                                             instruction.getClass().getName()));
                }
                clientMessages.add(clientMessage);
            }
        }
        finally
        {
            session.close();
        }
        return clientMessages;
    }

    private ClientMessage receiveMessage(final Session session,
                                         final Destination queue,
                                         final MessageDescription messageDescription) throws Exception
    {
        final Message message;
        MessageConsumer consumer = session.createConsumer(queue);
        try
        {
            message = consumer.receive(RECEIVE_TIMEOUT);
            System.out.println(String.format("Received message: %s", message));
            MessageVerifier.verifyMessage(messageDescription, message);
        }
        finally
        {
            consumer.close();
        }

        if (message != null && message.getJMSReplyTo() != null)
        {
            System.out.println(String.format("Received message had replyTo: %s", message.getJMSReplyTo()));
            sendReply(session,
                      message.getJMSReplyTo(),
                      messageDescription.getHeader(MessageDescription.MessageHeader.CORRELATION_ID));
        }

        return buildClientMessage(message);
    }

    private ClientMessage publishMessage(final Context context,
                                         final Session session,
                                         final MessagingInstruction.PublishMessage publishMessageInstruction)
            throws Exception
    {
        final MessageDescription messageDescription = publishMessageInstruction.getMessageDescription();

        Message message = MessageCreator.fromMessageDescription(session, messageDescription);
        MessageConsumer replyToConsumer = null;
        try
        {
            if (messageDescription.getReplyToJndiName() != null)
            {
                final Destination replyToDestination, consumerReplyToDestination;
                final String replyToJndiName = messageDescription.getReplyToJndiName();
                if (replyToJndiName.equals(EndToEndConversionTestBase.TEMPORARY_QUEUE_JNDI_NAME))
                {
                    replyToDestination = session.createTemporaryQueue();
                }
                else
                {
                    replyToDestination = (Destination) context.lookup(replyToJndiName);
                }

                if (publishMessageInstruction.getConsumeReplyToJndiName() != null)
                {
                    consumerReplyToDestination =
                            (Destination) context.lookup(publishMessageInstruction.getConsumeReplyToJndiName());
                }
                else
                {
                    consumerReplyToDestination = replyToDestination;
                }

                message.setJMSReplyTo(replyToDestination);
                replyToConsumer = session.createConsumer(consumerReplyToDestination);
            }

            final Destination destination =
                    (Destination) context.lookup(publishMessageInstruction.getDestinationJndiName());
            MessageProducer messageProducer = session.createProducer(destination);
            try
            {
                messageProducer.send(message,
                                     messageDescription.getHeader(MessageDescription.MessageHeader.DELIVERY_MODE,
                                                                  DeliveryMode.NON_PERSISTENT),
                                     messageDescription.getHeader(MessageDescription.MessageHeader.PRIORITY,
                                                                  Message.DEFAULT_PRIORITY),
                                     messageDescription.getHeader(MessageDescription.MessageHeader.EXPIRATION,
                                                                  Message.DEFAULT_TIME_TO_LIVE));
                System.out.println(String.format("Sent message: %s", message));
            }
            finally
            {
                messageProducer.close();
            }

            if (replyToConsumer != null)
            {
                receiveReply(replyToConsumer,
                             messageDescription.getHeader(MessageDescription.MessageHeader.CORRELATION_ID));
            }
        }
        finally
        {
            if (replyToConsumer != null)
            {
                replyToConsumer.close();
            }
        }

        return buildClientMessage(message);

    }

    private void receiveReply(final MessageConsumer consumer, final Serializable expectedCorrelationId)
            throws Exception
    {
        final Message message = consumer.receive(RECEIVE_TIMEOUT);
        System.out.println(String.format("Received message: %s", message));
        if (expectedCorrelationId != null)
        {
            if (expectedCorrelationId instanceof byte[])
            {
                if (!Arrays.equals((byte[]) expectedCorrelationId, message.getJMSCorrelationIDAsBytes()))
                {
                    throw new VerificationException("ReplyTo message has unexpected correlationId.");
                }
            }
            else
            {
                if (!expectedCorrelationId.equals(message.getJMSCorrelationID()))
                {
                    throw new VerificationException("ReplyTo message has unexpected correlationId.");
                }
            }
        }
    }

    private void sendReply(final Session session, final Destination jmsReplyTo, final Serializable correlationId)
            throws JMSException
    {
        final Message replyToMessage = session.createMessage();
        if (correlationId != null)
        {
            if (correlationId instanceof byte[])
            {
                replyToMessage.setJMSCorrelationIDAsBytes((byte[]) correlationId);
            }
            else
            {
                replyToMessage.setJMSCorrelationID((String) correlationId);
            }
        }
        System.out.println(String.format("Sending reply message: %s", replyToMessage));
        MessageProducer producer = session.createProducer(jmsReplyTo);
        try
        {
            producer.send(replyToMessage);
        }
        finally
        {
            producer.close();
        }
    }

    private ClientMessage buildClientMessage(final Message message) throws JMSException
    {
        String jmsMessageID = message.getJMSMessageID();
        String jmsCorrelationID = message.getJMSCorrelationID();
        byte[] jmsCorrelationIDAsBytes;
        try
        {
            jmsCorrelationIDAsBytes = message.getJMSCorrelationIDAsBytes();
        }
        catch (JMSException e)
        {
            jmsCorrelationIDAsBytes = null;
        }
        long jmsTimestamp = message.getJMSTimestamp();
        int jmsDeliveryMode = message.getJMSDeliveryMode();
        boolean jmsRedelivered = message.getJMSRedelivered();
        String jmsType = message.getJMSType();
        long jmsExpiration = message.getJMSExpiration();
        int jmsPriority = message.getJMSPriority();

        return new JMSMessageAdaptor(jmsMessageID,
                                     jmsTimestamp,
                                     jmsCorrelationID,
                                     jmsCorrelationIDAsBytes,
                                     jmsDeliveryMode,
                                     jmsRedelivered,
                                     jmsType,
                                     jmsExpiration,
                                     jmsPriority);
    }

    private static class JMSMessageAdaptor implements ClientMessage
    {
        private final String _jmsMessageID;
        private final long _jmsTimestamp;
        private final String _jmsCorrelationID;
        private final byte[] _jmsCorrelationIDAsBytes;
        private final int _jmsDeliveryMode;
        private final boolean _jmsRedelivered;
        private final String _jmsType;
        private final long _jmsExpiration;
        private final int _jmsPriority;

        JMSMessageAdaptor(final String jmsMessageID,
                          final long jmsTimestamp,
                          final String jmsCorrelationID,
                          final byte[] jmsCorrelationIDAsBytes,
                          final int jmsDeliveryMode,
                          final boolean jmsRedelivered,
                          final String jmsType, final long jmsExpiration, final int jmsPriority)
        {
            _jmsMessageID = jmsMessageID;
            _jmsTimestamp = jmsTimestamp;
            _jmsCorrelationID = jmsCorrelationID;
            _jmsCorrelationIDAsBytes = jmsCorrelationIDAsBytes;
            _jmsDeliveryMode = jmsDeliveryMode;
            _jmsRedelivered = jmsRedelivered;
            _jmsType = jmsType;
            _jmsExpiration = jmsExpiration;
            _jmsPriority = jmsPriority;
        }

        @Override
        public String getJMSMessageID()
        {
            return _jmsMessageID;
        }

        @Override
        public long getJMSTimestamp()
        {
            return _jmsTimestamp;
        }

        @Override
        public String getJMSCorrelationID()
        {
            return _jmsCorrelationID;
        }

        @Override
        public byte[] getJMSCorrelationIDAsBytes()
        {
            return _jmsCorrelationIDAsBytes;
        }

        @Override
        public int getJMSDeliveryMode()
        {
            return _jmsDeliveryMode;
        }

        @Override
        public boolean getJMSRedelivered()
        {
            return _jmsRedelivered;
        }

        @Override
        public String getJMSType()
        {
            return _jmsType;
        }

        @Override
        public long getJMSExpiration()
        {
            return _jmsExpiration;
        }

        @Override
        public int getJMSPriority()
        {
            return _jmsPriority;
        }
    }
}
