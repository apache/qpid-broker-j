/* Licensed to the Apache Software Foundation (ASF) under one
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
 */
package org.apache.qpid.client;

import static org.apache.qpid.transport.Option.NONE;
import static org.apache.qpid.transport.Option.SYNC;
import static org.apache.qpid.transport.Option.UNRELIABLE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.crypto.spec.SecretKeySpec;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQDestination.DestSyntax;
import org.apache.qpid.client.message.AMQMessageDelegate_0_10;
import org.apache.qpid.client.message.AbstractJMSMessage;
import org.apache.qpid.client.message.Encrypted010MessageFactory;
import org.apache.qpid.client.message.MessageEncryptionHelper;
import org.apache.qpid.client.message.QpidMessageProperties;
import org.apache.qpid.client.messaging.address.Link.Reliability;
import org.apache.qpid.client.util.JMSExceptionHelper;
import org.apache.qpid.transport.DeliveryProperties;
import org.apache.qpid.transport.Header;
import org.apache.qpid.transport.MessageAcceptMode;
import org.apache.qpid.transport.MessageAcquireMode;
import org.apache.qpid.transport.MessageDeliveryMode;
import org.apache.qpid.transport.MessageDeliveryPriority;
import org.apache.qpid.transport.MessageProperties;
import org.apache.qpid.transport.Option;
import org.apache.qpid.transport.codec.BBEncoder;
import org.apache.qpid.util.GZIPUtils;
import org.apache.qpid.util.Strings;

/**
 * This is a 0_10 message producer.
 */
public class BasicMessageProducer_0_10 extends BasicMessageProducer
{

    private static final Logger _logger = LoggerFactory.getLogger(BasicMessageProducer_0_10.class);
    private byte[] userIDBytes;

    BasicMessageProducer_0_10(AMQConnection connection, AMQDestination destination, boolean transacted, int channelId,
                              AMQSession session, long producerId, Boolean immediate, Boolean mandatory) throws
                                                                                                         QpidException
    {
        super(_logger, connection, destination, transacted, channelId, session, producerId, immediate, mandatory);
        
        userIDBytes = Strings.toUTF8(getUserID());
    }

    void declareDestination(AMQDestination destination) throws QpidException
    {
        if (destination.getDestSyntax() == DestSyntax.BURL)
        {
            if (getSession().isDeclareExchanges() && !getSession().isResolved(destination))
            {
                String name = destination.getExchangeName();
                ((AMQSession_0_10) getSession()).getQpidSession().exchangeDeclare
                    (name,
                     destination.getExchangeClass(),
                     null, null,
                     name.startsWith("amq.") ? Option.PASSIVE : Option.NONE,
                     destination.isExchangeDurable() ? Option.DURABLE : Option.NONE,
                     destination.isExchangeAutoDelete() ? Option.AUTO_DELETE : Option.NONE);

                getSession().setResolved(destination);
            }
        }
        else
        {       
            getSession().resolveAddress(destination,false,false);
            getSession().handleLinkCreation(destination);
            getSession().sync();
        }
    }

    //--- Overwritten methods

    /**
     * Sends a message to a given destination
     */
    void sendMessage(AMQDestination destination, Message origMessage, AbstractJMSMessage message,
                     UUID messageId, int deliveryMode, int priority, long timeToLive, boolean mandatory,
                     boolean immediate, final long deliveryDelay) throws JMSException
    {
        message.prepareForSending();

        AMQMessageDelegate_0_10 delegate = (AMQMessageDelegate_0_10) message.getDelegate();

        DeliveryProperties deliveryProp = delegate.getDeliveryProperties();
        MessageProperties messageProps = delegate.getMessageProperties();

        // On the receiving side, this will be read in to the JMSXUserID as well.
        if (getConnection().isPopulateUserId())
        {
            messageProps.setUserId(userIDBytes);
        }
                
        delegate.setJMSMessageID((String)null);
        if (messageId != null)
        {
            messageProps.setMessageId(messageId);
        }
        else if (messageProps.hasMessageId())
        {
            messageProps.clearMessageId();
        }

        long currentTime = 0;
        if (timeToLive > 0 || !isDisableTimestamps() || deliveryDelay != 0L)
        {
            currentTime = System.currentTimeMillis();
        }        
        
        if (timeToLive > 0)
        {
            deliveryProp.setTtl(timeToLive);
            message.setJMSExpiration(currentTime + timeToLive);
        }
        
        if (!isDisableTimestamps())
        {
            
            deliveryProp.setTimestamp(currentTime);            
            message.setJMSTimestamp(currentTime);
        }

        if (!deliveryProp.hasDeliveryMode() || deliveryProp.getDeliveryMode().getValue() != deliveryMode)
        {
            MessageDeliveryMode mode;
            switch (deliveryMode)
            {
            case DeliveryMode.PERSISTENT:
                mode = MessageDeliveryMode.PERSISTENT;
                break;
            case DeliveryMode.NON_PERSISTENT:
                mode = MessageDeliveryMode.NON_PERSISTENT;
                break;
            default:
                throw new IllegalArgumentException("illegal delivery mode: " + deliveryMode);
            }
            deliveryProp.setDeliveryMode(mode);
            message.setJMSDeliveryMode(deliveryMode);
        }
        if (!deliveryProp.hasPriority() || deliveryProp.getPriority().getValue() != priority)
        {
            deliveryProp.setPriority(MessageDeliveryPriority.get((short) priority));
            message.setJMSPriority(priority);
        }
        String exchangeName = destination.getExchangeName() == null ? "" : destination.getExchangeName();
        if ( deliveryProp.getExchange() == null || ! deliveryProp.getExchange().equals(exchangeName))
        {
            deliveryProp.setExchange(exchangeName);
        }
        String routingKey = destination.getRoutingKey();
        if (deliveryProp.getRoutingKey() == null || ! deliveryProp.getRoutingKey().equals(routingKey))
        {
            deliveryProp.setRoutingKey(routingKey);
        }
        
        Map<String,Object> appProps = messageProps.getApplicationHeaders();

        if (destination.getDestSyntax() == AMQDestination.DestSyntax.ADDR &&
           (destination.getSubject() != null ||
              (messageProps.getApplicationHeaders() != null && messageProps.getApplicationHeaders().get(QpidMessageProperties.QPID_SUBJECT) != null))
           )
        {
            if (appProps == null)
            {
                appProps = new HashMap<String,Object>();
                messageProps.setApplicationHeaders(appProps);          
            }
            
            if (appProps.get(QpidMessageProperties.QPID_SUBJECT) == null)
            {
                // use default subject in address string
                appProps.put(QpidMessageProperties.QPID_SUBJECT,destination.getSubject());
            }
                    
            if (destination.getAddressType() == AMQDestination.TOPIC_TYPE)
            {
                deliveryProp.setRoutingKey((String)
                        messageProps.getApplicationHeaders().get(QpidMessageProperties.QPID_SUBJECT));                
            }
        }


        if(deliveryDelay != 0L && (appProps == null || appProps.get(QpidMessageProperties.QPID_NOT_VALID_BEFORE) == null))
        {
            if (appProps == null)
            {
                appProps = new HashMap<String,Object>();
                messageProps.setApplicationHeaders(appProps);
            }

            appProps.put(QpidMessageProperties.QPID_NOT_VALID_BEFORE, deliveryDelay+currentTime);
        }

        ByteBuffer data = message.getData();
        boolean encrypt = message.getBooleanProperty(MessageEncryptionHelper.ENCRYPT_HEADER) || destination.sendEncrypted();
        if(encrypt)
        {
            MessageEncryptionHelper encryptionHelper = getSession().getMessageEncryptionHelper();
            try
            {
                MessageProperties origMessageProps = messageProps;
                DeliveryProperties origDeliveryProps = deliveryProp;
                messageProps = new MessageProperties(messageProps);
                deliveryProp = new DeliveryProperties(deliveryProp);
                SecretKeySpec secretKey = encryptionHelper.createSecretKey();

                final Map<String, Object> origApplicationHeaders = origMessageProps.getApplicationHeaders();
                if(origApplicationHeaders != null)
                {
                    origApplicationHeaders.remove(MessageEncryptionHelper.ENCRYPT_HEADER);
                }

                String recipientString = message.getStringProperty(MessageEncryptionHelper.ENCRYPT_RECIPIENTS_HEADER);
                if(recipientString == null)
                {
                    recipientString = destination.getEncryptedRecipients();
                }
                if(origApplicationHeaders != null)
                {
                    origApplicationHeaders.remove(MessageEncryptionHelper.ENCRYPT_RECIPIENTS_HEADER);
                }

                String unencryptedProperties = message.getStringProperty(MessageEncryptionHelper.UNENCRYPTED_PROPERTIES_HEADER);
                if(origApplicationHeaders != null)
                {
                    origApplicationHeaders.remove(MessageEncryptionHelper.UNENCRYPTED_PROPERTIES_HEADER);
                }

                BBEncoder encoder = new BBEncoder(1024);
                encoder.writeStruct32(origDeliveryProps);
                encoder.writeStruct32(origMessageProps);
                ByteBuffer buf = encoder.buffer();

                final int headerLength = buf.remaining();
                byte[] unencryptedBytes = new byte[headerLength + (data == null ? 0 : data.remaining())];

                buf.get(unencryptedBytes, 0, headerLength);

                if (data != null)
                {
                    data.get(unencryptedBytes, headerLength, data.remaining());
                }

                byte[] ivbytes = encryptionHelper.getInitialisationVector();

                byte[] encryptedBytes = encryptionHelper.encrypt(secretKey, unencryptedBytes, ivbytes);
                data = ByteBuffer.wrap(encryptedBytes);

                if (recipientString == null)
                {
                    throw new JMSException("When sending an encrypted message, recipients must be supplied");
                }
                String[] recipients = recipientString.split(";");
                List<List<Object>> encryptedKeys = new ArrayList<>();
                for(MessageEncryptionHelper.KeyTransportRecipientInfo info : encryptionHelper.getKeyTransportRecipientInfo(
                        Arrays.asList(recipients), secretKey))
                {
                    encryptedKeys.add(info.asList());
                }

                Map<String,Object>  newHeaders = messageProps.getApplicationHeaders();
                if(newHeaders != null)
                {
                    newHeaders.clear();
                }
                else
                {
                    newHeaders = new LinkedHashMap<>();
                    messageProps.setApplicationHeaders(newHeaders);
                }

                if(unencryptedProperties != null)
                {
                    List<String> unencryptedPropertyNames = Arrays.asList(unencryptedProperties.split(" *; *"));
                    for (String propertyName : unencryptedPropertyNames)
                    {
                        if (origApplicationHeaders.containsKey(propertyName))
                        {
                            newHeaders.put(propertyName, origApplicationHeaders.get(propertyName));
                        }
                    }
                }

                newHeaders.put(MessageEncryptionHelper.ENCRYPTED_KEYS_PROPERTY, encryptedKeys);
                newHeaders.put(MessageEncryptionHelper.ENCRYPTION_ALGORITHM_PROPERTY,
                               encryptionHelper.getMessageEncryptionCipherName());
                newHeaders.put(MessageEncryptionHelper.KEY_INIT_VECTOR_PROPERTY, ivbytes);
                messageProps.setContentType(Encrypted010MessageFactory.ENCRYPTED_0_10_CONTENT_TYPE);

            }
            catch (GeneralSecurityException | IOException e)
            {
                throw JMSExceptionHelper.chainJMSException(new JMSException("Unexpected Exception while encrypting message"), e);
            }

        }
        else
        {
            if (data != null
                && data.remaining() > getConnection().getMessageCompressionThresholdSize()
                && getConnection().getDelegate().isMessageCompressionSupported()
                && getConnection().isMessageCompressionDesired()
                && messageProps.getContentEncoding() == null)
            {
                byte[] compressed = GZIPUtils.compressBufferToArray(data);
                if (compressed != null)
                {
                    messageProps.setContentEncoding(GZIPUtils.GZIP_CONTENT_ENCODING);
                    data = ByteBuffer.wrap(compressed);
                }
            }
        }

        messageProps.setContentLength(data == null ? 0 : data.remaining());

        // send the message
        try
        {
            org.apache.qpid.transport.Session ssn = (org.apache.qpid.transport.Session)
                ((AMQSession_0_10) getSession()).getQpidSession();

            // if true, we need to sync the delivery of this message
            boolean sync = false;

            sync = ( (getPublishMode() == PublishMode.SYNC_PUBLISH_ALL) ||
                     (getPublishMode() == PublishMode.SYNC_PUBLISH_PERSISTENT &&
                         deliveryMode == DeliveryMode.PERSISTENT)
                   );  
            
            boolean unreliable = (destination.getDestSyntax() == DestSyntax.ADDR) &&
                                 (destination.getLink().getReliability() == Reliability.UNRELIABLE);
            

            ByteBuffer buffer = data == null ? ByteBuffer.allocate(0) : data.slice();
            
            ssn.messageTransfer(destination.getExchangeName() == null ? "" : destination.getExchangeName(),
                                MessageAcceptMode.NONE,
                                MessageAcquireMode.PRE_ACQUIRED,
                                new Header(deliveryProp, messageProps),
                    buffer, sync ? SYNC : NONE, unreliable ? UNRELIABLE : NONE);
            if (sync)
            {
                ssn.sync();
                ((AMQSession_0_10) getSession()).getCurrentException();
            }
            
        }
        catch (Exception e)
        {
            throw JMSExceptionHelper.chainJMSException(new JMSException("Exception when sending message:"
                                                                        + e.getMessage()), e);
        }
    }

    @Override
    public boolean isBound(AMQDestination destination) throws JMSException
    {
        return getSession().isQueueBound(destination);
    }
    
    // We should have a close and closed method to distinguish between normal close
    // and a close due to session or connection error.
    @Override
    public void close() throws JMSException
    {
        super.close();
    }

}

