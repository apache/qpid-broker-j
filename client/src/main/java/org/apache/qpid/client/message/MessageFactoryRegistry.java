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
package org.apache.qpid.client.message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.AMQSession_0_8;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.transport.DeliveryProperties;
import org.apache.qpid.transport.MessageProperties;
import org.apache.qpid.transport.MessageTransfer;

public class MessageFactoryRegistry
{
    /**
     * This class logger
     */
    private final Logger _logger = LoggerFactory.getLogger(getClass());

    private final Map<String, AbstractJMSMessageFactory> _mimeStringToFactoryMap = new HashMap<>();

    private final AbstractJMSMessageFactory _default = new JMSBytesMessageFactory();
    private final AMQSession<?, ?> _session;

    public MessageFactoryRegistry(final AMQSession<?, ?> session)
    {
        _session = session;
    }

    /**
     * Construct a new registry with the default message factories registered
     *
     * @return a message factory registry
     * @param session
     */
    public static MessageFactoryRegistry newDefaultRegistry(final AMQSession<?, ?> session)
    {
        MessageFactoryRegistry mf = new MessageFactoryRegistry(session);
        mf.registerFactory(JMSMapMessage.MIME_TYPE, new JMSMapMessageFactory());
        mf.registerFactory("text/plain", new JMSTextMessageFactory());
        mf.registerFactory("text/xml", new JMSTextMessageFactory());
        mf.registerFactory(JMSBytesMessage.MIME_TYPE, new JMSBytesMessageFactory());
        mf.registerFactory(JMSObjectMessage.MIME_TYPE, new JMSObjectMessageFactory(session.getAMQConnection()));
        mf.registerFactory(JMSStreamMessage.MIME_TYPE, new JMSStreamMessageFactory());
        mf.registerFactory(AMQPEncodedMapMessage.MIME_TYPE, new AMQPEncodedMapMessageFactory());
        mf.registerFactory(AMQPEncodedListMessage.MIME_TYPE, new AMQPEncodedListMessageFactory());

        mf.registerFactory(Encrypted091MessageFactory.ENCRYPTED_0_9_1_CONTENT_TYPE, new Encrypted091MessageFactory(mf));
        mf.registerFactory(Encrypted010MessageFactory.ENCRYPTED_0_10_CONTENT_TYPE, new Encrypted010MessageFactory(mf));

        mf.registerFactory(null, mf._default);
        return mf;
    }


    public void registerFactory(String mimeType, AbstractJMSMessageFactory mf)
    {
        _mimeStringToFactoryMap.put(mimeType, mf);
    }

    /**
     * Create a message. This looks up the MIME type from the content header and instantiates the appropriate
     * concrete message type.
     *
     *  @param deliveryTag   the AMQ message id
     * @param redelivered   true if redelivered
     * @param contentHeader the content header that was received
     * @param bodies        a list of ContentBody instances @return the message.
     * @param queueDestinationCache
     * @param topicDestinationCache
     * @param addressType
     * @throws JMSException
     * @throws QpidException
     */
    public AbstractJMSMessage createMessage(long deliveryTag,
                                            boolean redelivered,
                                            String exchange,
                                            String routingKey,
                                            ContentHeaderBody contentHeader,
                                            List bodies,
                                            AMQSession_0_8.DestinationCache<AMQQueue> queueDestinationCache,
                                            AMQSession_0_8.DestinationCache<AMQTopic> topicDestinationCache,
                                            final int addressType)
            throws QpidException, JMSException
    {
        BasicContentHeaderProperties properties = contentHeader.getProperties();

        // Get the message content type. This may be null for pure AMQP messages, but will always be set for JMS over
        // AMQP. When the type is null, it can only be assumed that the message is a byte message.
        AMQShortString contentTypeShortString = properties.getContentType();
        contentTypeShortString = (contentTypeShortString == null) ? new AMQShortString(JMSBytesMessage.MIME_TYPE) : contentTypeShortString;

        AbstractJMSMessageFactory mf = getMessageFactory(AMQShortString.toString(contentTypeShortString));

        return mf.createMessage(deliveryTag, redelivered, contentHeader, exchange, routingKey, bodies, queueDestinationCache, topicDestinationCache, addressType);
    }

    AbstractJMSMessageFactory getMessageFactory(final String contentTypeShortString)
    {
        AbstractJMSMessageFactory mf = _mimeStringToFactoryMap.get(contentTypeShortString);
        if (mf == null)
        {
            mf = _default;
        }
        return mf;
    }

    public AbstractJMSMessage createMessage(MessageTransfer transfer) throws QpidException, JMSException
    {

        MessageProperties mprop = transfer.getHeader().getMessageProperties();
        String messageType = "";
        if ( mprop == null || mprop.getContentType() == null)
        {
            _logger.debug("no message type specified, building a byte message");
            messageType = JMSBytesMessage.MIME_TYPE;
        }
        else
        {
           messageType = mprop.getContentType();
        }
        AbstractJMSMessageFactory mf = getMessageFactory(messageType);

        boolean redelivered = false;
        DeliveryProperties deliverProps;
        if((deliverProps = transfer.getHeader().getDeliveryProperties()) != null)
        {
            redelivered = deliverProps.getRedelivered();
        }
        return mf.createMessage(transfer.getId(),
                                redelivered,
                                mprop == null? new MessageProperties():mprop,
                                deliverProps == null? new DeliveryProperties():deliverProps,
                                transfer.getBody());
    }

    public AMQSession<?, ?> getSession()
    {
        return _session;
    }

    AbstractJMSMessageFactory getDefaultFactory()
    {
        return _default;
    }
}
