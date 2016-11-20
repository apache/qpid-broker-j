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
package org.apache.qpid.client.handler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.message.UnprocessedMessage_0_8;
import org.apache.qpid.client.protocol.AMQProtocolSession;
import org.apache.qpid.client.state.StateAwareMethodListener;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicDeliverBody;

public class BasicDeliverMethodHandler implements StateAwareMethodListener<BasicDeliverBody>
{
    private static final Logger _logger = LoggerFactory.getLogger(BasicDeliverMethodHandler.class);

    private static final ConcurrentMap<AMQShortString,String> CONSUMER_TAG_MAP = new ConcurrentHashMap<>();

    private static final BasicDeliverMethodHandler _instance = new BasicDeliverMethodHandler();

    public static BasicDeliverMethodHandler getInstance()
    {
        return _instance;
    }

    public void methodReceived(AMQProtocolSession session, BasicDeliverBody body, int channelId)
            throws QpidException
    {
        final UnprocessedMessage_0_8 msg = new UnprocessedMessage_0_8(
                body.getDeliveryTag(),
                getTagAsStringTag(body),
                body.getExchange(),
                body.getRoutingKey(),
                body.getRedelivered());
        if(_logger.isDebugEnabled())
        {
            _logger.debug("New JmsDeliver method received:" + session);
        }
        session.unprocessedMessageReceived(channelId, msg);
    }

    private static String getTagAsStringTag(final BasicDeliverBody body)
    {
        AMQShortString consumerTag = body.getConsumerTag();
        String tag = CONSUMER_TAG_MAP.get(consumerTag);
        if(tag == null)
        {
            tag = consumerTag.toString();
            CONSUMER_TAG_MAP.putIfAbsent(consumerTag, tag);
        }
        return tag;
    }
}
