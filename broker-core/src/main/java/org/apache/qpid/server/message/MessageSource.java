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
package org.apache.qpid.server.message;

import java.util.Collection;
import java.util.EnumSet;

import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.TransactionLogResource;

public interface MessageSource extends TransactionLogResource, MessageNode
{
    <T extends ConsumerTarget<T>> MessageInstanceConsumer<T> addConsumer(T target, FilterManager filters,
                                                                      Class<? extends ServerMessage> messageClass,
                                                                      String consumerName,
                                                                      EnumSet<ConsumerOption> options,
                                                                      Integer priority)
            throws ExistingExclusiveConsumer, ExistingConsumerPreventsExclusive,
                   ConsumerAccessRefused, QueueDeleted;

    Collection<? extends MessageInstanceConsumer> getConsumers();

    boolean verifySessionAccess(AMQPSession<?,?> session);

    void close();
    /**
     * ExistingExclusiveConsumer signals a failure to create a consumer, because an exclusive consumer
     * already exists.
     * <p>
     * TODO Move to top level, used outside this class.
     */
    static final class ExistingExclusiveConsumer extends Exception
    {

        public ExistingExclusiveConsumer()
        {
        }
    }

    /**
     * QueueDeleted signals a failure to create a consumer, because the queue has been deleted.
     * <p>
     * TODO Move to top level, used outside this class.
     */
    final class QueueDeleted extends Exception
    {

        public QueueDeleted()
        {
        }
    }

    /**
     * ExistingConsumerPreventsExclusive signals a failure to create an exclusive consumer, as a consumer
     * already exists.
     * <p>
     * TODO Move to top level, used outside this class.
     */
    static final class ExistingConsumerPreventsExclusive extends Exception
    {
        public ExistingConsumerPreventsExclusive()
        {
        }
    }

    static final class ConsumerAccessRefused extends Exception
    {
        public ConsumerAccessRefused()
        {
        }
    }

    enum MessageConversionExceptionHandlingPolicy
    {
        CLOSE,
        ROUTE_TO_ALTERNATE,
        REJECT
    }

    MessageConversionExceptionHandlingPolicy getMessageConversionExceptionHandlingPolicy();
}
