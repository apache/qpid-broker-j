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
package org.apache.qpid.server.model;

import java.util.Collection;
import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.exchange.ExchangeReferrer;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.MessageDestination;

@ManagedObject( description = Exchange.CLASS_DESCRIPTION,
        amqpName = "org.apache.qpid.Exchange"
        )
public interface Exchange<X extends Exchange<X>> extends ConfiguredObject<X>, MessageDestination,
                                                         ExchangeReferrer
{
    String CLASS_DESCRIPTION = "<p>An Exchange is a named entity within the Virtualhost which receives messages from "
                               + "producers and routes them to matching Queues within the Virtualhost.</p>"
                               + "<p>The server provides a set of exchange types with each exchange type implementing "
                               + "a different routing algorithm.</p>";

    String ALTERNATE_EXCHANGE                   = "alternateExchange";

    enum UnroutableMessageBehaviour
    {
        REJECT, DISCARD
    }

    // Attributes

    @ManagedAttribute
    Exchange<?> getAlternateExchange();

    @ManagedAttribute(description = "(AMQP 1.0 only) Default behaviour to apply when a message is not routed to any queues", defaultValue = "DISCARD")
    UnroutableMessageBehaviour getUnroutableMessageBehaviour();

    //children
    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Collection<Binding<?>> getBindings();

    // Statistics
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Bindings")
    long getBindingCount();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Dropped")
    long getBytesDropped();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Inbound")
    long getBytesIn();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Dropped")
    long getMessagesDropped();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Inbound")
    long getMessagesIn();


    @ManagedOperation(changesConfiguredObjectState = true)
    void bind(@Param(name="queue", mandatory = true) Queue<?> queue,
              @Param(name="bindingKey") String bindingKey,
              @Param(name="arguments", defaultValue = "{}") Map<String,Object> arguments);

    /**
     * @return true if the exchange will be deleted after all queues have been detached
     */
    boolean isAutoDelete();

    boolean addBinding(String bindingKey, Queue<?> queue, Map<String, Object> arguments);

    boolean deleteBinding(String bindingKey, Queue<?> queue);

    boolean hasBinding(String bindingKey, Queue<?> queue);

    boolean replaceBinding(String bindingKey,
                           Queue<?> queue,
                           Map<String, Object> arguments);

    /**
     * Determines whether a message would be isBound to a particular queue using a specific routing key and arguments
     * @param bindingKey
     * @param arguments
     * @param queue
     * @return
     */

    boolean isBound(String bindingKey, Map<String, Object> arguments, Queue<?> queue);

    /**
     * Determines whether a message would be isBound to a particular queue using a specific routing key
     * @param bindingKey
     * @param queue
     * @return
     */

    boolean isBound(String bindingKey, Queue<?> queue);

    /**
     * Determines whether a message is routing to any queue using a specific _routing key
     * @param bindingKey
     * @return
     */
    boolean isBound(String bindingKey);

    /**
     * Returns true if this exchange has at least one binding associated with it.
     * @return
     */
    boolean hasBindings();

    boolean isBound(Queue<?> queue);

    boolean isBound(Map<String, Object> arguments);

    boolean isBound(String bindingKey, Map<String, Object> arguments);

    boolean isBound(Map<String, Object> arguments, Queue<?> queue);

    void removeReference(ExchangeReferrer exchange);

    void addReference(ExchangeReferrer exchange);

    boolean hasReferrers();

    ListenableFuture<Void> removeBindingAsync(Binding<?> binding);

    EventLogger getEventLogger();

    void addBinding(Binding<?> binding);
}
