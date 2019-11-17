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

import org.apache.qpid.server.exchange.DestinationReferrer;
import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.queue.CreatingLinkInfo;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

@ManagedObject( description = Exchange.CLASS_DESCRIPTION,
        amqpName = "org.apache.qpid.Exchange"
        )
public interface Exchange<X extends Exchange<X>> extends ConfiguredObject<X>, MessageDestination,
                                                         DestinationReferrer, MessageSender
{
    String CLASS_DESCRIPTION = "<p>An Exchange is a named entity within the Virtualhost which receives messages from "
                               + "producers and routes them to matching Queues within the Virtualhost.</p>"
                               + "<p>The server provides a set of exchange types with each exchange type implementing "
                               + "a different routing algorithm.</p>";

    String ALTERNATE_BINDING = "alternateBinding";
    String DURABLE_BINDINGS = "durableBindings";
    String UNROUTABLE_MESSAGE_BEHAVIOUR = "unroutableMessageBehaviour";
    String CREATING_LINK_INFO = "creatingLinkInfo";

    enum UnroutableMessageBehaviour
    {
        REJECT, DISCARD
    }

    enum BehaviourOnUnknownDeclareArgument
    {
        IGNORE, LOG, FAIL
    }

    String UNKNOWN_EXCHANGE_DECLARE_ARGUMENT_BEHAVIOUR_NAME = "exchange.behaviourOnUnknownDeclareArgument";
    @ManagedContextDefault(name= UNKNOWN_EXCHANGE_DECLARE_ARGUMENT_BEHAVIOUR_NAME)
    BehaviourOnUnknownDeclareArgument
            ON_UNKNOWN_EXCHANGE_DECLARE_OPTION = BehaviourOnUnknownDeclareArgument.FAIL;


    // Attributes

    @ManagedAttribute(description = "Provides an alternate destination that, depending on behaviour requested by the "
                                    + "producer, may be used if a message arriving at this exchange cannot be routed "
                                    + "to at least one queue.")
    AlternateBinding getAlternateBinding();

    @ManagedAttribute(description = "(AMQP 1.0 only) Default behaviour to apply when a message is not routed to any queues", defaultValue = "DISCARD")
    UnroutableMessageBehaviour getUnroutableMessageBehaviour();

    @DerivedAttribute
    Collection<Binding> getBindings();

    @Override
    Collection<Binding> getPublishingLinks(MessageDestination destination);

    @DerivedAttribute(persist = true)
    Collection<Binding> getDurableBindings();

    @ManagedAttribute(immutable = true, description = "Information about the AMQP 1.0 Link that created this Exchange if any.")
    CreatingLinkInfo getCreatingLinkInfo();

    // Statistics
    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Bindings",
                      description = "Current number of bindings to this exchange.")
    long getBindingCount();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Dropped",
                      description = "Total size of all unroutable messages dropped by this exchange.")
    long getBytesDropped();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Inbound",
                      description = "Total size of messages received by this exchange.")
    long getBytesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Dropped",
                      description = "Number of unroutable messages dropped by this exchange.")
    long getMessagesDropped();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Inbound",
                      description = "Number of messages received by this exchange.")
    long getMessagesIn();


    @ManagedOperation(changesConfiguredObjectState = true,
                      description = "Bind a given destination to exchange using a given bindingKey and arguments."
                                    + " Existing binding arguments are replaced when replaceExistingArguments=true")
    boolean bind(@Param(name = "destination", mandatory = true) String destination,
                 @Param(name = "bindingKey", mandatory = true) String bindingKey,
                 @Param(name = "arguments", defaultValue = "{}") Map<String, Object> arguments,
                 @Param(name = "replaceExistingArguments", defaultValue = "false") boolean replaceExistingArguments);

    @ManagedOperation(changesConfiguredObjectState = true,
                      description = "Deletes the binding for a given destination with a given bindingKey")
    boolean unbind(@Param(name="destination", mandatory = true) String destination,
                   @Param(name="bindingKey", mandatory = true) String bindingKey);


    /**
     * @return true if the exchange will be deleted after all queues have been detached
     */
    boolean isAutoDelete();

    @DoOnConfigThread
    boolean addBinding(@Param(name = "bindingKey") String bindingKey,
                       @Param(name = "queue") Queue<?> queue,
                       @Param(name = "arguments") Map<String, Object> arguments) throws AMQInvalidArgumentException;

    @DoOnConfigThread
    boolean deleteBinding(@Param(name = "bindingKey") String bindingKey,
                          @Param(name = "queue") Queue<?> queue);

    @DoOnConfigThread
    boolean hasBinding(@Param(name = "bindingKey") String bindingKey,
                       @Param(name = "queue") Queue<?> queue);

    @DoOnConfigThread
    void replaceBinding(@Param(name = "bindingKey") String bindingKey,
                        @Param(name = "queue") Queue<?> queue,
                        @Param(name = "arguments") Map<String, Object> arguments) throws AMQInvalidArgumentException;

    QueueManagingVirtualHost<?> getVirtualHost();

    /**
     * Determines whether a message would be isBound to a particular queue using a specific routing key and arguments
     * @param bindingKey
     * @param arguments
     * @param queue
     * @return
     */
    @DoOnConfigThread
    boolean isBound(@Param(name = "bindingKey") String bindingKey,
                    @Param(name = "arguments") Map<String, Object> arguments,
                    @Param(name = "queue") Queue<?> queue);

    /**
     * Determines whether a message would be isBound to a particular queue using a specific routing key
     * @param bindingKey
     * @param queue
     * @return
     */
    @DoOnConfigThread
    boolean isBound(@Param(name = "bindingKey") String bindingKey,
                    @Param(name = "queue") Queue<?> queue);

    /**
     * Determines whether a message is routing to any queue using a specific _routing key
     * @param bindingKey
     * @return
     */
    @DoOnConfigThread
    boolean isBound(@Param(name = "bindingKey") String bindingKey);

    /**
     * Returns true if this exchange has at least one binding associated with it.
     * @return
     */
    @DoOnConfigThread
    boolean hasBindings();

    @DoOnConfigThread
    boolean isBound(@Param(name = "queue") Queue<?> queue);

    @DoOnConfigThread
    boolean isBound(@Param(name = "arguments") Map<String, Object> arguments);

    @DoOnConfigThread
    boolean isBound(@Param(name = "bindingKey") String bindingKey,
                    @Param(name = "arguments") Map<String, Object> arguments);

    @DoOnConfigThread
    boolean isBound(@Param(name = "arguments") Map<String, Object> arguments,
                    @Param(name = "queue") Queue<?> queue);

    EventLogger getEventLogger();

}
