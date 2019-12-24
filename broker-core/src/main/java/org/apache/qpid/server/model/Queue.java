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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.exchange.DestinationReferrer;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInfo;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.queue.CreatingLinkInfo;
import org.apache.qpid.server.queue.MessageGroupType;
import org.apache.qpid.server.queue.NotificationCheck;
import org.apache.qpid.server.queue.QueueConsumer;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.queue.QueueEntryIterator;
import org.apache.qpid.server.queue.QueueEntryVisitor;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.util.Deletable;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

@ManagedObject( defaultType = "standard",
        amqpName = "org.apache.qpid.Queue",
        description = Queue.CLASS_DESCRIPTION )
public interface Queue<X extends Queue<X>> extends ConfiguredObject<X>,
                                                   Comparable<X>, DestinationReferrer,
                                                   BaseQueue,
                                                   MessageSource,
                                                   MessageDestination,
                                                   Deletable<X>
{
    String CLASS_DESCRIPTION = "<p>Queues are named entities within a VirtualHost that hold/buffer messages for later "
                               + "delivery to consumer applications. Consumers subscribe to a queue in order to receive "
                               + "messages for it.</p>"
                               + "<p>The Broker supports different queue types, each with different delivery semantics. "
                               + "It also allows for messages on a queue to be treated as a group.</p>";

    String ALERT_REPEAT_GAP = "alertRepeatGap";
    String ALERT_THRESHOLD_MESSAGE_AGE = "alertThresholdMessageAge";
    String ALERT_THRESHOLD_MESSAGE_SIZE = "alertThresholdMessageSize";
    String ALERT_THRESHOLD_QUEUE_DEPTH_BYTES = "alertThresholdQueueDepthBytes";
    String ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES = "alertThresholdQueueDepthMessages";
    String ALTERNATE_BINDING = "alternateBinding";
    String EXCLUSIVE = "exclusive";
    String MESSAGE_DURABILITY = "messageDurability";
    String MESSAGE_GROUP_KEY_OVERRIDE = "messageGroupKeyOverride";
    String MESSAGE_GROUP_TYPE = "messageGroupType";
    String MESSAGE_GROUP_DEFAULT_GROUP = "messageGroupDefaultGroup";
    String MAXIMUM_DELIVERY_ATTEMPTS = "maximumDeliveryAttempts";
    String NO_LOCAL = "noLocal";
    String OWNER = "owner";
    String CREATING_LINK_INFO = "creatingLinkInfo";

    String QUEUE_FLOW_STOPPED = "queueFlowStopped";
    String MAXIMUM_MESSAGE_TTL = "maximumMessageTtl";
    String MINIMUM_MESSAGE_TTL = "minimumMessageTtl";
    String DEFAULT_FILTERS = "defaultFilters";
    String ENSURE_NONDESTRUCTIVE_CONSUMERS = "ensureNondestructiveConsumers";
    String HOLD_ON_PUBLISH_ENABLED = "holdOnPublishEnabled";
    String OVERFLOW_POLICY = "overflowPolicy";
    String MAXIMUM_QUEUE_DEPTH_MESSAGES = "maximumQueueDepthMessages";
    String MAXIMUM_QUEUE_DEPTH_BYTES = "maximumQueueDepthBytes";
    String EXPIRY_POLICY = "expiryPolicy";
    String MAXIMUM_LIVE_CONSUMERS = "maximumLiveConsumers";

    String QUEUE_SCAVANGE_COUNT = "qpid.queue.scavenge_count";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = QUEUE_SCAVANGE_COUNT)
    int DEFAULT_QUEUE_SCAVANGE_COUNT = 100;


    String MIME_TYPE_TO_FILE_EXTENSION = "qpid.mimeTypeToFileExtension";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = MIME_TYPE_TO_FILE_EXTENSION, description = "A mapping of MIME types to file extensions.")
    String DEFAULT_MIME_TYPE_TO_FILE_EXTENSION = "{\"application/json\":\".json\","
                                                 + "\"application/pdf\":\".pdf\","
                                                 + "\"application/xml\":\".xml\","
                                                 + "\"image/jpeg\":\".jpg\","
                                                 + "\"image/tiff\":\".tiff\","
                                                 + "\"text/plain\":\".txt\"}";

    String QUEUE_DEFAULT_ALTERNATE_BINDING = "queue.defaultAlternateBinding";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = QUEUE_DEFAULT_ALTERNATE_BINDING,
            description = "the ExclusivityPolicy to apply to queues where none is explicitly set")
    String DEFAULT_ALTERNATE_BINDING = "";

    @ManagedAttribute(defaultValue = "${queue.defaultAlternateBinding}", initialization = Initialization.materialize,
                      description = "Provides an alternate destination that will be used when the number of delivery "
                                    + "attempts exceeds the maximumDeliveryAttempt configured on this queue.  Messages "
                                    + "are also routed to this destination if this queue is deleted.")
    AlternateBinding getAlternateBinding();

    String QUEUE_DEFAULT_EXCLUSIVITY_POLICY = "queue.defaultExclusivityPolicy";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = QUEUE_DEFAULT_EXCLUSIVITY_POLICY,
            description = "the ExclusivityPolicy to apply to queues where none is explicitly set")
    String DEFAULT_EXCLUSIVITY = "NONE";

    @ManagedAttribute( defaultValue = "${queue.defaultExclusivityPolicy}")
    ExclusivityPolicy getExclusive();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.defaultEnsureNonDestructiveConsumers",
            description = "the value to use for the ensureNondestructiveCnsumers attribute of a queue where "
                          + "none is explicitly set")
    String DEFAULT_ENSURE_NON_DESTRUCTIVE_CONSUMERS = "false";

    String MESSAGE_CONVERSION_EXCEPTION_HANDLING_POLICY = "qpid.queue.messageConversion.exceptionHandlingPolicy";

    @ManagedContextDefault( name = MESSAGE_CONVERSION_EXCEPTION_HANDLING_POLICY,
            description = "The behaviour of consumer if it tries to consumer a messages that it cannot convert.")
    MessageConversionExceptionHandlingPolicy DEFAULT_MESSAGE_CONVERSION_EXCEPTION_HANDLING_POLICY = MessageConversionExceptionHandlingPolicy.REJECT;

    enum BehaviourOnUnknownDeclareArgument
    {
        IGNORE, LOG, FAIL
    }

    String UNKNOWN_QUEUE_DECLARE_ARGUMENT_BEHAVIOUR_NAME = "queue.behaviourOnUnknownDeclareArgument";
    @ManagedContextDefault(name= UNKNOWN_QUEUE_DECLARE_ARGUMENT_BEHAVIOUR_NAME)
    Queue.BehaviourOnUnknownDeclareArgument
            ON_UNKNOWN_QUEUE_DECLARE_OPTION = Queue.BehaviourOnUnknownDeclareArgument.FAIL;


    @SuppressWarnings("unused")
    @ManagedAttribute( defaultValue = "${queue.defaultEnsureNonDestructiveConsumers}" )
    boolean isEnsureNondestructiveConsumers();

    @DerivedAttribute( persist = true )
    String getOwner();

    @ManagedAttribute(immutable = true, description = "Information about the AMQP 1.0 Link that created this Queue if any.")
    CreatingLinkInfo getCreatingLinkInfo();

    @SuppressWarnings("unused")
    @ManagedAttribute
    boolean isNoLocal();

    @ManagedAttribute(description = "Specifies the name of the message header that provides the value for message "
                                    + "grouping purposes. If not set, for AMQP 1.0 the value of the group-id field "
                                    + "within the message properties is used. For AMQP 0-8..0-10, the value of the "
                                    + "message header JMSXGroupId is used.",
                      validValuePattern = "^(?!\\s*$).+")
    String getMessageGroupKeyOverride();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "qpid.broker_default-shared-message-group")
    String DEFAULT_SHARED_MESSAGE_GROUP = "qpid.no-group";

    @ManagedAttribute( defaultValue = "${qpid.broker_default-shared-message-group}",
                       description = "Fallback group used for messages without a group identifier. Used by SHARED_GROUPS only.",
                       validValuePattern = "^(?!\\s*$).+")
    String getMessageGroupDefaultGroup();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.maximumDistinctGroups")
    int DEFAULT_MAXIMUM_DISTINCT_GROUPS = 255;

    @ManagedAttribute( defaultValue = "${queue.maximumDistinctGroups}",
                       description = "Maximum number of distinct groups.  Used by STANDARD groups only")
    int getMaximumDistinctGroups();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.messageGroupType")
    MessageGroupType DEFAULT_MESSAGE_GROUP_TYPE = MessageGroupType.NONE;

    @ManagedAttribute( defaultValue = "${queue.messageGroupType}")
    MessageGroupType getMessageGroupType();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.maximumDeliveryAttempts")
    int DEFAULT_MAXIMUM_DELIVERY_ATTEMPTS = 0;

    @ManagedAttribute( defaultValue = "${queue.maximumDeliveryAttempts}")
    int getMaximumDeliveryAttempts();

    String QUEUE_FLOW_RESUME_LIMIT = "queue.queueFlowResumeLimit";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = QUEUE_FLOW_RESUME_LIMIT,
            description = "Percentage used to evaluate flow resume limit based on the values of attributes"
                          + " 'maximumQueueDepthBytes' and 'maximumQueueDepthMessages'.")
    double DEFAULT_FLOW_CONTROL_RESUME_LIMIT = 80.0;

    @SuppressWarnings("unused")
    @DerivedAttribute
    boolean isQueueFlowStopped();

    @Override
    @DerivedAttribute
    MessageConversionExceptionHandlingPolicy getMessageConversionExceptionHandlingPolicy();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.alertThresholdMessageAge")
    long DEFAULT_ALERT_THRESHOLD_MESSAGE_AGE = 0L;

    @ManagedAttribute( defaultValue = "${queue.alertThresholdMessageAge}")
    long getAlertThresholdMessageAge();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.alertThresholdMessageSize")
    long DEFAULT_ALERT_THRESHOLD_MESSAGE_SIZE = 0L;

    @ManagedAttribute( defaultValue = "${queue.alertThresholdMessageSize}")
    long getAlertThresholdMessageSize();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.alertThresholdQueueDepthBytes")
    long DEFAULT_ALERT_THRESHOLD_QUEUE_DEPTH = 0L;

    @ManagedAttribute( defaultValue = "${queue.alertThresholdQueueDepthBytes}")
    long getAlertThresholdQueueDepthBytes();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.alertThresholdQueueDepthMessages")
    long DEFAULT_ALERT_THRESHOLD_MESSAGE_COUNT = 0L;

    @ManagedAttribute( defaultValue = "${queue.alertThresholdQueueDepthMessages}")
    long getAlertThresholdQueueDepthMessages();


    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.alertRepeatGap")
    long DEFAULT_ALERT_REPEAT_GAP = 30000L;

    @ManagedAttribute( defaultValue = "${queue.alertRepeatGap}")
    long getAlertRepeatGap();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.defaultMessageDurability",
            description = "the value to use for the messageDurability attribute of a queue where "
                          + "none is explicitly set")
    String DEFAULT_MESSAGE_DURABILTY = "DEFAULT";

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.minimumMessageTtl",
            description = "the value to use for the minimumMessageTtl attribute of a queue where "
                          + "none is explicitly set")
    long DEFAULT_MINIMUM_MESSAGE_TTL = 0L;

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.maximumMessageTtl",
            description = "the value to use for the maximumMessageTtl attribute of a queue where "
                          + "none is explicitly set")
    long DEFAULT_MAXIMUM_MESSAGE_TTL = 0L;

    @Override
    @ManagedAttribute( defaultValue = "${queue.defaultMessageDurability}" )
    MessageDurability getMessageDurability();

    @SuppressWarnings("unused")
    @ManagedAttribute( defaultValue = "${queue.minimumMessageTtl}",
                       description = "Minimum message time to live (TTL) in ms. Messages arriving with smaller TTL"
                                    + " values will be overridden by this value")
    long getMinimumMessageTtl();

    @SuppressWarnings("unused")
    @ManagedAttribute( defaultValue = "${queue.maximumMessageTtl}",
                       description = "Maximum message time to live (TTL) in ms. Messages arriving with larger TTL"
                                    + " values (including those with no TTL, which are considered to have a TTL value of"
                                    + " infinity) will be overridden by this value.")
    long getMaximumMessageTtl();

    @SuppressWarnings("unused")
    @ManagedAttribute
    Map<String, Map<String,List<String>>> getDefaultFilters();


    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.holdOnPublishEnabled")
    boolean DEFAULT_HOLD_ON_PUBLISH_ENABLED = false;

    @ManagedAttribute( defaultValue = "${queue.holdOnPublishEnabled}",
                       description = "If true then entries in the queue will be held (not made available for delivery or "
                                     + "browsing) until the time (specified in milliseconds since the epoch) given in "
                                     + "the message header (AMQP 0-8,0-9,0-9-1,0-10) or message annotation (AMQP 1.0) "
                                     + "\"x-qpid-not-valid-before\".  Note that the actual time the entry is made "
                                     + "visible may depend on how frequently the virtual host housekeeping thread runs.")
    boolean isHoldOnPublishEnabled();

    @ManagedContextDefault(name = "queue.defaultMaximumQueueDepthMessages",
            description = "Maximum number of messages on queue allowed by overflow policy.")
    long DEFAULT_MAXIMUM_QUEUE_DEPTH_MESSAGES = -1;

    @ManagedAttribute(defaultValue = "${queue.defaultMaximumQueueDepthMessages}",
            description = "Maximum number of messages on queue allowed by overflow policy."
                          + " Negative value indicates that queue depth is unlimited. Default, -1.")
    long getMaximumQueueDepthMessages();

    @ManagedContextDefault(name = "queue.defaultMaximumQueueDepthBytes",
            description = "Maximum number of bytes on queue allowed by overflow policy." )
    long DEFAULT_MAXIMUM_QUEUE_DEPTH_BYTES = -1;

    @ManagedAttribute(defaultValue = "${queue.defaultMaximumQueueDepthBytes}",
            description = "Maximum number of bytes on queue allowed by overflow policy."
                          + "  Negative value indicates that queue depth is unlimited. Default, -1.")
    long getMaximumQueueDepthBytes();

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = "queue.defaultOverflowPolicy",
            description = "Specifies the default value for queue overflow policy. ")
    OverflowPolicy DEFAULT_OVERFLOW_POLICY = OverflowPolicy.NONE;

    @ManagedAttribute(defaultValue = "${queue.defaultOverflowPolicy}",
            description = "Queue overflow policy."
                          + " Options are ProducerFlowControl, Ring, FlowToDisk, Reject, and None."
                          + " The policy comes into effect where queue limits described by maximumQueueDepthBytes"
                          + " and/or maximumQueueDepthMessage are breached."
                          + " ProducerFlowControl - the producing sessions are blocked until queue size"
                          + " falls beneath resume threshold (see context variable queue.queueFlowResumeLimit)."
                          + " Ring - oldest messages are discarded."
                          + " Reject - incoming messages are rejected."
                          + " FlowToDisk - new incoming messages are written to disk and immediately evicted from memory."
                          + " None - queue capacity is unbounded.",
            mandatory = true)
    OverflowPolicy getOverflowPolicy();

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = "queue.defaultExpiryPolicy",
            description = "Specifies the default value for queue expiry policy. ")
    ExpiryPolicy DEFAULT_EXPIRY_POLICY = ExpiryPolicy.DELETE;

    @ManagedAttribute(defaultValue = "${queue.defaultExpiryPolicy}",
            description = "Queue expiry policy."
                          + " Options are Delete, and RouteToAlternate."
                          + " The policy comes into effect where a message on the queue has exceeded its time to live."
                          + " Delete - the expired message is deleted from the queue."
                          + " RouteToAlternate - new expired message is routed to the alternate destination for the"
                          + " queue, if present, or deleted if there is no alternate destination.",
            mandatory = true)
    ExpiryPolicy getExpiryPolicy();

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = "queue.defaultMaximumLiveConsumers",
            description = "Specifies the default value for maximum live consumers. ")
    int DEFAULT_MAXIMUM_LIVE_CONSUMERS = 0;

    @ManagedAttribute(defaultValue = "${queue.defaultMaximumLiveConsumers}",
            description = "Maximum live consumers. If the maximum number of live consumers is set to a number greater "
                          + "than zero, then consumers in excess of this limit are held in reserve and will only become "
                          + "eligible to receive messages if a live consumer is removed")
    int getMaximumLiveConsumers();


    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false, skipAclCheck = true)
    Collection<PublishingLink> getPublishingLinks();


    @Override
    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false, skipAclCheck = true)
    Collection<QueueConsumer<?,?>> getConsumers();

    //operations

    void visit(QueueEntryVisitor visitor);

    ListenableFuture<Integer> deleteAndReturnCountAsync();

    int deleteAndReturnCount();


    void setNotificationListener(QueueNotificationListener listener);

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Bindings",
                      description = "Current number of bindings to this queue.")
    int getBindingCount();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Consumers",
                      description = "Current number of consumers attached to this queue.")
    int getConsumerCount();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Consumers with credit",
                      description = "Current number of consumers attached to this queue with credit")
    int getConsumerCountWithCredit();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Delivered (Persistent)",
                      description = "Total size of all persistent messages delivered by this queue.")
    long getPersistentDequeuedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Delivered (Persistent)",
                      description = "Total number of persistent messages delivered by this queue.")
    long getPersistentDequeuedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Enqueued (Persistent)",
                      description = "Total size of all persistent messages received by this queue.")
    long getPersistentEnqueuedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Enqueued (Persistent)",
                      description = "Total number of persistent messages received by this queue.")
    long getPersistentEnqueuedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Queue Depth",
                      description = "Current size of all messages enqueued by this queue.")
    long getQueueDepthBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Queue Depth",
                      description = "Current number of messages enqueued by this queue.")
    int getQueueDepthMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Delivered",
                      description = "Total size of all messages delivered by this queue.")
    long getTotalDequeuedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Delivered",
                      description = "Total number of messages delivered by this queue.")
    long getTotalDequeuedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Enqueued",
                      description = "Total size of all messages received by this queue.")
    long getTotalEnqueuedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Enqueued",
                      description = "Total number of messages received by this queue.")
    long getTotalEnqueuedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Expired",
            description = "Total size of all messages expired by message time-to-live on this queue.")
    long getTotalExpiredBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Expired",
            description = "Total number of messages expired by message time-to-live on this queue.")
    long getTotalExpiredMessages();


    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Prefetched",
                      description = "Total size of all messages currently prefetched by consumers.")
    long getUnacknowledgedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Prefetched",
                      description = "Total number of messages currently prefetched by consumers.")
    long getUnacknowledgedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Available",
                      description = "Total size of all messages currently available.")
    long getAvailableBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Available",
                      description = "Total number of messages currently available.")
    int getAvailableMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Available HWM",
                      description = "Maximum recorded size of available messages.")
    long getAvailableBytesHighWatermark();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Available HWM",
                      description = "Maximum recorded number of available messages.")
    int getAvailableMessagesHighWatermark();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Queue Depth HWM",
                      description = "Maximum recorded size of enqueued messages.")
    long getQueueDepthBytesHighWatermark();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Queue Depth HWM",
                      description = "Maximum recorded number of enqueued messages.")
    int getQueueDepthMessagesHighWatermark();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.TIME_DURATION, label = "Oldest Message",
                      description = "Current age of oldest message on the queue.")
    long getOldestMessageAge();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Malformed",
            description = "Total size of enqueued malformed messages.")
    long getTotalMalformedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Malformed",
            description = "Total number of enqueued malformed messages.")
    long getTotalMalformedMessages();

    @ManagedOperation(description = "move messages from this queue to another", changesConfiguredObjectState = false)
    List<Long> moveMessages(@Param(name = "destination", description = "The queue to which the messages should be moved", mandatory = true) Queue<?> destination,
                            @Param(name = "messageIds", description = "If provided, only messages in the queue whose (internal) message-id is supplied will be considered for moving") List<Long> messageIds,
                            @Param(name = "selector", description = "A (JMS) selector - if provided, only messages which match the selector will be considered for moving") String selector,
                            @Param(name = "limit", description = "Maximum number of messages to move", defaultValue = "-1") int limit);


    @ManagedOperation(description = "copies messages from this queue to another", changesConfiguredObjectState = false)
    List<Long> copyMessages(@Param(name = "destination", description = "The queue to which the messages should be copied", mandatory = true) Queue<?> destination,
                            @Param(name = "messageIds", description = "If provided, only messages in the queue whose (internal) message-id is supplied will be considered for copying") List<Long> messageIds,
                            @Param(name = "selector", description = "A (JMS) selector - if provided, only messages which match the selector will be considered for copying")  String selector,
                            @Param(name = "limit", description = "Maximum number of messages to copy", defaultValue = "-1") int limit);


    @ManagedOperation(description = "removes messages from this queue", changesConfiguredObjectState = false)
    List<Long> deleteMessages(@Param(name = "messageIds", description = "If provided, only messages in the queue whose (internal) message-id is supplied will be considered for deletion") List<Long> messageIds,
                              @Param(name = "selector", description = "A (JMS) selector - if provided, only messages which match the selector will be considered for deletion") String selector,
                              @Param(name = "limit", description = "Maximum number of messages to delete", defaultValue = "-1") int limit);


    @ManagedOperation(description = "removes all messages from this queue", changesConfiguredObjectState = false)
    long clearQueue();

    @ManagedOperation(nonModifying = true, secure = true, changesConfiguredObjectState = false,
                      description = "Gets the message content")
    Content getMessageContent(@Param(name = "messageId") long messageId,
                              @Param(name = "limit", defaultValue = "-1",
                                      description = "Number of bytes to return") long limit,
                              @Param(name = "returnJson", defaultValue = "false",
                                      description = "If true, converts message content into JSON format. "
                                                    + "If false, returned bytes may be in an encoded form") boolean returnJson,
                              @Param(name = "decompressBeforeLimiting", defaultValue = "false",
                                      description = "If true, the operation will attempt to decompress the message"
                                                    + "(should it be compressed) before applying any limit. If"
                                                    + "decompression fails the operation will fail.") boolean decompressBeforeLimiting);

    @ManagedOperation(description = "get information about a range of messages",
            nonModifying = true,
            paramRequiringSecure = "includeHeaders",
            changesConfiguredObjectState = false)
    List<MessageInfo> getMessageInfo(@Param(name = "first", defaultValue = "-1") int first,
                                     @Param(name = "last",  defaultValue = "-1") int last,
                                     @Param(name = "includeHeaders", defaultValue = "false") boolean includeHeaders);

    @ManagedOperation(description = "get information about the message with the given Id",
            nonModifying = true,
            paramRequiringSecure = "includeHeaders",
            changesConfiguredObjectState = false)
    MessageInfo getMessageInfoById(@Param(name = "messageId") long messageId,
                                   @Param(name = "includeHeaders", defaultValue = "true") boolean includeHeaders);

    boolean isExclusive();

    LogSubject getLogSubject();

    QueueManagingVirtualHost<?> getVirtualHost();

    boolean isUnused();

    boolean isEmpty();

    long getOldestMessageArrivalTime();

    void requeue(QueueEntry entry);

    List<? extends QueueEntry> getMessagesOnTheQueue();

    QueueEntry getMessageOnTheQueue(long messageId);

    /**
     * Checks the status of messages on the queue, purging expired ones, firing age related alerts etc.
     */
    void checkMessageStatus();

    void reallocateMessages();

    Set<NotificationCheck> getNotificationChecks();

    Collection<String> getAvailableAttributes();

    void completeRecovery();

    void recover(ServerMessage<?> message, MessageEnqueueRecord enqueueRecord);

    boolean isHeld(QueueEntry queueEntry, final long evaluationTime);

    void checkCapacity();

    void deleteEntry(QueueEntry entry);

    QueueEntry getLeastSignificantOldestEntry();

    QueueEntryIterator queueEntryIterator();

    boolean checkValid(QueueEntry queueEntry);

    enum ExpiryPolicy
    {
        DELETE,
        ROUTE_TO_ALTERNATE
    }
}
