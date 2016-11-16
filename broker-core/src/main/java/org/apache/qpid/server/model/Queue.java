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

import org.apache.qpid.server.exchange.ExchangeReferrer;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInfo;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.protocol.CapacityChecker;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.queue.NotificationCheck;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.queue.QueueEntryVisitor;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.util.Deletable;

@ManagedObject( defaultType = "standard", description = Queue.CLASS_DESCRIPTION )
public interface Queue<X extends Queue<X>> extends ConfiguredObject<X>,
                                                   Comparable<X>, ExchangeReferrer,
                                                   BaseQueue,
                                                   MessageSource,
                                                   CapacityChecker,
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
    String ALTERNATE_EXCHANGE = "alternateExchange";
    String EXCLUSIVE = "exclusive";
    String MESSAGE_DURABILITY = "messageDurability";
    String MESSAGE_GROUP_KEY = "messageGroupKey";
    String MESSAGE_GROUP_SHARED_GROUPS = "messageGroupSharedGroups";
    String MESSAGE_GROUP_DEFAULT_GROUP = "messageGroupDefaultGroup";
    String MAXIMUM_DELIVERY_ATTEMPTS = "maximumDeliveryAttempts";
    String NO_LOCAL = "noLocal";
    String OWNER = "owner";
    String QUEUE_FLOW_CONTROL_SIZE_BYTES = "queueFlowControlSizeBytes";
    String QUEUE_FLOW_RESUME_SIZE_BYTES = "queueFlowResumeSizeBytes";
    String QUEUE_FLOW_STOPPED = "queueFlowStopped";
    String MAXIMUM_MESSAGE_TTL = "maximumMessageTtl";
    String MINIMUM_MESSAGE_TTL = "minimumMessageTtl";
    String DEFAULT_FILTERS = "defaultFilters";
    String ENSURE_NONDESTRUCTIVE_CONSUMERS = "ensureNondestructiveConsumers";
    String HOLD_ON_PUBLISH_ENABLED = "holdOnPublishEnabled";


    String QUEUE_MINIMUM_ESTIMATED_MEMORY_FOOTPRINT = "queue.minimumEstimatedMemoryFootprint";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = QUEUE_MINIMUM_ESTIMATED_MEMORY_FOOTPRINT)
    long DEFAULT_MINIMUM_ESTIMATED_MEMORY_FOOTPRINT = 102400L;

    String QUEUE_ESTIMATED_MESSAGE_MEMORY_OVERHEAD = "queue.estimatedMessageMemoryOverhead";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = QUEUE_ESTIMATED_MESSAGE_MEMORY_OVERHEAD)
    long DEFAULT_ESTIMATED_MESSAGE_MEMORY_OVERHEAD = 1024L;

    String QUEUE_SCAVANGE_COUNT = "qpid.queue.scavenge_count";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = QUEUE_SCAVANGE_COUNT)
    int DEFAULT_QUEUE_SCAVANGE_COUNT = 50;


    String MIME_TYPE_TO_FILE_EXTENSION = "qpid.mimeTypeToFileExtension";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = MIME_TYPE_TO_FILE_EXTENSION, description = "A mapping of MIME types to file extensions.")
    String DEFAULT_MIME_TYPE_TO_FILE_EXTENSION = "{\"application/json\":\".json\","
                                                 + "\"application/pdf\":\".pdf\","
                                                 + "\"application/xml\":\".xml\","
                                                 + "\"image/jpeg\":\".jpg\","
                                                 + "\"image/tiff\":\".tiff\","
                                                 + "\"text/plain\":\".txt\"}";

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.defaultExclusivityPolicy",
            description = "the ExclusivityPolicy to apply to queues where none is explicitly set")
    String DEFAULT_EXCLUSIVITY = "NONE";

    @ManagedAttribute
    Exchange getAlternateExchange();

    @ManagedAttribute( defaultValue = "${queue.defaultExclusivityPolicy}")
    ExclusivityPolicy getExclusive();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.defaultEnsureNonDestructiveConsumers",
            description = "the value to use for the ensureNondestructiveCnsumers attribute of a queue where "
                          + "none is explicitly set")
    String DEFAULT_ENSURE_NON_DESTRUCTIVE_CONSUMERS = "false";


    @SuppressWarnings("unused")
    @ManagedAttribute( defaultValue = "${queue.defaultEnsureNonDestructiveConsumers}" )
    boolean isEnsureNondestructiveConsumers();

    @DerivedAttribute( persist = true )
    String getOwner();

    @SuppressWarnings("unused")
    @ManagedAttribute
    boolean isNoLocal();


    @ManagedAttribute
    String getMessageGroupKey();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "qpid.broker_default-shared-message-group")
    String DEFAULT_SHARED_MESSAGE_GROUP = "qpid.no-group";

    @ManagedAttribute( defaultValue = "${qpid.broker_default-shared-message-group}")
    String getMessageGroupDefaultGroup();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.maximumDistinctGroups")
    int DEFAULT_MAXIMUM_DISTINCT_GROUPS = 255;

    @ManagedAttribute( defaultValue = "${queue.maximumDistinctGroups}")
    int getMaximumDistinctGroups();

    @ManagedAttribute
    boolean isMessageGroupSharedGroups();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.maximumDeliveryAttempts")
    int DEFAULT_MAXIMUM_DELIVERY_ATTEMPTS = 0;

    @ManagedAttribute( defaultValue = "${queue.maximumDeliveryAttempts}")
    int getMaximumDeliveryAttempts();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.queueFlowControlSizeBytes")
    long DEFAULT_FLOW_CONTROL_SIZE_BYTES = 0L;

    @ManagedAttribute( defaultValue = "${queue.queueFlowControlSizeBytes}")
    long getQueueFlowControlSizeBytes();

    @SuppressWarnings("unused")
    @ManagedContextDefault( name = "queue.queueFlowResumeSizeBytes")
    long DEFAULT_FLOW_CONTROL_RESUME_SIZE_BYTES = 0L;

    @ManagedAttribute( defaultValue = "${queue.queueFlowResumeSizeBytes}")
    long getQueueFlowResumeSizeBytes();


    @SuppressWarnings("unused")
    @DerivedAttribute
    boolean isQueueFlowStopped();

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



    @ManagedAttribute( defaultValue = "${queue.defaultMessageDurability}" )
    MessageDurability getMessageDurability();

    @SuppressWarnings("unused")
    @ManagedAttribute
    long getMinimumMessageTtl();

    @SuppressWarnings("unused")
    @ManagedAttribute
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

    //children
    Collection<? extends Binding<?>> getBindings();


    Collection<? extends Consumer<?>> getConsumers();

    //operations

    void visit(QueueEntryVisitor visitor);

    ListenableFuture<Integer> deleteAndReturnCountAsync();

    int deleteAndReturnCount();


    void setNotificationListener(QueueNotificationListener listener);

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Bindings")
    int getBindingCount();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Consumers")
    int getConsumerCount();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Consumers with credit")
    int getConsumerCountWithCredit();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Delivered (Persistent)")
    long getPersistentDequeuedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Delivered (Persistent)")
    long getPersistentDequeuedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Enqueued (Persistent)")
    long getPersistentEnqueuedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Enqueued (Persistent)")
    long getPersistentEnqueuedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Queue Depth")
    long getQueueDepthBytes();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Queue Depth")
    int getQueueDepthMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Delivered")
    long getTotalDequeuedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Delivered")
    long getTotalDequeuedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Enqueued")
    long getTotalEnqueuedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Enqueued")
    long getTotalEnqueuedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Prefetched")
    long getUnacknowledgedBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Prefetched")
    long getUnacknowledgedMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Available")
    long getAvailableBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Available")
    int getAvailableMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Available HWM")
    long getAvailableBytesHighWatermark();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Available HWM")
    int getAvailableMessagesHighWatermark();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Queue Depth HWM")
    long getQueueDepthBytesHighWatermark();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Queue Depth HWM")
    int getQueueDepthMessagesHighWatermark();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.ABSOLUTE_TIME, label = "Oldest Message")
    long getOldestMessageAge();

    @ManagedOperation(description = "reset cumulative and high watermark statistics values", changesConfiguredObjectState = false)
    void resetStatistics();

    @ManagedOperation(description = "move messages from this queue to another", changesConfiguredObjectState = false)
    List<Long> moveMessages(@Param(name = "destination", description = "The queue to which the messages should be moved") Queue<?> destination,
                            @Param(name = "messageIds", description = "If provided, only messages in the queue whose (internal) message-id is supplied will be considered for moving") List<Long> messageIds,
                            @Param(name = "selector", description = "A (JMS) selector - if provided, only messages which match the selector will be considered for moving") String selector,
                            @Param(name = "limit", description = "Maximum number of messages to move", defaultValue = "-1") int limit);


    @ManagedOperation(description = "copies messages from this queue to another", changesConfiguredObjectState = false)
    List<Long> copyMessages(@Param(name = "destination", description = "The queue to which the messages should be copied") Queue<?> destination,
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
                                      description = "If true, converts message content into JSON format.") boolean returnJson,
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

    void addBinding(Binding<?> binding);

    void removeBinding(Binding<?> binding);

    LogSubject getLogSubject();

    VirtualHost<?> getVirtualHost();

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

    Set<NotificationCheck> getNotificationChecks();

    Collection<String> getAvailableAttributes();

    void completeRecovery();

    void recover(ServerMessage<?> message, MessageEnqueueRecord enqueueRecord);

    void setTargetSize(long targetSize);

    long getPotentialMemoryFootprint();

    boolean isHeld(QueueEntry queueEntry, final long evaluationTime);

    void checkCapacity();
}
