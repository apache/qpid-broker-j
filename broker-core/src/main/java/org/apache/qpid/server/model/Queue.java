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

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.message.MessageInfo;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.queue.QueueEntryVisitor;
import org.apache.qpid.server.store.MessageDurability;

@ManagedObject( defaultType = "standard", description = Queue.CLASS_DESCRIPTION )
public interface Queue<X extends Queue<X>> extends ConfiguredObject<X>
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
    @ManagedContextDefault( name = QUEUE_MINIMUM_ESTIMATED_MEMORY_FOOTPRINT)
    long DEFAULT_MINIMUM_ESTIMATED_MEMORY_FOOTPRINT = 102400l;

    String QUEUE_ESTIMATED_MESSAGE_MEMORY_OVERHEAD = "queue.estimatedMessageMemoryOverhead";
    @ManagedContextDefault( name = QUEUE_ESTIMATED_MESSAGE_MEMORY_OVERHEAD)
    long DEFAULT_ESTIMATED_MESSAGE_MEMORY_OVERHEAD = 1024l;

    String MAX_ASYNCHRONOUS_DELIVERIES = "queue.maxAsynchronousDeliveries";
    @ManagedContextDefault(name = MAX_ASYNCHRONOUS_DELIVERIES )
    int DEFAULT_MAX_ASYNCHRONOUS_DELIVERIES = 80;

    String MIME_TYPE_TO_FILE_EXTENSION = "qpid.mimeTypeToFileExtension";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = MIME_TYPE_TO_FILE_EXTENSION)
    String DEFAULT_MIME_TYPE_TO_FILE_EXTENSION = "{\"application/json\":\".json\","
                                                 + "\"application/pdf\":\".pdf\","
                                                 + "\"application/xml\":\".xml\","
                                                 + "\"image/jpeg\":\".jpg\","
                                                 + "\"image/tiff\":\".tiff\","
                                                 + "\"text/plain\":\".txt\"}";

    @ManagedContextDefault( name = "queue.defaultExclusivityPolicy")
    String DEFAULT_EXCLUSIVITY = "NONE";

    @ManagedAttribute
    Exchange getAlternateExchange();

    @ManagedAttribute( defaultValue = "${queue.defaultExclusivityPolicy}")
    ExclusivityPolicy getExclusive();

    @ManagedContextDefault( name = "queue.defaultEnsureNonDestructiveConsumers")
    String DEFAULT_ENSURE_NON_DESTRUCTIVE_CONSUMERS = "false";

    @ManagedAttribute( defaultValue = "${queue.defaultEnsureNonDestructiveConsumers}" )
    boolean isEnsureNondestructiveConsumers();

    @DerivedAttribute( persist = true )
    String getOwner();

    @ManagedAttribute
    boolean isNoLocal();


    @ManagedAttribute
    String getMessageGroupKey();

    @ManagedContextDefault( name = "qpid.broker_default-shared-message-group")
    String DEFAULT_SHARED_MESSAGE_GROUP = "qpid.no-group";

    @ManagedAttribute( defaultValue = "${qpid.broker_default-shared-message-group}")
    String getMessageGroupDefaultGroup();

    @ManagedContextDefault( name = "queue.maximumDistinctGroups")
    int DEFAULT_MAXIMUM_DISTINCT_GROUPS = 255;

    @ManagedAttribute( defaultValue = "${queue.maximumDistinctGroups}")
    int getMaximumDistinctGroups();

    @ManagedAttribute
    boolean isMessageGroupSharedGroups();

    @ManagedContextDefault( name = "queue.maximumDeliveryAttempts")
    int DEFAULT_MAXIMUM_DELIVERY_ATTEMPTS = 0;

    @ManagedAttribute( defaultValue = "${queue.maximumDeliveryAttempts}")
    int getMaximumDeliveryAttempts();

    @ManagedContextDefault( name = "queue.queueFlowControlSizeBytes")
    long DEFAULT_FLOW_CONTROL_SIZE_BYTES = 0l;

    @ManagedAttribute( defaultValue = "${queue.queueFlowControlSizeBytes}")
    long getQueueFlowControlSizeBytes();

    @ManagedContextDefault( name = "queue.queueFlowResumeSizeBytes")
    long DEFAULT_FLOW_CONTROL_RESUME_SIZE_BYTES = 0l;

    @ManagedAttribute( defaultValue = "${queue.queueFlowResumeSizeBytes}")
    long getQueueFlowResumeSizeBytes();


    // TODO - this is not an attribute
    @DerivedAttribute
    boolean isQueueFlowStopped();

    @ManagedContextDefault( name = "queue.alertThresholdMessageAge")
    long DEFAULT_ALERT_THRESHOLD_MESSAGE_AGE = 0l;

    @ManagedAttribute( defaultValue = "${queue.alertThresholdMessageAge}")
    long getAlertThresholdMessageAge();

    @ManagedContextDefault( name = "queue.alertThresholdMessageSize")
    long DEFAULT_ALERT_THRESHOLD_MESSAGE_SIZE = 0l;

    @ManagedAttribute( defaultValue = "${queue.alertThresholdMessageSize}")
    long getAlertThresholdMessageSize();

    @ManagedContextDefault( name = "queue.alertThresholdQueueDepthBytes")
    long DEFAULT_ALERT_THRESHOLD_QUEUE_DEPTH = 0l;

    @ManagedAttribute( defaultValue = "${queue.alertThresholdQueueDepthBytes}")
    long getAlertThresholdQueueDepthBytes();

    @ManagedContextDefault( name = "queue.alertThresholdQueueDepthMessages")
    long DEFAULT_ALERT_THRESHOLD_MESSAGE_COUNT = 0l;

    @ManagedAttribute( defaultValue = "${queue.alertThresholdQueueDepthMessages}")
    long getAlertThresholdQueueDepthMessages();


    @ManagedContextDefault( name = "queue.alertRepeatGap")
    long DEFAULT_ALERT_REPEAT_GAP = 30000l;

    @ManagedAttribute( defaultValue = "${queue.alertRepeatGap}")
    long getAlertRepeatGap();

    @ManagedContextDefault( name = "queue.defaultMessageDurability")
    String DEFAULT_MESSAGE_DURABILTY = "DEFAULT";

    @ManagedAttribute( defaultValue = "${queue.defaultMessageDurability}" )
    MessageDurability getMessageDurability();

    @ManagedAttribute
    long getMinimumMessageTtl();

    @ManagedAttribute
    long getMaximumMessageTtl();

    @ManagedAttribute
    Map<String, Map<String,List<String>>> getDefaultFilters();


    @ManagedContextDefault( name = "queue.holdOnPublishEnabled")
    boolean DEFAULT_HOLD_ON_PUBLISH_ENABLED = false;

    @ManagedAttribute( defaultValue = "${queue.holdOnPublishEnabled}")
    boolean isHoldOnPublishEnabled();

    //children
    Collection<? extends Binding> getBindings();


    Collection<? extends Consumer> getConsumers();

    //operations

    void visit(QueueEntryVisitor visitor);

    ListenableFuture<Integer> deleteAndReturnCount();

    void setNotificationListener(QueueNotificationListener listener);

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Bindings")
    int getBindingCount();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Consumers")
    int getConsumerCount();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Consumers with credit")
    int getConsumerCountWithCredit();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Delivered (Persistent)")
    long getPersistentDequeuedBytes();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Delivered (Persistent)")
    long getPersistentDequeuedMessages();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Enqueued (Persistent)")
    long getPersistentEnqueuedBytes();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Enqueued (Persistent)")
    long getPersistentEnqueuedMessages();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Queue Depth")
    long getQueueDepthBytes();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Queue Depth")
    int getQueueDepthMessages();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Delivered")
    long getTotalDequeuedBytes();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Delivered")
    long getTotalDequeuedMessages();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Enqueued")
    long getTotalEnqueuedBytes();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Enqueued")
    long getTotalEnqueuedMessages();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Prefetched")
    long getUnacknowledgedBytes();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Prefetched")
    long getUnacknowledgedMessages();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.ABSOLUTE_TIME, label = "Oldest Message")
    long getOldestMessageAge();

    @ManagedOperation
    List<Long> moveMessages(@Param(name = "destination", description = "The queue to which the messages should be moved") Queue<?> destination,
                            @Param(name = "messageIds", description = "If provided, only messages in the queue whose (internal) message-id is supplied will be considered for moving") List<Long> messageIds,
                            @Param(name = "selector", description = "A (JMS) selector - if provided, only messages which match the selector will be considered for moving") String selector,
                            @Param(name = "limit", description = "Maximum number of messages to move", defaultValue = "-1") int limit);


    @ManagedOperation
    List<Long> copyMessages(@Param(name = "destination", description = "The queue to which the messages should be copied") Queue<?> destination,
                            @Param(name = "messageIds", description = "If provided, only messages in the queue whose (internal) message-id is supplied will be considered for copying") List<Long> messageIds,
                            @Param(name = "selector", description = "A (JMS) selector - if provided, only messages which match the selector will be considered for copying")  String selector,
                            @Param(name = "limit", description = "Maximum number of messages to copy", defaultValue = "-1") int limit);


    @ManagedOperation
    List<Long> deleteMessages(@Param(name = "messageIds", description = "If provided, only messages in the queue whose (internal) message-id is supplied will be considered for deletion") List<Long> messageIds,
                              @Param(name = "selector", description = "A (JMS) selector - if provided, only messages which match the selector will be considered for deletion") String selector,
                              @Param(name = "limit", description = "Maximum number of messages to delete", defaultValue = "-1") int limit);


    @ManagedOperation
    long clearQueue();

    @ManagedOperation(nonModifying = true)
    Content getMessageContent(@Param(name = "messageId") long messageId,
                              @Param(name = "limit", defaultValue = "-1", description = "Number of bytes to return") long limit);

    @ManagedOperation(nonModifying = true)
    List<MessageInfo> getMessageInfo(@Param(name = "first", defaultValue = "-1") int first, @Param(name = "last", defaultValue = "-1") int last);

    @ManagedOperation(nonModifying = true)
    MessageInfo getMessageInfoById(@Param(name = "messageId") long messageId);


    boolean isHeld(QueueEntry queueEntry, final long evaluationTime);
}
