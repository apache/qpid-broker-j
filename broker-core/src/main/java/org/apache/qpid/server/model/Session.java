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

import java.util.Set;

@ManagedObject( creatable = false, amqpName = "org.apache.qpid.Session")
public interface Session<X extends Session<X>> extends ConfiguredObject<X>
{
    String CHANNEL_ID = "channelId";
    // PRODUCER_FLOW_BLOCKED is exposed as an interim step.  We will expose attribute(s) that exposing
    // available credit of both producer and consumer sides.
    String PRODUCER_FLOW_BLOCKED = "producerFlowBlocked";

    String TRANSACTION_TIMEOUT_NOTIFICATION_REPEAT_PERIOD = "qpid.session.transactionTimeoutNotificationRepeatPeriod";

    @ManagedContextDefault(name = TRANSACTION_TIMEOUT_NOTIFICATION_REPEAT_PERIOD,
                           description = "Frequency, in milliseconds, with which transaction timeout warnings will be repeated.")
    long TRANSACTION_TIMEOUT_NOTIFICATION_REPEAT_PERIOD_DEFAULT = 10000;

    String PRODUCER_AUTH_CACHE_SIZE = "producer.authCacheSize";
    @ManagedContextDefault(name = PRODUCER_AUTH_CACHE_SIZE,
                           description = "Maximum number of distinct destinations for which a cached auth value may be held")
    int PRODUCER_AUTH_CACHE_SIZE_DEFAULT = 20;


    String PRODUCER_AUTH_CACHE_TIMEOUT = "producer.authCacheTimeout";
    @ManagedContextDefault(name = PRODUCER_AUTH_CACHE_TIMEOUT,
            description = "Maximum time, in milliseconds, for which a cached auth value may be retained")
    long PRODUCER_AUTH_CACHE_TIMEOUT_DEFAULT = 300000L;


    @DerivedAttribute
    int getChannelId();

    @DerivedAttribute
    boolean isProducerFlowBlocked();


    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Consumers")
    long getConsumerCount();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Prefetched")
    long getUnacknowledgedMessages();

    @ManagedOperation(nonModifying = true,
            changesConfiguredObjectState = false,
            skipAclCheck = true)
    Set<? extends Consumer<?, ?>> getConsumers();
}
