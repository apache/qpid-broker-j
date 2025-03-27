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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.server.message.MessageDestination;

@ManagedObject(creatable = false, amqpName = "org.apache.qpid.Producer")
public interface Producer<X extends Producer<X>> extends ConfiguredObject<X>
{
    enum DeliveryType { DELAYED_DELIVERY, STANDARD_DELIVERY }

    enum DestinationType
    {
        EXCHANGE,
        QUEUE;

        public static DestinationType from(MessageDestination messageDestination)
        {
            if (messageDestination instanceof Exchange)
            {
                return EXCHANGE;
            }
            else if (messageDestination instanceof Queue)
            {
                return QUEUE;
            }
            return null;
        }

        public static UUID getId(MessageDestination messageDestination)
        {
            final DestinationType destinationType = from(messageDestination);
            return destinationType == null ? null : ((ConfiguredObject<?>) messageDestination).getId();
        }
    }

    void registerMessageDelivered(long messageSize);

    @DerivedAttribute(description = "Session ID")
    String getSessionId();

    @DerivedAttribute(description = "Session name")
    String getSessionName();

    @DerivedAttribute(description = "Connection principal")
    String getPrincipal();

    @DerivedAttribute(description = "Connection remote address")
    String getRemoteAddress();

    @DerivedAttribute(description = "Destination name")
    String getDestination();

    void setDestination(String destination);

    @DerivedAttribute(description = "Destination id")
    UUID getDestinationId();

    void setDestinationId(UUID destinationId);

    @DerivedAttribute(description = "DeliveryType type (standard or delayed)")
    DeliveryType getDeliveryType();

    @DerivedAttribute(description = "Destination type (exchange or queue)")
    DestinationType getDestinationType();

    void setDestinationType(DestinationType destinationType);

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, resettable = true)
    int getMessagesOut();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, resettable = true)
    long getBytesOut();

    @ManagedOperation(description = "Resets producer statistics", changesConfiguredObjectState = true)
    void resetStatistics();

    CompletableFuture<Void> deleteNoChecks();
}
