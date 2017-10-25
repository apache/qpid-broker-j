/*
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
 */
package org.apache.qpid.server.stats;

/**
 * This interface is to be implemented by any broker business object that
 * wishes to gather statistics about messages delivered through it.
 * 
 * These statistics are exposed using a management interface, which
 * calls these methods to retrieve the underlying statistics values.
 * This interface gives a standard way for
 * parts of the broker to set up and configure statistics collection.
 * <p>
 * When creating these objects, there should be a parent/child relationship
 * between them, such that the lowest level gatherer can record statistics if
 * enabled, and pass on the notification to the parent object to allow higher
 * level aggregation. When resetting statistics, this works in the opposite
 * direction, with higher level gatherers also resetting all of their children.
 */
public interface StatisticsGatherer
{
    /**
     * This method is responsible for registering the receipt of a message
     * with the counters.
     *
     * @param messageSize the size in bytes of the delivered message
     */
    void registerMessageReceived(long messageSize);

    void registerTransactedMessageReceived();

    /**
     * This method is responsible for registering the delivery of a message
     * with the counters.
     * 
     * @param messageSize the size in bytes of the delivered message
     */
    void registerMessageDelivered(long messageSize);

    void registerTransactedMessageDelivered();

    /**
     * Returns a number of delivered messages
     * 
     * @return the number of delivered messages
     */
    long getMessagesOut();
    
    /**
     * Returns a number of received messages
     * 
     * @return the number of received messages
     */
    long getMessagesIn();
    
    /**
     * Returns a number of delivered bytes
     * 
     * @return the number of delivered bytes
     */
    long getBytesOut();
    
    /**
     * Returns a number of received bytes
     * 
     * @return the number of received bytes
     */
    long getBytesIn();

}
