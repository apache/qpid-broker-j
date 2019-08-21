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

package org.apache.qpid.tests.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpQueueAdmin implements QueueAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpQueueAdmin.class);

    @Override
    public void createQueue(BrokerAdmin brokerAdmin, final String queueName)
    {
        LOGGER.debug(String.format("creation of queue '%s' requested", queueName));
    }

    @Override
    public void deleteQueue(BrokerAdmin brokerAdmin, final String queueName)
    {
        LOGGER.debug(String.format("deletion of queue '%s' requested", queueName));
    }

    @Override
    public void putMessageOnQueue(BrokerAdmin brokerAdmin, final String queueName, final String... messages)
    {
        LOGGER.debug(String.format("putting of %d messages on queue '%s' requested", messages.length, queueName));
    }

    @Override
    public boolean isDeleteQueueSupported()
    {
        return false;
    }

    @Override
    public boolean isPutMessageOnQueueSupported()
    {
        return false;
    }
}
