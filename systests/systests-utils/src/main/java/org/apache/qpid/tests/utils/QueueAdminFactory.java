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

class QueueAdminFactory
{
    static final String QUEUE_ADMIN_TYPE_PROPERTY_NAME = "qpid.tests.protocol.broker.external.queueAdmin";
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueAdminFactory.class);

    @SuppressWarnings("unchecked")
    QueueAdmin create() throws BrokerAdminException
    {
        final String queueAdminClassName =
                System.getProperty(QUEUE_ADMIN_TYPE_PROPERTY_NAME, NoOpQueueAdmin.class.getName());
        LOGGER.debug(String.format("Using queue admin of type '%s'", queueAdminClassName));
        try
        {
            final Class<? extends QueueAdmin> queueCreatorClass =
                    (Class<? extends QueueAdmin>) Class.forName(queueAdminClassName);
            return queueCreatorClass.newInstance();
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException e)
        {
            throw new BrokerAdminException(String.format("Unable to instantiate queue admin of type '%s'",
                                                         queueAdminClassName), e);
        }
    }
}
