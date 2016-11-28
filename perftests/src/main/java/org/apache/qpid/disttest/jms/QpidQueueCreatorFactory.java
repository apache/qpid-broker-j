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
 *
 */

package org.apache.qpid.disttest.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.disttest.DistributedTestException;

public class QpidQueueCreatorFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidQueueCreatorFactory.class);

    public static final String QUEUE_CREATOR_CLASS_NAME_SYSTEM_PROPERTY = "qpid.disttest.queue.creator.class";

    private QpidQueueCreatorFactory()
    {
    }

    public static QueueCreator createInstance()
    {
        String queueCreatorClassName = System.getProperty(QUEUE_CREATOR_CLASS_NAME_SYSTEM_PROPERTY);
        if(queueCreatorClassName == null)
        {
            queueCreatorClassName = NoOpQueueCreator.class.getName();
        }
        else
        {
            LOGGER.info("Using overridden queue creator class " + queueCreatorClassName);
        }

        try
        {
            Class<? extends QueueCreator> queueCreatorClass = (Class<? extends QueueCreator>) Class.forName(queueCreatorClassName);
            return queueCreatorClass.newInstance();
        }
        catch (ClassNotFoundException e)
        {
            throw new DistributedTestException("Unable to instantiate queue creator using class name " + queueCreatorClassName, e);
        }
        catch (InstantiationException e)
        {
            throw new DistributedTestException("Unable to instantiate queue creator using class name " + queueCreatorClassName, e);
        }
        catch (IllegalAccessException e)
        {
            throw new DistributedTestException("Unable to instantiate queue creator using class name " + queueCreatorClassName, e);
        }
    }
}
