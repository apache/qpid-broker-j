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
package org.apache.qpid.server.model.port;

import org.apache.qpid.server.configuration.IllegalConfigurationException;

public interface PortWithThreadPool
{
    String THREAD_POOL_MINIMUM = "threadPoolMinimum";
    String THREAD_POOL_MAXIMUM = "threadPoolMaximum";

    int getThreadPoolMaximum();

    int getThreadPoolMinimum();

    class PortWithThreadPoolValidator
    {
        public static void validate(PortWithThreadPool port)
        {
            if (port.getThreadPoolMaximum() < 1)
            {
                throw new IllegalConfigurationException(String.format("Thread pool maximum %d is too small. Must be greater than zero.", port.getThreadPoolMaximum()));
            }
            if (port.getThreadPoolMinimum() < 1)
            {
                throw new IllegalConfigurationException(String.format("Thread pool minimum %d is too small. Must be greater than zero.", port.getThreadPoolMinimum()));
            }
            if (port.getThreadPoolMinimum() > port.getThreadPoolMaximum())
            {
                throw new IllegalConfigurationException(String.format("Thread pool minimum %d cannot be greater than thread pool maximum %d.", port.getThreadPoolMinimum() , port.getThreadPoolMaximum()));
            }
        }
    }
}
