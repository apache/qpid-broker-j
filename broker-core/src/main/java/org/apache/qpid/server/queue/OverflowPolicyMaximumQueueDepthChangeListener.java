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
package org.apache.qpid.server.queue;

import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;

abstract class OverflowPolicyMaximumQueueDepthChangeListener extends AbstractConfigurationChangeListener
{
    private final OverflowPolicy _overflowPolicy;
    private boolean _maximumQueueDepthChangeDetected;

    OverflowPolicyMaximumQueueDepthChangeListener(final OverflowPolicy overflowPolicy)
    {
        _overflowPolicy = overflowPolicy;
    }

    @Override
    public void attributeSet(final ConfiguredObject<?> object,
                             final String attributeName,
                             final Object oldAttributeValue,
                             final Object newAttributeValue)
    {
        super.attributeSet(object, attributeName, oldAttributeValue, newAttributeValue);
        if (Queue.MAXIMUM_QUEUE_DEPTH_BYTES.equals(attributeName)
            || Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES.equals(attributeName))
        {
            _maximumQueueDepthChangeDetected = true;
        }
    }

    @Override
    public void bulkChangeEnd(final ConfiguredObject<?> object)
    {
        super.bulkChangeEnd(object);
        if (object instanceof Queue)
        {
            Queue<?> queue = (Queue<?>) object;

            if (queue.getOverflowPolicy() == _overflowPolicy)
            {
                if (_maximumQueueDepthChangeDetected)
                {
                    _maximumQueueDepthChangeDetected = false;
                    onMaximumQueueDepthChange(queue);
                }
            }
            else
            {
                queue.removeChangeListener(this);
            }
        }
    }

    abstract void onMaximumQueueDepthChange(final Queue<?> queue);
}
