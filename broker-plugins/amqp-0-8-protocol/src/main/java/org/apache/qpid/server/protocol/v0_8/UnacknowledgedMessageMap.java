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
package org.apache.qpid.server.protocol.v0_8;

import java.util.Collection;
import java.util.Map;

import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.message.MessageInstance;


public interface UnacknowledgedMessageMap
{
    interface Visitor
    {
        /**
         * @param deliveryTag
         * @param message the message being iterated over @return true to stop iteration, false to continue
         */
        boolean callback(final long deliveryTag, MessageInstance message);

        void visitComplete();
    }

    void visit(Visitor visitor);

    void add(long deliveryTag, MessageInstance message, final ConsumerTarget target);

    MessageInstance remove(long deliveryTag, final boolean restoreCredit);

    int size();

    MessageInstance get(long deliveryTag);

    Collection<MessageInstance> acknowledge(long deliveryTag, boolean multiple);
    void collect(long key, boolean multiple, Map<Long, MessageInstance> msgs);
}


