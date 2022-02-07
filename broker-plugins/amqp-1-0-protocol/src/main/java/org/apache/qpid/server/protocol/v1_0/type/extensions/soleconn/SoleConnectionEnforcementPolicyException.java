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
package org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.qpid.server.protocol.v1_0.AMQPConnection_1_0;
import org.apache.qpid.server.security.limit.ConnectionLimitException;

public class SoleConnectionEnforcementPolicyException extends ConnectionLimitException
{
    private static final String MESSAGE =
            "Single connection with container ID '%s' is required due to sole connection enforcement policy '%s'";

    private final Set<AMQPConnection_1_0<?>> _existingConnections;

    private final SoleConnectionEnforcementPolicy _policy;

    private final String _containerID;

    public SoleConnectionEnforcementPolicyException(SoleConnectionEnforcementPolicy policy,
                                                    Collection<? extends AMQPConnection_1_0<?>> connections,
                                                    String containerID)
    {
        super(String.format(MESSAGE, containerID, policy));
        _policy = Objects.requireNonNull(policy);
        _existingConnections = new HashSet<>(connections);
        _containerID = Objects.requireNonNull(containerID);
    }

    public SoleConnectionEnforcementPolicy getPolicy()
    {
        return _policy;
    }

    public Set<AMQPConnection_1_0<?>> getExistingConnections()
    {
        return Collections.unmodifiableSet(_existingConnections);
    }

    public String getContainerID()
    {
        return _containerID;
    }
}
