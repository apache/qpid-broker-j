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
package org.apache.qpid.server.user.connection.limits.config;

import java.util.Optional;

abstract class AbstractRule implements Rule
{
    private final String _port;
    private final String _identity;

    AbstractRule(String port, String identity)
    {
        super();
        this._port = Optional.ofNullable(port).orElse(RulePredicates.ALL_PORTS);
        this._identity = Optional.ofNullable(identity).orElse(RulePredicates.ALL_USERS);
    }

    @Override
    public String getPort()
    {
        return _port;
    }

    @Override
    public String getIdentity()
    {
        return _identity;
    }
}
