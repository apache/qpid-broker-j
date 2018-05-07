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

package org.apache.qpid.systests.end_to_end_conversion.client;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AugumentConnectionUrl implements ClientInstruction
{
    private Map<String, String> _connectionUrlConfig;

    public AugumentConnectionUrl(final Map<String, String> connectionUrlConfig)
    {

        _connectionUrlConfig = new HashMap<>(connectionUrlConfig);
    }


    public Map<String, String> getConnectionUrlConfig()
    {
        return Collections.unmodifiableMap(_connectionUrlConfig);
    }
}
