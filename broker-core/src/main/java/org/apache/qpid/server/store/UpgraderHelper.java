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

package org.apache.qpid.server.store;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class UpgraderHelper
{
    public static final Map<String, String> MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES = new HashMap<>();
    static
    {
        MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.put("qpid.security.tls.protocolWhiteList",
                                                                                     "qpid.security.tls.protocolAllowList");
        MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.put("qpid.security.tls.protocolBlackList",
                                                                                     "qpid.security.tls.protocolDenyList");
        MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.put("qpid.security.tls.cipherSuiteWhiteList",
                                                                                     "qpid.security.tls.cipherSuiteAllowList");
        MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.put("qpid.security.tls.cipherSuiteBlackList",
                                                                                     "qpid.security.tls.cipherSuiteDenyList");
    }

    public static Map<String, String> renameContextVariables(final Map<String, String> context,
                                                             final Map<String, String> oldToNewNameMapping)
    {
        final Map<String, String> newContext = new HashMap<>(context);
        oldToNewNameMapping.forEach((oldName, newName) -> {
            if (newContext.containsKey(oldName))
            {
                final String value = newContext.remove(oldName);
                newContext.put(newName, value);
            }
        });
        return newContext;
    }

    public static Map<String, String> reverse(Map<String, String> map)
    {
        return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }
}
