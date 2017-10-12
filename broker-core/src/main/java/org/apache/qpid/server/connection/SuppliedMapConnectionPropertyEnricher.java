/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.qpid.server.connection;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.plugin.ConnectionPropertyEnricher;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.transport.AMQPConnection;

@PluggableService
public class SuppliedMapConnectionPropertyEnricher implements ConnectionPropertyEnricher
{
    private static final Logger LOG = LoggerFactory.getLogger(SuppliedMapConnectionPropertyEnricher.class);

    private static final String PROPERTY_MAP_CONTEXT = "qpid.connection.property_map";

    @Override
    public Map<String, Object> addConnectionProperties(final AMQPConnection<?> connection,
                                                       final Map<String, Object> existingProperties)
    {

        if(connection.getContextKeys(false).contains(PROPERTY_MAP_CONTEXT))
        {
            Map<String,Object> modifiedProperties = new LinkedHashMap<>(existingProperties);

            // This will be OK as the context will come from JSON which necessitates a string-keyed map
            @SuppressWarnings("unchecked")
            Map<String, Object> map = connection.getContextValue(Map.class, PROPERTY_MAP_CONTEXT);

            modifiedProperties.putAll(map);
            return Collections.unmodifiableMap(modifiedProperties);
        }
        else
        {
            return existingProperties;
        }


    }

    @Override
    public String getType()
    {
        return "SUPPLIED-MAP";
    }
}
