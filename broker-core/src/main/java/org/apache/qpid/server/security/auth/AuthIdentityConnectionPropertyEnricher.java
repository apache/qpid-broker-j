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

package org.apache.qpid.server.security.auth;

import java.security.Principal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.plugin.ConnectionPropertyEnricher;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.QpidPrincipal;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.transport.AMQPConnection;

@PluggableService
public class AuthIdentityConnectionPropertyEnricher implements ConnectionPropertyEnricher
{
    private static final Logger LOG = LoggerFactory.getLogger(AuthIdentityConnectionPropertyEnricher.class);

    @Override
    public Map<String, Object> addConnectionProperties(final AMQPConnection<?> connection,
                                                       final Map<String, Object> existingProperties)
    {
        Map<String,Object> modifiedProperties = new LinkedHashMap<>(existingProperties);

        final Principal principal = connection.getAuthorizedPrincipal();
        if(principal != null)
        {
            GenericPrincipal genericPrincipal = new GenericPrincipal((QpidPrincipal)principal);
            Map<String,String> claims = new LinkedHashMap<>();
            claims.put("sub", genericPrincipal.toExternalForm());
            claims.put("preferred_username", genericPrincipal.getName());
            modifiedProperties.put("authenticated-identity", claims);

        }
        Set<GroupPrincipal> groups = connection.getSubject().getPrincipals(GroupPrincipal.class);
        List<String> groupNames = groups.stream().map(GroupPrincipal::getName).collect(Collectors.toList());
        modifiedProperties.put("groups", groupNames);
        return Collections.unmodifiableMap(modifiedProperties);
    }


    @Override
    public String getType()
    {
        return "AUTH_IDENTITY";
    }
}
