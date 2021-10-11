/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.security.access.config;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public enum Property
{
    ROUTING_KEY,
    NAME,
    QUEUE_NAME,
    OWNER,
    TYPE,
    ALTERNATE,
    DURABLE,
    EXCLUSIVE,
    TEMPORARY,
    AUTO_DELETE,
    COMPONENT,
    PACKAGE,
    CLASS,
    FROM_NETWORK,
    FROM_HOSTNAME,
    VIRTUALHOST_NAME,
    METHOD_NAME,
    ATTRIBUTES,
    CREATED_BY,
    CONNECTION_LIMIT,
    CONNECTION_FREQUENCY_LIMIT;

    private static final Set<Property> BOOLEAN_PROPERTIES = EnumSet.of(DURABLE, EXCLUSIVE, TEMPORARY, AUTO_DELETE);

    private static final Map<String, Property> CANONICAL_NAME_TO_PROPERTY = new HashMap<>();

    static
    {
        for (final Property property : values())
        {
            CANONICAL_NAME_TO_PROPERTY.put(property.getCanonicalName(), property);
        }
        // There is a bug in the documentation, broker's documentation stated attribute_names instead of attributes.
        CANONICAL_NAME_TO_PROPERTY.put(getCanonicalName("attribute_names"), ATTRIBUTES);
    }

    public static boolean isBooleanType(Property property)
    {
        return BOOLEAN_PROPERTIES.contains(property);
    }

    /**
     * Properties are parsed using their canonical name (see {@link #getCanonicalName(String)})
     * so that, for the sake of user-friendliness, the ACL file parser is insensitive to
     * case and underscores.
     */
    public static Property parse(String text)
    {
        final String propertyName = getCanonicalName(text);
        final Property property = CANONICAL_NAME_TO_PROPERTY.get(propertyName);

        if (property == null)
        {
            throw new IllegalArgumentException("Not a valid property: " + text
                    + " because " + propertyName
                    + " is not in " + CANONICAL_NAME_TO_PROPERTY.keySet());
        }
        return property;
    }

    public String getCanonicalName()
    {
        return getCanonicalName(name());
    }

    private static String getCanonicalName(String name)
    {
        return name.replace("_", "").toLowerCase(Locale.ENGLISH);
    }
}
