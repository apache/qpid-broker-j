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

package org.apache.qpid.server.model.preferences;

import java.security.Principal;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.QpidPrincipal;

public class GenericPrincipal implements Principal
{
    private static final Pattern PATTERN = Pattern.compile("(\\w+)@(\\w+)\\('(\\w+)'\\)");

    private final String _name;
    private final String _originType;
    private final String _originName;

    public GenericPrincipal(final String name)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("Principal name cannot be null");
        }
        Matcher m = PATTERN.matcher(name);
        if (!m.matches())
        {
            throw new IllegalArgumentException("Principal has unexpected format");
        }
        _name = m.group(1);
        _originType = m.group(2);
        _originName = m.group(3);
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final GenericPrincipal that = (GenericPrincipal) o;

        return _name.equals(that._name) && _originType.equals(that._originType) && _originName.equals(that._originName);
    }

    @Override
    public int hashCode()
    {
        return _name.hashCode();
    }

    @Override
    public String toString()
    {
        return "GenericPrincipal{" +
               "_name='" + _name + '\'' +
               ", _originType='" + _originType + '\'' +
               ", _originName='" + _originName + '\'' +
               '}';
    }

    public String getOriginType()
    {
        return _originType;
    }

    public String getOriginName()
    {
        return _originName;
    }


    public static boolean principalsContain(Collection<Principal> principals, Principal principal)
    {
        for (Principal currentPrincipal : principals)
        {
            if (principalsEqual(principal, currentPrincipal))
            {
                return true;
            }
        }
        return false;
    }

    public static boolean principalsEqual(final Principal p1, final Principal p2)
    {
        if (p1 == null)
        {
            return p2 == null;
        }
        else if (p2 == null)
        {
            return false;
        }

        if (p1 instanceof GenericPrincipal)
        {
            return genericPrincipalEquals((GenericPrincipal) p1, p2);
        }
        if (p2 instanceof GenericPrincipal)
        {
            return genericPrincipalEquals((GenericPrincipal) p2, p1);
        }

        return p1.equals(p2);
    }

    private static boolean genericPrincipalEquals(GenericPrincipal genericPrincipal, Principal otherPrincipal)
    {
        if (otherPrincipal instanceof GenericPrincipal)
        {
            GenericPrincipal otherGenericPrincipal = (GenericPrincipal) otherPrincipal;
            return genericPrincipalEqualsByStrings(genericPrincipal, otherGenericPrincipal.getName(), otherGenericPrincipal.getOriginType(), otherGenericPrincipal.getOriginName());
        }
        else if (otherPrincipal instanceof QpidPrincipal)
        {
            ConfiguredObject<?> origin = ((QpidPrincipal) otherPrincipal).getOrigin();
            return genericPrincipalEqualsByStrings(genericPrincipal, otherPrincipal.getName(), origin.getType(), origin.getName());
        }
        return genericPrincipal.equals(otherPrincipal);
    }

    private static boolean genericPrincipalEqualsByStrings(GenericPrincipal genericPrincipal, String name, String originType, String originName)
    {
        return (genericPrincipal.getName().equals(name)
                && genericPrincipal.getOriginType().equals(originType)
                && genericPrincipal.getOriginName().equals(originName));
    }
}
