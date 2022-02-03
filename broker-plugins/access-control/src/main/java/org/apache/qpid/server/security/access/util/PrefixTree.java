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
package org.apache.qpid.server.security.access.util;

import java.util.Collection;
import java.util.Map;

public interface PrefixTree extends Iterable<String>
{
    String prefix();

    char firstPrefixCharacter();

    Map<Character, PrefixTree> branches();

    int size();

    boolean match(String str);

    default PrefixTree mergeWith(String str)
    {
        if (str == null || str.isEmpty())
        {
            throw new IllegalArgumentException("Prefix tree can not be merged with an empty value");
        }
        if ("*".equals(str))
        {
            return Any.INSTANCE;
        }
        if (str.endsWith("*"))
        {
            return mergeWithPrefix(str.substring(0, str.length() - 1));
        }
        return mergeWithFinalValue(str);
    }

    default PrefixTree mergeWith(Collection<String> collection)
    {
        PrefixTree tree = this;
        for (final String str : collection)
        {
            tree = tree.mergeWith(str);
        }
        return tree;
    }

    PrefixTree mergeWithFinalValue(String str);

    PrefixTree mergeWithPrefix(String prefix);

    static PrefixTree empty()
    {
        return Empty.INSTANCE;
    }

    static PrefixTree from(String str)
    {
        if (str == null || str.isEmpty())
        {
            throw new IllegalArgumentException("A non null string is required");
        }
        if ("*".equals(str))
        {
            return Any.INSTANCE;
        }
        if (str.endsWith("*"))
        {
            return fromPrefixWithWildCard(str.substring(0, str.length() - 1));
        }
        return fromFinalValue(str);
    }

    static PrefixTree from(Collection<String> collection)
    {
        return empty().mergeWith(collection);
    }

    static PrefixTree fromFinalValue(String value)
    {
        if (value == null || value.isEmpty())
        {
            throw new IllegalArgumentException("A non empty value is required");
        }
        return new FinalBranch(value);
    }

    static PrefixTree fromPrefixWithWildCard(String prefix)
    {
        if (prefix == null || prefix.isEmpty())
        {
            throw new IllegalArgumentException("A non empty prefix is required");
        }
        return new WildCardBranch(prefix);
    }
}
