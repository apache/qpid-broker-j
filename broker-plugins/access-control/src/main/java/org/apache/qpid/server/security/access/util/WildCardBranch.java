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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.qpid.server.util.Strings;

final class WildCardBranch extends AbstractTreeBranch
{
    WildCardBranch(String prefix)
    {
        super(prefix);
    }

    @Override
    public int size()
    {
        return 1;
    }

    @Override
    public Map<Character, PrefixTree> branches()
    {
        return Collections.emptyMap();
    }

    @Override
    boolean contains(String str)
    {
        return str.startsWith(_prefix);
    }

    @Override
    public Iterator<String> iterator()
    {
        return Stream.of(_prefix + "*").iterator();
    }

    @Override
    AbstractTreeBranch mergeString(String str)
    {
        final String common = Strings.commonPrefix(str, _prefix);
        if (common.isEmpty())
        {
            return new TreeRoot(this, new FinalBranch(str));
        }
        final int commonLength = common.length();
        if (commonLength == _length)
        {
            return this;
        }
        if (commonLength == str.length())
        {
            return new FinalBranch(common, new WildCardBranch(_prefix.substring(commonLength)));
        }
        return new TreeBranch(common,
                new WildCardBranch(_prefix.substring(commonLength)),
                new FinalBranch(str.substring(commonLength)));
    }

    @Override
    AbstractTreeBranch mergeWildCard(String prefix)
    {
        final String common = Strings.commonPrefix(prefix, _prefix);
        if (common.isEmpty())
        {
            return new TreeRoot(this, new WildCardBranch(prefix));
        }
        final int commonLength = common.length();
        if (commonLength == _length)
        {
            return this;
        }
        if (commonLength == prefix.length())
        {
            return new WildCardBranch(prefix);
        }
        return new TreeBranch(common,
                new WildCardBranch(_prefix.substring(commonLength)),
                new WildCardBranch(prefix.substring(commonLength)));
    }
}
