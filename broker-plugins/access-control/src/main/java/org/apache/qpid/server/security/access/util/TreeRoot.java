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

import java.util.HashMap;
import java.util.Map;

final class TreeRoot extends TreeBranch
{
    TreeRoot(AbstractTreeBranch first, AbstractTreeBranch second)
    {
        super(first, second);
    }

    TreeRoot(Map<Character, AbstractTreeBranch> map)
    {
        super(map);
    }

    @Override
    boolean contains(String str)
    {
        final PrefixTree subTree = _branches.get(str.charAt(0));
        return subTree != null && subTree.match(str);
    }

    @Override
    AbstractTreeBranch mergeString(String str)
    {
        final Map<Character, AbstractTreeBranch> branches = new HashMap<>(_branches);
        final char key = str.charAt(0);
        final AbstractTreeBranch branch = _branches.get(key);
        if (branch == null)
        {
            branches.put(key, new FinalBranch(str));
        }
        else
        {
            branches.put(key, branch.mergeString(str));
        }
        return new TreeRoot(branches);
    }

    @Override
    AbstractTreeBranch mergeWildCard(String prefix)
    {
        final Map<Character, AbstractTreeBranch> branches = new HashMap<>(_branches);
        final char key = prefix.charAt(0);
        final AbstractTreeBranch branch = _branches.get(key);
        if (branch == null)
        {
            branches.put(key, new WildCardBranch(prefix));
        }
        else
        {
            branches.put(key, branch.mergeWildCard(prefix));
        }
        return new TreeRoot(branches);
    }
}
