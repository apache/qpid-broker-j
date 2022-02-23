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

import java.util.Iterator;
import java.util.Map;

final class FinalBranch extends TreeBranch
{
    FinalBranch(String prefix)
    {
        super(prefix);
    }

    FinalBranch(String prefix, Map<Character, AbstractTreeBranch> branches)
    {
        super(prefix, branches);
    }

    FinalBranch(String prefix, AbstractTreeBranch first)
    {
        super(prefix, first);
    }

    @Override
    public int size()
    {
        return 1 + super.size();
    }

    @Override
    boolean contains(String str)
    {
        return _prefix.equals(str) || super.contains(str);
    }

    @Override
    public Iterator<String> iterator()
    {
        return new IteratorImpl(this, "");
    }

    @Override
    FinalBranch newFinalBranch()
    {
        return this;
    }

    @Override
    TreeBranch newTree(String substring, Map<Character, AbstractTreeBranch> branches)
    {
        return new FinalBranch(substring, branches);
    }
}
