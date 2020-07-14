/*
 *
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

package org.apache.qpid.server.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class CollectionUtils
{
    private CollectionUtils()
    {
        super();
    }

    public static boolean isSingle(Collection<?> collection)
    {
        return collection != null && collection.size() == 1;
    }

    public static boolean isEmpty(Collection<?> collection)
    {
        return collection == null || collection.isEmpty();
    }

    public static <T> Set<T> immutable(Set<? extends T> set)
    {
        if (set == null)
        {
            return Collections.emptySet();
        }
        switch (set.size())
        {
            case 0:
                return Collections.emptySet();
            case 1:
                return Collections.singleton(set.iterator().next());
            default:
                return Collections.unmodifiableSet(new LinkedHashSet<>(set));
        }
    }
}
