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
package org.apache.qpid.server.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Collection utilities
 */
public final class CollectionUtils
{
    /** Utility class shouldn't be instantiated directly */
    private CollectionUtils()
    {

    }

    /**
     * Returns a fixed-size list backed by the specified array. When array is null, returns an empty list
     * @param array Array of elements
     * @return List of elements
     * @param <T> Element type
     */
    public static <T> List<T> nullSafeList(final T[] array)
    {
        if (array == null)
        {
            return new ArrayList<>();
        }
        return Arrays.asList(array);
    }
}
