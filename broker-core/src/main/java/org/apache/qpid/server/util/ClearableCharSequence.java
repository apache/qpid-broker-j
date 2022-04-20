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

/**
 * CharSequence backed up by a StringBuilder and implementing AutoCloseable.
 * On close() StringBuilder is cleared.
 */
public class ClearableCharSequence implements CharSequence, AutoCloseable
{
    private StringBuilder _stringBuilder = new StringBuilder();

    public ClearableCharSequence(final Object object)
    {
        _stringBuilder.append(object == null ? "null".toCharArray() : object.toString().toCharArray());
    }

    @Override
    public void close()
    {
        final int length = _stringBuilder.length();
        _stringBuilder.setLength(0);
        _stringBuilder.setLength(length);
        _stringBuilder = null;
    }

    @Override
    public int length()
    {
        return _stringBuilder.length();
    }

    @Override
    public char charAt(final int index)
    {
        return _stringBuilder.charAt(index);
    }

    @Override
    public CharSequence subSequence(final int start, final int end)
    {
        return _stringBuilder.subSequence(start, end);
    }

    @Override
    public String toString()
    {
        return _stringBuilder.toString();
    }
}
