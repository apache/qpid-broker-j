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

package org.apache.qpid.server.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ModelVersion
{
    private static final Pattern MODEL_VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)$");

    private final int _major;
    private final int _minor;

    public static ModelVersion fromString(String versionString)
    {
        if (versionString == null)
        {
            throw new IllegalArgumentException("Cannot parse null");
        }
        final Matcher matcher = MODEL_VERSION_PATTERN.matcher(versionString);
        if (!matcher.matches())
        {
            throw new IllegalArgumentException(String.format("Could not parse model version string '%s'", versionString));
        }
        return new ModelVersion(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
    }

    public ModelVersion(final int major, final int minor)
    {
        _major = major;
        _minor = minor;
    }

    public int getMajor()
    {
        return _major;
    }

    public int getMinor()
    {
        return _minor;
    }

    public boolean lessThan(ModelVersion other)
    {
        return (getMajor() < other.getMajor() || (getMajor() == other.getMajor() && getMinor() < other.getMinor()));
    }

    @Override
    public String toString()
    {
        return _major + "." + _minor;
    }
}
