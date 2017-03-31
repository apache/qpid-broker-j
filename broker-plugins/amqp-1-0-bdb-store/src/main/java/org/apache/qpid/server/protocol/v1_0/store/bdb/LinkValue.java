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

package org.apache.qpid.server.protocol.v1_0.store.bdb;

import org.apache.qpid.server.protocol.v1_0.LinkDefinition;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;

public class LinkValue
{
    static final byte CURRENT_VERSION = 0;
    private final Source _source;
    private final Target _target;
    private final byte _version;

    public LinkValue(final Source source, final Target target, final byte version)
    {
        _source = source;
        _target = target;
        _version = version;
    }

    public LinkValue(final LinkDefinition<Source, Target> link)
    {
        this(link.getSource(), link.getTarget(), CURRENT_VERSION);
    }

    public Source getSource()
    {
        return _source;
    }

    public Target getTarget()
    {
        return _target;
    }

    public byte getVersion()
    {
        return _version;
    }
}
