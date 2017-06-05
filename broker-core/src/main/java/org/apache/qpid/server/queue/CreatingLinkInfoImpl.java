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
 *
 */

package org.apache.qpid.server.queue;

public class CreatingLinkInfoImpl implements CreatingLinkInfo
{
    private final boolean _isSendingLink;
    private final String _remoteContainerId;
    private final String _linkName;

    public CreatingLinkInfoImpl(final boolean isSendingLink,
                                final String remoteContainerId,
                                final String linkName)
    {
        _isSendingLink = isSendingLink;
        _remoteContainerId = remoteContainerId;
        _linkName = linkName;
    }

    @Override
    public boolean isSendingLink()
    {
        return _isSendingLink;
    }

    @Override
    public String getRemoteContainerId()
    {
        return _remoteContainerId;
    }

    @Override
    public String getLinkName()
    {
        return _linkName;
    }
}
