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
package org.apache.qpid.server.virtualhost;

import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.qpid.server.protocol.LinkModel;

public interface LinkRegistryModel<T extends LinkModel>
{
    T getSendingLink(String remoteContainerId, String linkName);

    T getReceivingLink(String remoteContainerId, String linkName);

    @Deprecated
    Collection<T> findSendingLinks(final Pattern containerIdPattern,
                                   final Pattern linkNamePattern);

    void visitSendingLinks(LinkVisitor<T> visitor);

    void purgeSendingLinks(Pattern containerIdPattern, Pattern linkNamePattern);

    void purgeReceivingLinks(Pattern containerIdPattern, Pattern linkNamePattern);

    void open();

    void close();

    void delete();

    Object dump();

    interface LinkVisitor<T extends LinkModel>
    {
        boolean visit(T link);
    }
}
