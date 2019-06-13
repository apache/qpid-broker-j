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
package org.apache.qpid.server.store;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public interface StoredMessage<M extends StorableMessageMetaData>
{
    M getMetaData();

    long getMessageNumber();

    /**
     * Returns length bytes of message content beginning from the given offset.  Caller is responsible
     * for the disposal of the returned buffer.  If length is {@link Integer#MAX_VALUE}, length is not
     * constrained.
     */
    QpidByteBuffer getContent(int offset, int length);

    int getContentSize();

    int getMetadataSize();

    void remove();

    boolean isInContentInMemory();

    long getInMemorySize();

    boolean flowToDisk();

    void reallocate();
}
