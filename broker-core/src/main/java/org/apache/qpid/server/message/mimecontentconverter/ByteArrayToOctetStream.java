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
package org.apache.qpid.server.message.mimecontentconverter;

import org.apache.qpid.server.plugin.PluggableService;

@PluggableService
public class ByteArrayToOctetStream implements ObjectToMimeContentConverter<byte[]>
{
    @Override
    public String getType()
    {
        return getMimeType();
    }

    @Override
    public String getMimeType()
    {
        return "application/octet-stream";
    }

    @Override
    public Class<byte[]> getObjectClass()
    {
        return byte[].class;
    }

    @Override
    public int getRank()
    {
        return 0;
    }

    @Override
    public boolean isAcceptable(final byte[] object)
    {
        return true;
    }

    @Override
    public byte[] toMimeContent(final byte[] object)
    {
        if (object == null)
        {
            return new byte[0];
        }
        return object;
    }
}
