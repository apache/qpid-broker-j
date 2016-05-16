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
package org.apache.qpid.server.logging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.qpid.server.model.CustomRestHeaders;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.model.Content;

public class PathContent implements Content, CustomRestHeaders
{
    private final Path _path;
    private final String _contentType;
    private final String _disposition;
    private final long _contentSize;

    public PathContent(Path path, String contentType)
    {
        _path = path;
        _contentType = contentType;
        _disposition = _path == null ? "attachment" : "attachment; filename=\"" + _path.getFileName().toString() + "\"";
        _contentSize = _path == null ? 0 : _path.toFile().length();
    }

    @RestContentHeader("Content-Type")
    public String getContentType()
    {
        return _contentType;
    }

    @RestContentHeader("Content-Disposition")
    public String getContentDisposition()
    {
        return _disposition;
    }

    @Override
    public void write(OutputStream outputStream) throws IOException
    {
        if (_path != null && _path.toFile().exists())
        {
            Files.copy(_path, outputStream);
        }
        else
        {
            throw new FileNotFoundException();
        }
    }

    @Override
    public void release()
    {
    }
}
