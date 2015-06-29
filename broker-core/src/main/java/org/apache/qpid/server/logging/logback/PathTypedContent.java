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
package org.apache.qpid.server.logging.logback;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import org.apache.qpid.server.model.TypedContent;

public class PathTypedContent implements TypedContent
{
    private final Path _path;
    private final String _contentType;

    public PathTypedContent(Path path,  String contentType)
    {
        _path = path;
        _contentType = contentType;
    }

    @Override
    public String getContentType()
    {
        return _contentType;
    }

    @Override
    public InputStream openInputStream() throws IOException
    {
        return _path == null ? null : new FileInputStream(_path.toFile());
    }

    @Override
    public long getSize()
    {
        return _path == null ? 0 : _path.toFile().length();
    }

    @Override
    public String getFileName()
    {
        return _path == null ? null : _path.getFileName().toString();
    }
}
