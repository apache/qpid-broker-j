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

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class RolloverWatcher implements RollingPolicyDecorator.RolloverListener
{
    private volatile Collection<String> _rolledFiles;
    private volatile Path _baseFolder;

    public RolloverWatcher()
    {
        _rolledFiles = Collections.emptyList();
    }

    @Override
    public void onRollover(Path baseFolder, String[] relativeFileNames)
    {
        _rolledFiles = Collections.unmodifiableCollection(Arrays.asList(relativeFileNames));
        _baseFolder = baseFolder;
    }

    public PathTypedContent getTypedContent(String fileName, boolean activeFile)
    {
        if (fileName == null)
        {
            throw new IllegalArgumentException("File name cannot be null");
        }

        Path path = null;
        if (activeFile)
        {
            path =  new File(fileName).toPath();
        }
        else if (_rolledFiles.contains(fileName))
        {
            path = _baseFolder.resolve(fileName);
        }
        return new PathTypedContent(path, getContentType(fileName));
    }

    public Collection<String> getRolledFiles()
    {
        return _rolledFiles;
    }

    public String getContentType(String fileName)
    {
        String fileNameLower = fileName.toLowerCase();
        if (fileNameLower.endsWith(".gz"))
        {
            return "application/x-gzip";
        }
        else if (fileNameLower.endsWith(".zip"))
        {
            return "application/x-zip";
        }
        else
        {
            return "text/plain";
        }
    }
}
