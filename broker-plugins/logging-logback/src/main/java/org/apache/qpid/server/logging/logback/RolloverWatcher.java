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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.LogFileDetails;
import org.apache.qpid.server.logging.PathContent;
import org.apache.qpid.server.logging.ZippedContent;

public class RolloverWatcher implements RollingPolicyDecorator.RolloverListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RolloverWatcher.class);
    private static final Comparator<LogFileDetails> LAST_MODIFIED_COMPARATOR = new Comparator<LogFileDetails>()
    {
        @Override
        public int compare(final LogFileDetails o1, final LogFileDetails o2)
        {
            return Long.compare(o2.getLastModified(), o1.getLastModified());
        }
    };
    private final Path _activeFilePath;
    private volatile Collection<String> _rolledFiles;
    private volatile Path _baseFolder;

    public RolloverWatcher(final String activeFileName)
    {
        _activeFilePath = new File(activeFileName).toPath();
        _rolledFiles = Collections.emptyList();
    }

    @Override
    public void onRollover(Path baseFolder, String[] relativeFileNames)
    {
        _rolledFiles = Collections.unmodifiableCollection(Arrays.asList(relativeFileNames));
        _baseFolder = baseFolder;
    }

    @Override
    public void onNoRolloverDetected(final Path baseFolder, final String[] relativeFileNames)
    {
        if (_baseFolder == null)
        {
            _baseFolder = baseFolder;
        }
        LOGGER.info("Exceeded maximum number of rescans without detecting rolled over log file.");
    }

    public PathContent getFileContent(String fileName)
    {
        if (fileName == null)
        {
            throw new IllegalArgumentException("File name cannot be null");
        }

        Path path = getPath(fileName);
        return new PathContent(path, getContentType(fileName));
    }

    private Path getPath(String fileName)
    {
        Path path = null;
        File active = _activeFilePath.toFile();
        if (fileName.equals(active.getName()))
        {
            path =  active.toPath();
        }
        else if (_rolledFiles.contains(fileName))
        {
            path = _baseFolder.resolve(fileName);
        }
        return path;
    }

    public Collection<String> getRolledFiles()
    {
        return _rolledFiles;
    }

    public List<LogFileDetails> getLogFileDetails()
    {
        List<LogFileDetails> results = new ArrayList<>();
        results.add(getFileDetails(_activeFilePath));
        List<String> rolledFiles = new ArrayList<>(_rolledFiles);
        for (String fileName : rolledFiles)
        {
            Path file = _baseFolder.resolve(fileName);
            LogFileDetails details = getFileDetails(file);
            results.add(details);
        }
        Collections.sort(results, LAST_MODIFIED_COMPARATOR);
        return results;
    }

    private LogFileDetails getFileDetails(Path path)
    {
        File file = path.toFile();
        return new LogFileDetails(getDisplayName(path), file.lastModified(), file.length());
    }

    private String getDisplayName(Path path)
    {
        String displayName = path.getFileName().toString();
        if (!_activeFilePath.equals(path) && (_baseFolder != null))
        {
            try
            {
                displayName = _baseFolder.relativize(path).toString();
            }
            catch (IllegalArgumentException e)
            {
                // active file might not be relative to root
                // returning a file name
            }
        }
        return displayName;
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

    public ZippedContent getFilesAsZippedContent(Set<String> fileNames)
    {
        if (fileNames == null)
        {
            throw new IllegalArgumentException("File name cannot be null");
        }

        Map<String, Path> paths = new TreeMap<>();
        for (String name: fileNames)
        {
            Path filePath = getPath(name);
            if (filePath != null)
            {
                paths.put(name, filePath);
            }
        }

        return new ZippedContent(paths);
    }

    public ZippedContent getAllFilesAsZippedContent()
    {
        Set<String> fileNames = new HashSet<>(_rolledFiles);
        fileNames.add(getDisplayName(_activeFilePath));

        return getFilesAsZippedContent(fileNames);
    }
}
