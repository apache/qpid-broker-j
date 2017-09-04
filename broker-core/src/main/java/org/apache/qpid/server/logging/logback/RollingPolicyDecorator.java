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
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.RollingPolicy;
import ch.qos.logback.core.rolling.RollingPolicyBase;
import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.helper.CompressionMode;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollingPolicyDecorator implements RollingPolicy
{
    public static final int DEFAULT_RESCAN_DELAY = 5000;
    public static final String ROLLOVER_RESCAN_DELAY_MS_PROPERTY_NAME = "qpid.logger_rollover_rescan_delay_ms";
    public static final int DEFAULT_RESCAN_LIMIT = 60;
    public static final String ROLLOVER_RESCAN_LIMIT_PROPERTY_NAME = "qpid.logger_rollover_rescan_limit";
    private static final Logger LOGGER = LoggerFactory.getLogger(RollingPolicyDecorator.class);

    private final RollingPolicyBase _decorated;
    private final RolloverListener _listener;
    private final Path _rolledFilesBaseFolder;
    private final Pattern _rolledFileRegExp;
    private final ScheduledExecutorService _executorService;
    private final long _rescanDelayMillis = Long.getLong(ROLLOVER_RESCAN_DELAY_MS_PROPERTY_NAME, DEFAULT_RESCAN_DELAY);
    private final long _rescanLimit = Long.getLong(ROLLOVER_RESCAN_LIMIT_PROPERTY_NAME, DEFAULT_RESCAN_LIMIT);
    private final Lock _publishResultsLock = new ReentrantLock();
    private ScanTask _currentScanTask;
    private String[] _previousScanResults;

    public RollingPolicyDecorator(RollingPolicyBase decorated, RolloverListener listener, ScheduledExecutorService executorService)
    {
        _decorated = decorated;
        _listener = listener;
        _executorService = executorService;

        String filePathPattern = _decorated.getFileNamePattern();
        String filePathRegExp = new FileNamePattern(filePathPattern, _decorated.getContext()).toRegex();
        FilePathBaseFolderAndPatternPair pair = new FilePathBaseFolderAndPatternPair(filePathRegExp);
        _rolledFilesBaseFolder = pair.getBaseFolder();
        _rolledFileRegExp = pair.getPattern();
        _currentScanTask = null;
    }

    @Override
    public void rollover() throws RolloverFailure
    {
        ScanTask task = createScanTaskAndCancelInProgress();
        _decorated.rollover();
        _executorService.execute(task);
    }

    @Override
    public String getActiveFileName()
    {
        return _decorated.getActiveFileName();
    }

    @Override
    public CompressionMode getCompressionMode()
    {
        return _decorated.getCompressionMode();
    }

    @Override
    public void setParent(FileAppender appender)
    {
        _decorated.setParent(appender);
    }

    @Override
    public void start()
    {
        ScanTask task = createScanTaskAndCancelInProgress();
        _decorated.start();
        _executorService.execute(task);
    }

    @Override
    public void stop()
    {
        _decorated.stop();
        synchronized (_publishResultsLock)
        {
            if (_currentScanTask != null)
            {
                _currentScanTask.cancel();
            }
            _previousScanResults = null;
        }
    }

    @Override
    public boolean isStarted()
    {
        return _decorated.isStarted();
    }


    public interface RolloverListener
    {
        void onRollover(Path baseFolder, String[] relativeFileNames);
        void onNoRolloverDetected(Path baseFolder, String[] relativeFileNames);
    }

    public RollingPolicyBase getDecorated()
    {
        return _decorated;
    }

    private ScanTask createScanTaskAndCancelInProgress()
    {
        ScanTask task = new ScanTask();
        synchronized (_publishResultsLock)
        {
            if (_currentScanTask != null)
            {
                _currentScanTask.cancel();
            }
            _currentScanTask = task;
        }
        return task;
    }

    private class ScanTask implements Runnable
    {
        private int _rescanCounter;
        private volatile boolean _canceled;

        @Override
        public void run()
        {
            if (!isCanceled() )
            {
                String[] rolloverFiles = scan();

                if (!publishScanResults(rolloverFiles) && !isCanceled())
                {
                    if (_rescanCounter < _rescanLimit)
                    {
                        ++_rescanCounter;
                        _executorService.schedule(this, _rescanDelayMillis, TimeUnit.MILLISECONDS);
                    }
                    else
                    {
                        _listener.onNoRolloverDetected(_rolledFilesBaseFolder, rolloverFiles);
                    }
                }
            }
        }

        private boolean publishScanResults(String[] rolloverFiles)
        {
            boolean published = false;

            if (rolloverFiles != null && !isCanceled() )
            {
                synchronized (_publishResultsLock)
                {
                    if (!isCanceled() && (_previousScanResults == null || !Arrays.equals(rolloverFiles, _previousScanResults)))
                    {
                        _previousScanResults = rolloverFiles;
                        published = true;
                    }
                }
            }

            if (published)
            {
                _listener.onRollover(_rolledFilesBaseFolder, rolloverFiles);
            }

            return published;
        }

        public String[] scan()
        {
            final List<Path> rolledFiles = new ArrayList<>();
            try
            {
                Files.walkFileTree(_rolledFilesBaseFolder, new FileVisitor<Path>()
                {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    {
                        return (isCanceled() ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE);
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    {
                        String absolutePath = file.toAbsolutePath().toString();
                        if (File.separatorChar == '\\')
                        {
                            absolutePath = absolutePath.replace('\\', '/');
                        }

                        if (_rolledFileRegExp.matcher(absolutePath).matches())
                        {
                            rolledFiles.add(file);
                        }
                        return (isCanceled() ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE);
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc)
                    {
                        return (isCanceled() ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE);
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                    {
                        return (isCanceled() ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE);
                    }
                });
            }
            catch(IOException e)
            {
                LOGGER.warn("Unexpected IOException while scanning for rollover files.", e);
            }
            return (isCanceled() ? null : relativizeAndSort(_rolledFilesBaseFolder, rolledFiles));
        }

        public void cancel()
        {
            _canceled = true;
        }


        private String[] relativizeAndSort(Path parent, List<Path> rolledFiles)
        {
            String[] results = new String[rolledFiles.size()];
            int i = 0;
            for (Path f : rolledFiles)
            {
                results[i++] = parent.relativize(f).toString();
            }

            Arrays.sort(results, new Comparator<String>()
            {
                @Override
                public int compare(String o1, String o2)
                {
                    return o1.compareTo(o2);
                }
            });
            return results;
        }

        public boolean isCanceled()
        {
            return _canceled || Thread.currentThread().isInterrupted();
        }

    }

    private static class FilePathBaseFolderAndPatternPair
    {
        private final Path _baseFolder;
        private final Pattern _pattern;

        public FilePathBaseFolderAndPatternPair(String fileNamePattern)
        {
            String path;
            int firstDigitPatternPosition= fileNamePattern.indexOf("\\d");
            if (firstDigitPatternPosition == -1)
            {
                throw new RuntimeException("Rolling policy file pattern does not seem to contain date or integer token");
            }
            int slashBeforeDigitPatternPosition = fileNamePattern.lastIndexOf("/", firstDigitPatternPosition);
            if (slashBeforeDigitPatternPosition != -1)
            {
                path = fileNamePattern.substring(0, slashBeforeDigitPatternPosition);
                fileNamePattern = fileNamePattern.substring( slashBeforeDigitPatternPosition + 1);
            }
            else
            {
                path = System.getProperty("user.dir");
            }
            _baseFolder = new File(path).toPath().toAbsolutePath();
            _pattern = Pattern.compile(Pattern.quote(path) + "/" + fileNamePattern);
        }

        public Path getBaseFolder()
        {
            return _baseFolder;
        }

        public Pattern getPattern()
        {
            return _pattern;
        }
    }
}
