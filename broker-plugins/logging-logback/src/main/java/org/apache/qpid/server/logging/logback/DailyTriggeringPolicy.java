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
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.qos.logback.core.rolling.LengthCounter;
import ch.qos.logback.core.rolling.LengthCounterBase;
import ch.qos.logback.core.rolling.TimeBasedFileNamingAndTriggeringPolicyBase;
import ch.qos.logback.core.rolling.helper.CompressionMode;
import ch.qos.logback.core.rolling.helper.Compressor;
import ch.qos.logback.core.rolling.helper.FileFilterUtil;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import ch.qos.logback.core.rolling.helper.RollingCalendar;
import ch.qos.logback.core.rolling.helper.SizeAndTimeBasedArchiveRemover;
import ch.qos.logback.core.util.FileSize;

/**
 * Adds restart triggering to daily and size-based naming. Logback's TimeBasedRollingPolicy still performs rollover,
 * compression and history removal. A restart consumes the next index in the existing period, rather than resetting it
 * by pretending that a new time period has started.
 */
final class DailyTriggeringPolicy<E> extends TimeBasedFileNamingAndTriggeringPolicyBase<E>
{
    private static final long RETRY_DELAY_MILLIS = 30_000L;

    private final FileSize _maxFileSize;
    private final boolean _rollOnRestart;
    private final LengthCounter _lengthCounter = new LengthCounterBase();
    private FileNamePattern _uncompressedPattern;
    private int _nextIndex;
    private boolean _firstEvent;
    private boolean _scanRequired;
    private long _nextScanAttempt;
    private long _nextRolloverAttempt;

    DailyTriggeringPolicy(final FileSize maxFileSize, final boolean rollOnRestart)
    {
        _maxFileSize = Objects.requireNonNull(maxFileSize, "maxFileSize");
        _rollOnRestart = rollOnRestart;
    }

    @Override
    public void start()
    {
        if (isStarted())
        {
            return;
        }
        super.start();
        if (!isErrorFree())
        {
            return;
        }

        final String pattern = enclosingTBRP.getFileNamePattern();
        final FileNamePattern archivePattern = new FileNamePattern(pattern, getContext());
        _uncompressedPattern = new FileNamePattern(Compressor.computeFileNameStrWithoutCompSuffix(pattern,
                enclosingTBRP.getCompressionMode()), getContext());
        if (archivePattern.getIntegerTokenConverter() == null)
        {
            addError("Daily rolling requires an archive index token");
            return;
        }
        archiveRemover = new SizeAndTimeBasedArchiveRemover(archivePattern, _uncompressedPattern,
                (RollingCalendar) rc.clone());
        archiveRemover.setContext(getContext());
        _lengthCounter.reset();
        // Resolve the archive index when a rollover is needed, so a scan failure cannot stop log writes.
        _nextIndex = 0;
        _scanRequired = true;
        _nextScanAttempt = 0;
        _nextRolloverAttempt = 0;
        _firstEvent = true;
        started = true;
    }

    @Override
    public boolean isTriggeringEvent(final File activeFile, final E event)
    {
        if (!isStarted() || activeFile == null)
        {
            return false;
        }
        final boolean restart = _firstEvent && _rollOnRestart;
        final long now = getCurrentTime();
        final boolean hasContent = _lengthCounter.getLength() > 0;
        final boolean newPeriod = now >= atomicNextCheck.get();
        if (!hasContent)
        {
            _firstEvent = false;
            if (newPeriod)
            {
                beginPeriod(now);
            }
            return false;
        }
        if (!newPeriod && !restart && _lengthCounter.getLength() < _maxFileSize.getSize())
        {
            _firstEvent = false;
            return false;
        }
        if (now < _nextRolloverAttempt)
        {
            return false;
        }
        try
        {
            _nextIndex = resolveIndex(now);
            final int followingIndex = Math.incrementExact(_nextIndex);
            elapsedPeriodsFileName = getCurrentPeriodsFileNameWithoutCompressionSuffix();
            _nextIndex = followingIndex;
            if (newPeriod)
            {
                beginPeriod(now);
            }
            _firstEvent = false;
            _lengthCounter.reset();
            return true;
        }
        catch (final IOException | ArithmeticException e)
        {
            _scanRequired = true;
            _nextRolloverAttempt = now + RETRY_DELAY_MILLIS;
            addWarn("Cannot determine a safe daily log archive name; appending and retrying in 30 seconds", e);
            return false;
        }
    }

    @Override
    public String getCurrentPeriodsFileNameWithoutCompressionSuffix()
    {
        return _uncompressedPattern.convertMultipleArguments(dateInCurrentPeriod, _nextIndex);
    }

    @Override
    public LengthCounter getLengthCounter()
    {
        return _lengthCounter;
    }

    FileSize getMaxFileSize()
    {
        return _maxFileSize;
    }

    private void beginPeriod(final long now)
    {
        setDateInCurrentPeriod(now);
        atomicNextCheck.set(computeNextCheck(now));
        _nextIndex = 0;
        _scanRequired = true;
    }

    private int resolveIndex(final long now) throws IOException
    {
        int candidate = _nextIndex;
        if (_scanRequired && now >= _nextScanAttempt)
        {
            try
            {
                candidate = Math.max(candidate, findNextIndex(dateInCurrentPeriod));
                _scanRequired = false;
            }
            catch (final IOException | DirectoryIteratorException | NumberFormatException e)
            {
                _nextScanAttempt = now + RETRY_DELAY_MILLIS;
                addWarn("Cannot scan daily log archives; checking individual archive filenames instead", e);
            }
        }
        for (int index = candidate; ; index = Math.incrementExact(index))
        {
            final String fileName = _uncompressedPattern.convertMultipleArguments(dateInCurrentPeriod, index);
            if (!archiveExists(Path.of(fileName)) &&
                    (enclosingTBRP.getCompressionMode() != CompressionMode.GZ ||
                            !archiveExists(Path.of(fileName + ".gz"))))
            {
                return index;
            }
        }
    }

    private boolean archiveExists(final Path archive) throws IOException
    {
        try
        {
            Files.readAttributes(archive, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return true;
        }
        catch (final NoSuchFileException ignore)
        {
            // Only a missing path is available; permission and other I/O failures must not imply absence.
            return false;
        }
    }

    private int findNextIndex(final Instant period) throws IOException
    {
        final Path directory = Path.of(_uncompressedPattern.convertMultipleArguments(period, 0))
                .toAbsolutePath().getParent();
        final String stem = FileFilterUtil.afterLastSlash(_uncompressedPattern.toRegexForFixedDate(period));
        // An uncompressed archive can still be waiting for Logback's asynchronous compression.
        final Pattern pattern = Pattern.compile(stem +
                (enclosingTBRP.getCompressionMode() == CompressionMode.GZ ? "(?:\\.gz)?" : ""));
        int nextIndex = 0;
        try (final DirectoryStream<Path> files = Files.newDirectoryStream(directory))
        {
            for (final Path file : files)
            {
                final Matcher matcher = pattern.matcher(file.getFileName().toString());
                if (matcher.matches())
                {
                    final int index = Integer.parseInt(matcher.group(matcher.groupCount()));
                    nextIndex = Math.max(nextIndex, Math.incrementExact(index));
                }
            }
        }
        catch (final NoSuchFileException e)
        {
            // The archive directory may not exist until the first rollover.
            return 0;
        }
        return nextIndex;
    }
}
