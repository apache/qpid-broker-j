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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.qos.logback.core.rolling.LengthCounter;
import ch.qos.logback.core.rolling.LengthCounterBase;
import ch.qos.logback.core.rolling.TimeBasedFileNamingAndTriggeringPolicy;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.rolling.helper.ArchiveRemover;
import ch.qos.logback.core.rolling.helper.Compressor;
import ch.qos.logback.core.rolling.helper.DateTokenConverter;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import ch.qos.logback.core.rolling.helper.RollingCalendar;
import ch.qos.logback.core.rolling.helper.SizeAndTimeBasedArchiveRemover;
import ch.qos.logback.core.spi.ContextAwareBase;
import ch.qos.logback.core.util.FileSize;

final class DailyTriggeringPolicy<E> extends ContextAwareBase implements TimeBasedFileNamingAndTriggeringPolicy<E>
{
    private static final long RETRY_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(30);

    private final FileSize _maxFileSize;
    private final boolean _rollOnRestart;
    private final LengthCounter _lengthCounter = new LengthCounterBase();
    private TimeBasedRollingPolicy<E> _rollingPolicy;
    private FileNamePattern _uncompressedPattern;
    private RollingCalendar _calendar;
    private ArchiveRemover _archiveRemover;
    private String _compressionSuffix;
    private Instant _period;
    private Instant _elapsedPeriod;
    private long _nextCheck;
    private long _retryAfter;
    private long _lengthBeforeRollover;
    private int _nextArchiveIndex;
    private int _elapsedArchiveIndex;
    private String _elapsedFileName;
    private boolean _rolloverPending;
    private boolean _rescanArchives;
    private boolean _archiveErrorReported;
    private boolean _started;
    private long _currentTime = -1;

    DailyTriggeringPolicy(final FileSize maxFileSize, final boolean rollOnRestart)
    {
        _maxFileSize = Objects.requireNonNull(maxFileSize, "maxFileSize");
        _rollOnRestart = rollOnRestart;
    }

    @Override
    public void start()
    {
        if (_started)
        {
            return;
        }
        if (_rollingPolicy == null || _rollingPolicy.getParentsRawFileProperty() == null || _maxFileSize.getSize() <= 0)
        {
            addError("Daily rolling requires a rolling policy, an active filename and a positive maximum file size");
            return;
        }

        final String pattern = _rollingPolicy.getFileNamePattern();
        final FileNamePattern archivePattern = new FileNamePattern(pattern, getContext());
        final String uncompressedPattern = Compressor
                .computeFileNameStrWithoutCompSuffix(pattern, _rollingPolicy.getCompressionMode());
        _uncompressedPattern = new FileNamePattern(uncompressedPattern, getContext());
        _compressionSuffix = pattern.substring(uncompressedPattern.length());
        final DateTokenConverter<Object> dateConverter = archivePattern.getPrimaryDateTokenConverter();
        if (dateConverter == null || archivePattern.getIntegerTokenConverter() == null ||
                !uncompressedPattern.endsWith(".%i"))
        {
            addError("Daily rolling requires a date token and a final .%i archive index");
            return;
        }
        _calendar = dateConverter.getZoneId() == null
                ? new RollingCalendar(dateConverter.getDatePattern())
                : new RollingCalendar(dateConverter.getDatePattern(), TimeZone.getTimeZone(dateConverter.getZoneId()),
                        Locale.getDefault());
        if (!_calendar.isCollisionFree())
        {
            addError("The archive date pattern does not distinguish successive periods");
            return;
        }

        try
        {
            final BasicFileAttributes attributes = readActiveFileAttributes();
            final boolean nonempty = attributes != null && attributes.size() > 0;
            _period = nonempty ? attributes.lastModifiedTime().toInstant() : Instant.ofEpochMilli(getCurrentTime());
            _nextCheck = _calendar.getNextTriggeringDate(_period).toEpochMilli();
            _nextArchiveIndex = nextArchiveIndex(_period);
            _rolloverPending = _rollOnRestart && nonempty;
            _rescanArchives = false;
            _archiveErrorReported = false;
            _elapsedFileName = null;
            _elapsedPeriod = null;
            _retryAfter = 0;
            _lengthCounter.reset();
            final SizeAndTimeBasedArchiveRemover remover =
                    new SizeAndTimeBasedArchiveRemover(archivePattern, (RollingCalendar) _calendar.clone());
            remover.setContext(getContext());
            _archiveRemover = remover;
            _started = true;
        }
        catch (IOException | DirectoryIteratorException | ArithmeticException e)
        {
            addError("Cannot initialize daily log archives", e);
        }
    }

    private BasicFileAttributes readActiveFileAttributes() throws IOException
    {
        try
        {
            return Files.readAttributes(Path.of(_rollingPolicy.getParentsRawFileProperty()), BasicFileAttributes.class);
        }
        catch (NoSuchFileException ignore)
        {
            // A new active file will be opened by RollingFileAppender after this policy has started
            return null;
        }
    }

    @Override
    public boolean isTriggeringEvent(final File activeFile, final E event)
    {
        final long now = getCurrentTime();
        if (!_started || now < _retryAfter)
        {
            return false;
        }
        try
        {
            if (_rescanArchives)
            {
                _nextArchiveIndex = nextArchiveIndex(_period);
                _rescanArchives = false;
            }
            if (now >= _nextCheck)
            {
                final Instant nextPeriod = Instant.ofEpochMilli(now);
                final int nextIndex = nextArchiveIndex(nextPeriod);
                prepareRollover();
                _period = nextPeriod;
                _nextArchiveIndex = nextIndex;
                _nextCheck = _calendar.getNextTriggeringDate(nextPeriod).toEpochMilli();
                _rolloverPending = false;
                _retryAfter = 0;
                return _lengthBeforeRollover > 0;
            }
            if (_rolloverPending || _lengthCounter.getLength() >= _maxFileSize.getSize())
            {
                final int nextIndex = Math.incrementExact(_nextArchiveIndex);
                prepareRollover();
                _nextArchiveIndex = nextIndex;
                _rolloverPending = false;
                _retryAfter = 0;
                return true;
            }
        }
        catch (IOException | DirectoryIteratorException | ArithmeticException e)
        {
            _retryAfter = now + RETRY_DELAY_MILLIS;
            if (!_archiveErrorReported)
            {
                _archiveErrorReported = true;
                addError("Cannot determine a safe daily archive name; continuing to append and retrying later", e);
            }
        }
        return false;
    }

    private void prepareRollover()
    {
        _elapsedPeriod = _period;
        _elapsedArchiveIndex = _nextArchiveIndex;
        _elapsedFileName = getCurrentPeriodsFileNameWithoutCompressionSuffix();
        _lengthBeforeRollover = _lengthCounter.getLength();
        _lengthCounter.reset();
        _archiveErrorReported = false;
    }

    void rolloverFailed()
    {
        _period = _elapsedPeriod;
        _nextArchiveIndex = _elapsedArchiveIndex;
        _nextCheck = _calendar.getNextTriggeringDate(_period).toEpochMilli();
        _rolloverPending = true;
        _rescanArchives = true;
        _retryAfter = getCurrentTime() + RETRY_DELAY_MILLIS;
        try
        {
            final BasicFileAttributes attributes = readActiveFileAttributes();
            _lengthCounter.reset();
            if (attributes != null)
            {
                _lengthCounter.add(attributes.size());
            }
        }
        catch (final IOException e)
        {
            _lengthCounter.add(_lengthBeforeRollover);
            addWarn("Cannot read the active log size after a failed rollover; retaining the previous byte count", e);
        }
    }

    private int nextArchiveIndex(final Instant period) throws IOException
    {
        final Path firstArchive = Path.of(_uncompressedPattern.convertMultipleArguments(period, 0)).toAbsolutePath();
        final String firstName = firstArchive.getFileName().toString();
        final String prefix = firstName.substring(0, firstName.length() - 1);
        final String suffix = _compressionSuffix.isEmpty() ? "" : "(?:" + Pattern.quote(_compressionSuffix) + ")?";
        final Pattern archiveNames = Pattern.compile(Pattern.quote(prefix) + "([0-9]+)" + suffix);
        int highest = -1;
        try (final DirectoryStream<Path> entries = Files.newDirectoryStream(firstArchive.getParent()))
        {
            for (final Path entry : entries)
            {
                final Matcher matcher = archiveNames.matcher(entry.getFileName().toString());
                if (matcher.matches())
                {
                    final int index;
                    try
                    {
                        index = Integer.parseInt(matcher.group(1));
                    }
                    catch (NumberFormatException e)
                    {
                        throw new IOException("Archive index is too large: " + entry, e);
                    }
                    highest = Math.max(highest, index);
                }
            }
        }
        return Math.incrementExact(highest);
    }

    @Override
    public LengthCounter getLengthCounter()
    {
        return _lengthCounter;
    }

    @Override
    public void setTimeBasedRollingPolicy(final TimeBasedRollingPolicy<E> rollingPolicy)
    {
        _rollingPolicy = Objects.requireNonNull(rollingPolicy, "rollingPolicy");
    }

    @Override
    public String getElapsedPeriodsFileName()
    {
        return _elapsedFileName;
    }

    @Override
    public String getCurrentPeriodsFileNameWithoutCompressionSuffix()
    {
        return _uncompressedPattern.convertMultipleArguments(_period, _nextArchiveIndex);
    }

    @Override
    public ArchiveRemover getArchiveRemover()
    {
        return _archiveRemover;
    }

    @Override
    public long getCurrentTime()
    {
        return _currentTime < 0 ? System.currentTimeMillis() : _currentTime;
    }

    @Override
    public void setCurrentTime(final long currentTime)
    {
        _currentTime = currentTime;
    }

    @Override
    public void stop()
    {
        _started = false;
    }

    @Override
    public boolean isStarted()
    {
        return _started;
    }
}
