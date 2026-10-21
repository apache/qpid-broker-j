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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;
import ch.qos.logback.core.util.FileSize;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.test.utils.UnitTestBase;

class DailyTriggeringPolicyTest extends UnitTestBase
{
    private static final Instant NOW = Instant.parse("2026-02-10T12:00:00Z");
    private static final LocalDate TODAY = LocalDate.of(2026, 2, 10);
    private static final int MAX_FILE_SIZE = 64;
    private static final String FULL_FILE = "x".repeat(MAX_FILE_SIZE);

    @TempDir
    private Path _directory;
    private Path _activeFile;

    @BeforeEach
    void setUp()
    {
        _activeFile = _directory.resolve("broker.log");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restartAndSizeRolloverContinueArchiveNumbering(final boolean compressed) throws IOException
    {
        writeActive("old", NOW);
        writeArchive(TODAY, 0, compressed, "archive-zero");
        writeArchive(TODAY, 2, compressed, "archive-two");

        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.start();
            assertEquals(3, appender.length());
            appender.append(FULL_FILE);
            assertEquals(MAX_FILE_SIZE, appender.length());
            appender.append("next");
            assertEquals(4, appender.length());
            appender.assertNoErrors();
        }

        assertEquals("archive-zero", readArchive(TODAY, 0, compressed));
        assertEquals("archive-two", readArchive(TODAY, 2, compressed));
        assertEquals("old", readArchive(TODAY, 3, compressed));
        assertEquals(FULL_FILE, readArchive(TODAY, 4, compressed));
        Files.setLastModifiedTime(_activeFile, FileTime.from(NOW));

        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.start();
            appender.append("after-restart");
            appender.assertNoErrors();
        }

        assertEquals("next", readArchive(TODAY, 5, compressed));
        assertEquals("after-restart", Files.readString(_activeFile));
        assertFalse(Files.exists(archive(TODAY, 1, compressed)), "Existing gaps must not be reused");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restartDisabledCountsExistingAndEncodedBytes(final boolean compressed) throws IOException
    {
        final String euro = "\u20ac";
        final String existing = "x".repeat(MAX_FILE_SIZE - 2);
        writeActive(existing, NOW);
        try (final TestAppender appender = new TestAppender(false, compressed))
        {
            appender.start();
            appender.append(euro);
            assertEquals(MAX_FILE_SIZE + 1, appender.length(), "Count UTF-8 bytes, not characters");
            assertEquals(existing + euro, Files.readString(_activeFile));
            appender.append("next");
            assertEquals(4, appender.length());
            appender.assertNoErrors();
        }
        assertEquals(existing + euro, readArchive(TODAY, 0, compressed));
        assertEquals("next", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void previousDayUsesItsOwnArchiveSequence(final boolean restart, final boolean compressed) throws IOException
    {
        writeActive("yesterday", NOW.minusSeconds(86400));
        writeArchive(TODAY.minusDays(1), 2, compressed, "old-archive");
        writeArchive(TODAY, 4, compressed, "today-archive");
        try (final TestAppender appender = new TestAppender(restart, compressed))
        {
            appender.start();
            appender.append(FULL_FILE);
            appender.append("today");
            appender.assertNoErrors();
        }
        assertEquals("old-archive", readArchive(TODAY.minusDays(1), 2, compressed));
        assertEquals("yesterday", readArchive(TODAY.minusDays(1), 3, compressed));
        assertEquals("today-archive", readArchive(TODAY, 4, compressed));
        assertEquals(FULL_FILE, readArchive(TODAY, 5, compressed));
        assertEquals("today", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "empty", "empty-previous-day"})
    void restartDoesNotArchiveEmptyOrMissingFiles(final String initialState) throws IOException
    {
        if (!"missing".equals(initialState))
        {
            writeActive("", "empty".equals(initialState) ? NOW : NOW.minusSeconds(86400));
        }
        try (final TestAppender appender = new TestAppender(true, false))
        {
            appender.start();
            appender.append("first");
            appender.append("second");
            assertEquals(11, appender.length());
            appender.assertNoErrors();
        }
        assertEquals("firstsecond", Files.readString(_activeFile));
        try (final Stream<Path> files = Files.list(_directory))
        {
            assertEquals(List.of(_activeFile), files.toList());
        }
    }

    @Test
    void restartRolloverWaitsForTheFirstEvent() throws IOException
    {
        writeActive("old", NOW);
        try (final TestAppender appender = new TestAppender(true, false))
        {
            appender.start();
            assertEquals("old", Files.readString(_activeFile));
            assertFalse(Files.exists(archive(TODAY, 0, false)));
        }
        assertEquals("old", Files.readString(_activeFile));
    }

    @Test
    void interruptedCompressionReservesItsArchiveIndex() throws IOException
    {
        writeActive("old", NOW);
        writeArchive(TODAY, 2, true, "compressed");
        writeArchive(TODAY, 5, false, "pending-compression");
        try (final TestAppender appender = new TestAppender(true, true))
        {
            appender.start();
            appender.append("new");
            appender.assertNoErrors();
        }
        assertEquals("compressed", readArchive(TODAY, 2, true));
        assertEquals("pending-compression", readArchive(TODAY, 5, false));
        assertEquals("old", readArchive(TODAY, 6, true));
    }

    @Test
    void archiveScanMatchesTheLiteralFileName() throws IOException
    {
        _activeFile = _directory.resolve("broker[1].log");
        writeActive("old", NOW);
        writeArchive(TODAY, 2, false, "existing");
        final Path unrelated = _directory.resolve("broker1Xlog." + TODAY + ".99");
        Files.writeString(unrelated, "unrelated");
        try (final TestAppender appender = new TestAppender(true, false))
        {
            appender.start();
            appender.append("new");
            appender.assertNoErrors();
        }
        assertEquals("old", readArchive(TODAY, 3, false));
        assertEquals("unrelated", Files.readString(unrelated));
    }

    @ParameterizedTest
    @CsvSource({"2026-03-29T00:00:00+01:00,2026-03-30T00:00:00+02:00",
            "2026-10-25T00:00:00+02:00,2026-10-26T00:00:00+01:00"})
    void dailyRolloverFollowsLocalMidnightAcrossDaylightSaving(final String start, final String midnight)
            throws IOException
    {
        final Instant initialTime = Instant.parse(start);
        final Instant nextMidnight = Instant.parse(midnight);
        final LocalDate date = initialTime.atZone(ZoneId.of("Europe/Prague")).toLocalDate();
        writeActive("old", initialTime);
        try (final TestAppender appender = new TestAppender(false, false))
        {
            appender._rollingPolicy.setFileNamePattern(_activeFile + ".%d{yyyy-MM-dd,Europe/Prague}.%i");
            appender._triggeringPolicy.setCurrentTime(initialTime.toEpochMilli());
            appender.start();
            appender._triggeringPolicy.setCurrentTime(nextMidnight.toEpochMilli() - 1);
            appender.append("before");
            assertFalse(Files.exists(archive(date, 0, false)));
            appender._triggeringPolicy.setCurrentTime(nextMidnight.toEpochMilli());
            appender.append(FULL_FILE);
            assertEquals(MAX_FILE_SIZE, appender.length());
            appender.append("after");
            assertEquals(5, appender.length());
            appender.assertNoErrors();
        }
        assertEquals("oldbefore", readArchive(date, 0, false));
        assertEquals(FULL_FILE, readArchive(date.plusDays(1), 0, false));
        assertEquals("after", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(strings = {"2147483647", "999999999999999999999999"})
    void unsafeArchiveIndexPreventsStartup(final String index) throws IOException
    {
        writeActive("old", NOW);
        final Path existing = Path.of(_activeFile + "." + TODAY + "." + index);
        Files.writeString(existing, "existing");
        try (final TestAppender appender = new TestAppender(true, false))
        {
            appender.startAppender();
            assertFalse(appender._triggeringPolicy.isStarted());
            assertFalse(appender._rollingPolicy.isStarted());
            assertFalse(appender._appender.isStarted());
            assertTrue(appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .anyMatch(status -> status.getLevel() == Status.ERROR));
        }
        assertEquals("old", Files.readString(_activeFile));
        assertEquals("existing", Files.readString(existing));
    }

    @Test
    void archiveScanFailureKeepsLoggingAndRetries() throws IOException
    {
        final Path archiveDirectory = Files.createDirectory(_directory.resolve("archives"));
        writeActive("old", NOW);
        try (final TestAppender appender = new TestAppender(false, false))
        {
            appender._rollingPolicy.setFileNamePattern(archiveDirectory.resolve("broker.log") +
                    ".%d{yyyy-MM-dd,UTC}.%i");
            appender.start();
            Files.delete(archiveDirectory);
            Files.writeString(archiveDirectory, "blocked");
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(86400).toEpochMilli());
            appender.append("during-failure");
            assertEquals("oldduring-failure", Files.readString(_activeFile));

            Files.delete(archiveDirectory);
            Files.createDirectory(archiveDirectory);
            appender.append("during-backoff");
            assertEquals("oldduring-failureduring-backoff", Files.readString(_activeFile));
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(86431).toEpochMilli());
            appender.append("recovered");
            assertEquals(9, appender.length());
        }
        assertEquals("oldduring-failureduring-backoff",
                Files.readString(archiveDirectory.resolve("broker.log." + TODAY + ".0")));
        assertEquals("recovered", Files.readString(_activeFile));
    }

    @Test
    void archiveScanFailureDoesNotReenterThroughStatusListener() throws IOException
    {
        final Path archiveDirectory = Files.createDirectory(_directory.resolve("archives"));
        writeActive("old", NOW);
        final BrokerFileLogger<?> brokerLogger = mock(BrokerFileLogger.class);
        final SystemConfig<?> systemConfig = mock(SystemConfig.class);
        when(brokerLogger.getContextValue(Boolean.class, BrokerFileLogger.BROKER_FAIL_ON_LOGGER_IO_ERROR))
                .thenReturn(false);
        final BrokerLoggerStatusListener listener = new BrokerLoggerStatusListener(brokerLogger, systemConfig,
                BrokerFileLogger.BROKER_FAIL_ON_LOGGER_IO_ERROR, IOException.class);

        try (final TestAppender appender = new TestAppender(false, false))
        {
            appender._rollingPolicy.setFileNamePattern(archiveDirectory.resolve("broker.log") +
                    ".%d{yyyy-MM-dd,UTC}.%i");
            appender.start();
            final AtomicInteger errors = new AtomicInteger();
            final StatusListener boundedListener = status ->
            {
                if (status.getOrigin() == appender._triggeringPolicy && status.getLevel() == Status.ERROR)
                {
                    if (errors.incrementAndGet() == 1)
                    {
                        listener.addStatusEvent(status);
                    }
                }
            };
            final Logger statusLogger = (Logger) LoggerFactory.getLogger(BrokerLoggerStatusListener.class);
            final Level originalLevel = statusLogger.getLevel();
            final boolean originalAdditive = statusLogger.isAdditive();
            final TurboFilter statusFilter = new TurboFilter()
            {
                @Override
                public FilterReply decide(final Marker marker, final Logger logger, final Level level,
                                          final String format, final Object[] params, final Throwable throwable)
                {
                    return logger == statusLogger && level == Level.ERROR ? FilterReply.ACCEPT : FilterReply.NEUTRAL;
                }
            };
            statusFilter.start();
            statusLogger.getLoggerContext().getTurboFilterList().add(0, statusFilter);
            statusLogger.setLevel(Level.ERROR);
            statusLogger.setAdditive(false);
            statusLogger.addAppender(appender._appender);
            appender._context.getStatusManager().add(boundedListener);
            try
            {
                Files.delete(archiveDirectory);
                Files.writeString(archiveDirectory, "blocked");
                appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(86400).toEpochMilli());
                appender.append("during-failure");
                assertEquals(1, errors.get(), "The listener must not recursively report the archive scan failure");
                final String failureContent = Files.readString(_activeFile);
                assertTrue(failureContent.contains("Unexpected error whilst trying to store log entry."));
                assertTrue(failureContent.endsWith("during-failure"));

                appender.append("during-backoff");
                appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(86431).toEpochMilli());
                appender.append("during-retry");
                assertEquals(1, errors.get(), "Retries must not report the same failure again");
                final String expectedArchive = failureContent + "during-backoffduring-retry";
                assertEquals(expectedArchive, Files.readString(_activeFile));

                Files.delete(archiveDirectory);
                Files.createDirectory(archiveDirectory);
                appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(86462).toEpochMilli());
                appender.append("recovered");
                assertEquals("recovered", Files.readString(_activeFile));
                assertEquals(expectedArchive,
                        Files.readString(archiveDirectory.resolve("broker.log." + TODAY + ".0")));
                assertEquals(1, errors.get());
                verify(brokerLogger, never()).stopLogging();
                verify(systemConfig, never()).closeAsync();
            }
            finally
            {
                appender._context.getStatusManager().remove(boundedListener);
                statusLogger.detachAppender(appender._appender);
                statusLogger.getLoggerContext().getTurboFilterList().remove(statusFilter);
                statusFilter.stop();
                statusLogger.setLevel(originalLevel);
                statusLogger.setAdditive(originalAdditive);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rolloverFailurePreservesActiveFile(final boolean compressed) throws IOException
    {
        writeActive("old", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.start();
            doThrow(new RolloverFailure("test failure")).doCallRealMethod().when(appender._rollingPolicy).rollover();
            appender.append("new");
            assertEquals("oldnew", Files.readString(_activeFile));
            assertEquals(Files.size(_activeFile), appender.length());
            appender.append(FULL_FILE);
            assertEquals(Files.size(_activeFile), appender.length());
            verify(appender._rollingPolicy).rollover();
            writeArchive(TODAY, 0, compressed, "existing");
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(30).toEpochMilli());
            appender.append("after");
            assertEquals(5, appender.length());
            verify(appender._rollingPolicy, times(2)).rollover();
            appender.assertNoErrors();
        }
        assertEquals("existing", readArchive(TODAY, 0, compressed));
        assertEquals("oldnew" + FULL_FILE, readArchive(TODAY, 1, compressed));
        assertEquals("after", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restartRolloverRetriesRepeatedFailuresBelowSizeLimit(final boolean compressed) throws IOException
    {
        writeActive("old", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.start();
            doThrow(new RolloverFailure("first failure"), new RolloverFailure("second failure"))
                    .doCallRealMethod().when(appender._rollingPolicy).rollover();
            appender.append("one");
            assertEquals(Files.size(_activeFile), appender.length());
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(29).toEpochMilli());
            appender.append("two");
            verify(appender._rollingPolicy).rollover();

            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(30).toEpochMilli());
            appender.append("three");
            assertEquals(Files.size(_activeFile), appender.length());
            verify(appender._rollingPolicy, times(2)).rollover();
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(59).toEpochMilli());
            appender.append("four");
            verify(appender._rollingPolicy, times(2)).rollover();
            assertTrue(appender.length() < MAX_FILE_SIZE);

            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(60).toEpochMilli());
            appender.append("recovered");
            assertEquals(9, appender.length());
            verify(appender._rollingPolicy, times(3)).rollover();
            appender.assertNoErrors();
        }
        assertEquals("oldonetwothreefour", readArchive(TODAY, 0, compressed));
        assertEquals("recovered", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void failedRolloverCountsExistingBytesAcrossSizeLimit(final boolean restart, final boolean compressed)
            throws IOException
    {
        final String existing = "x".repeat(restart ? MAX_FILE_SIZE - 4 : MAX_FILE_SIZE);
        writeActive(existing, NOW);
        try (final TestAppender appender = new TestAppender(restart, compressed))
        {
            appender.start();
            doThrow(new RolloverFailure("test failure")).doCallRealMethod().when(appender._rollingPolicy).rollover();
            for (final String message : List.of("123", "45", "6"))
            {
                appender.append(message);
                assertEquals(Files.size(_activeFile), appender.length());
            }
            assertTrue(appender.length() > MAX_FILE_SIZE);
            verify(appender._rollingPolicy).rollover();

            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(30).toEpochMilli());
            appender.append("next");
            assertEquals(4, appender.length());
            verify(appender._rollingPolicy, times(2)).rollover();
            appender.assertNoErrors();
        }
        assertEquals(existing + "123456", readArchive(TODAY, 0, compressed));
        assertEquals("next", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void dailyRolloverFailurePreservesArchiveDateAndNumbering(final boolean compressed) throws IOException
    {
        writeActive("yesterday", NOW.minusSeconds(86400));
        writeArchive(TODAY.minusDays(1), 2, compressed, "yesterday-archive");
        writeArchive(TODAY, 4, compressed, "today-archive");
        try (final TestAppender appender = new TestAppender(false, compressed))
        {
            appender.start();
            doThrow(new RolloverFailure("test failure")).doCallRealMethod().when(appender._rollingPolicy).rollover();
            appender.append("new");
            assertEquals(Files.size(_activeFile), appender.length());
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(30).toEpochMilli());
            appender.append(FULL_FILE);
            assertEquals(MAX_FILE_SIZE, appender.length());
            appender.append("next");
            assertEquals(4, appender.length());
            appender.assertNoErrors();
        }
        assertEquals("yesterday-archive", readArchive(TODAY.minusDays(1), 2, compressed));
        assertEquals("yesterdaynew", readArchive(TODAY.minusDays(1), 3, compressed));
        assertEquals("today-archive", readArchive(TODAY, 4, compressed));
        assertEquals(FULL_FILE, readArchive(TODAY, 5, compressed));
        assertEquals("next", Files.readString(_activeFile));
    }

    @Test
    void rolloverFailureRetainsCounterWhenFileAttributesCannotBeRead() throws IOException
    {
        writeActive("old", NOW);
        try (final TestAppender appender = new TestAppender(true, false))
        {
            appender.start();
            doThrow(new RolloverFailure("test failure")).doCallRealMethod().when(appender._rollingPolicy).rollover();
            final AtomicBoolean failNextRead = new AtomicBoolean(true);
            try (final MockedStatic<Files> mockedFiles = mockStatic(Files.class, invocation ->
            {
                if ("readAttributes".equals(invocation.getMethod().getName()) &&
                        _activeFile.equals(invocation.getArgument(0)) && failNextRead.compareAndSet(true, false))
                {
                    throw new IOException("Temporary file attribute failure");
                }
                return invocation.callRealMethod();
            }))
            {
                appender.append("new");
            }
            assertFalse(failNextRead.get(), "The file size read after the failed rollover must fail");
            assertEquals("oldnew", Files.readString(_activeFile));
            assertEquals(Files.size(_activeFile), appender.length());
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(30).toEpochMilli());
            appender.append("recovered");
            assertEquals(9, appender.length());
            appender.assertNoErrors();
        }
        assertEquals("oldnew", readArchive(TODAY, 0, false));
        assertEquals("recovered", Files.readString(_activeFile));
    }

    @Test
    void archiveScanFailureAfterFailedRolloverIsReportedOnce() throws IOException
    {
        final Path archiveDirectory = Files.createDirectory(_directory.resolve("archives"));
        writeActive("old", NOW);
        try (final TestAppender appender = new TestAppender(true, false))
        {
            appender._rollingPolicy.setFileNamePattern(archiveDirectory.resolve("broker.log") +
                    ".%d{yyyy-MM-dd,UTC}.%i");
            appender.start();
            doThrow(new RolloverFailure("test failure")).doCallRealMethod().when(appender._rollingPolicy).rollover();
            appender.append("new");
            appender.assertNoErrors();

            Files.delete(archiveDirectory);
            Files.writeString(archiveDirectory, "blocked");
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(30).toEpochMilli());
            appender.append("first-retry");
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(60).toEpochMilli());
            appender.append("second-retry");
            assertEquals(Files.size(_activeFile), appender.length());
            assertEquals(1L, appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .filter(status -> status.getOrigin() == appender._triggeringPolicy &&
                            status.getLevel() == Status.ERROR && status.getThrowable() instanceof IOException).count());
            verify(appender._rollingPolicy).rollover();

            Files.delete(archiveDirectory);
            Files.createDirectory(archiveDirectory);
            appender._triggeringPolicy.setCurrentTime(NOW.plusSeconds(90).toEpochMilli());
            appender.append("recovered");
            assertEquals(9, appender.length());
            verify(appender._rollingPolicy, times(2)).rollover();
        }
        assertEquals("oldnewfirst-retrysecond-retry",
                Files.readString(archiveDirectory.resolve("broker.log." + TODAY + ".0")));
        assertEquals("recovered", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nativeRetentionRemovesExpiredDaysAndKeepsAllRecentIndices(final boolean compressed) throws IOException
    {
        writeActive("old", NOW);
        writeArchive(TODAY.minusDays(3), 0, compressed, "expired-zero");
        writeArchive(TODAY.minusDays(3), 4, compressed, "expired-four");
        writeArchive(TODAY.minusDays(1), 0, compressed, "retained-zero");
        writeArchive(TODAY.minusDays(1), 4, compressed, "retained-four");
        final Path unrelated = _directory.resolve("other.log." + TODAY.minusDays(3) + ".0");
        Files.writeString(unrelated, "unrelated");
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender._rollingPolicy.setMaxHistory(2);
            appender.start();
            appender.append("new");
        }
        assertFalse(Files.exists(archive(TODAY.minusDays(3), 0, compressed)));
        assertFalse(Files.exists(archive(TODAY.minusDays(3), 4, compressed)));
        assertEquals("retained-zero", readArchive(TODAY.minusDays(1), 0, compressed));
        assertEquals("retained-four", readArchive(TODAY.minusDays(1), 4, compressed));
        assertEquals("old", readArchive(TODAY, 0, compressed));
        assertEquals("unrelated", Files.readString(unrelated));
    }

    @Test
    void headerBytesAreCountedWithoutCausingRestartRollover() throws IOException
    {
        writeActive("", NOW);
        final String header;
        final String content;
        try (final TestAppender appender = new TestAppender(true, false))
        {
            appender._encoder.setOutputPatternAsHeader(true);
            final byte[] headerBytes = appender._encoder.headerBytes();
            header = new String(headerBytes, StandardCharsets.UTF_8);
            content = "x".repeat(MAX_FILE_SIZE - headerBytes.length);
            appender.start();
            assertEquals(headerBytes.length, appender.length());
            appender.append(content);
            assertFalse(Files.exists(archive(TODAY, 0, false)), "The encoder header is not a preexisting log");
            assertEquals(MAX_FILE_SIZE, appender.length());
            appender.append("next");
            assertEquals(headerBytes.length + 4, appender.length());
            appender.assertNoErrors();
        }
        assertEquals(header + content, readArchive(TODAY, 0, false));
        assertEquals(header + "next", Files.readString(_activeFile));
    }

    @Test
    void concurrentLoggingPreservesEveryEventAcrossSizeRollovers() throws Exception
    {
        final int threadsCount = 4;
        final int iterationsCount = 100;
        final Set<String> expected = new HashSet<>();
        final List<Future<?>> writes = new ArrayList<>();
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        try (final TestAppender appender = new TestAppender(false, false))
        {
            appender.start();
            for (int i = 0; i < threadsCount; i++)
            {
                final String prefix = "thread-" + i + "-";
                for (int j = 0; j < iterationsCount; j++)
                {
                    expected.add(prefix + j);
                }
                writes.add(executor.submit(() ->
                {
                    for (int j = 0; j < iterationsCount; j++)
                    {
                        appender.append(prefix + j + "\n");
                    }
                }));
            }
            for (final Future<?> write : writes)
            {
                write.get(10, TimeUnit.SECONDS);
            }
            appender.assertNoErrors();
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        final List<String> actual = new ArrayList<>();
        try (final Stream<Path> paths = Files.list(_directory))
        {
            for (final Path path : paths.toList())
            {
                actual.addAll(Files.readAllLines(path));
            }
        }
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, new HashSet<>(actual));
    }

    private void writeActive(final String content, final Instant modified) throws IOException
    {
        Files.writeString(_activeFile, content);
        Files.setLastModifiedTime(_activeFile, FileTime.from(modified));
    }

    private Path archive(final LocalDate date, final int index, final boolean compressed)
    {
        return Path.of(_activeFile + "." + date + "." + index + (compressed ? ".gz" : ""));
    }

    private void writeArchive(final LocalDate date, final int index, final boolean compressed, final String content)
            throws IOException
    {
        final Path path = archive(date, index, compressed);
        if (compressed)
        {
            try (final GZIPOutputStream output = new GZIPOutputStream(Files.newOutputStream(path)))
            {
                output.write(content.getBytes(StandardCharsets.UTF_8));
            }
        }
        else
        {
            Files.writeString(path, content);
        }
    }

    private String readArchive(final LocalDate date, final int index, final boolean compressed) throws IOException
    {
        final Path path = archive(date, index, compressed);
        if (compressed)
        {
            try (final GZIPInputStream input = new GZIPInputStream(Files.newInputStream(path)))
            {
                return new String(input.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        return Files.readString(path);
    }

    private final class TestAppender implements AutoCloseable
    {
        private final LoggerContext _context = new LoggerContext();
        private final RollingFileAppender<ILoggingEvent> _appender = new RollingFileAppender<>();
        private final TimeBasedRollingPolicy<ILoggingEvent> _rollingPolicy = spy(new TimeBasedRollingPolicy<>());
        private final DailyTriggeringPolicy<ILoggingEvent> _triggeringPolicy;
        private final PatternLayoutEncoder _encoder = new PatternLayoutEncoder();
        private final Logger _logger;

        private TestAppender(final boolean restart, final boolean compressed)
        {
            _context.setMDCAdapter(new LogbackMDCAdapter());
            _context.start();
            _appender.setContext(_context);
            _appender.setName("test-appender");
            _appender.setFile(_activeFile.toString());
            _triggeringPolicy = new DailyTriggeringPolicy<>(new FileSize(MAX_FILE_SIZE), restart);
            _triggeringPolicy.setCurrentTime(NOW.toEpochMilli());
            _rollingPolicy.setContext(_context);
            _rollingPolicy.setParent(_appender);
            _rollingPolicy.setFileNamePattern(_activeFile + ".%d{yyyy-MM-dd,UTC}.%i" + (compressed ? ".gz" : ""));
            _rollingPolicy.setTimeBasedFileNamingAndTriggeringPolicy(_triggeringPolicy);
            _encoder.setContext(_context);
            _encoder.setCharset(StandardCharsets.UTF_8);
            _encoder.setPattern("%msg");
            _encoder.start();
            _appender.setEncoder(_encoder);
            _logger = _context.getLogger(getTestName());
            _logger.setAdditive(false);
            _logger.addAppender(_appender);
        }

        private void start()
        {
            startAppender();
            assertTrue(_appender.isStarted(), () -> _context.getStatusManager().getCopyOfStatusList().toString());
        }

        private void startAppender()
        {
            final RollingPolicyDecorator decorator = new RollingPolicyDecorator(_rollingPolicy,
                    mock(RollingPolicyDecorator.RolloverListener.class), mock(ScheduledExecutorService.class));
            _appender.setRollingPolicy(decorator);
            _appender.setTriggeringPolicy(_triggeringPolicy);
            decorator.start();
            _appender.start();
        }

        private void append(final String message)
        {
            _logger.info(message);
        }

        private long length()
        {
            return _triggeringPolicy.getLengthCounter().getLength();
        }

        private void assertNoErrors()
        {
            final List<Status> statuses = _context.getStatusManager().getCopyOfStatusList();
            assertFalse(statuses.stream().anyMatch(status -> status.getLevel() == Status.ERROR), statuses::toString);
        }

        @Override
        public void close()
        {
            _appender.stop();
            _rollingPolicy.stop();
            _triggeringPolicy.stop();
            _encoder.stop();
            _context.stop();
        }
    }
}
