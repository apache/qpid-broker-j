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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.util.FileSize;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

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
    void testRestartRollsBeforeFirstEvent(final boolean compressed) throws Exception
    {
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            assertEquals("previous", Files.readString(_activeFile));
            assertEquals(List.of(_activeFile), files());
            appender.append("current");
        }
        assertEquals("previous", readArchive(TODAY, 0, compressed));
        assertEquals("current", Files.readString(_activeFile));
        assertEquals(2, files().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRestartAndSizeRolloversContinueArchiveNumbering(final boolean compressed) throws Exception
    {
        writeArchive(TODAY, 0, compressed, "first archive");
        writeArchive(TODAY, 4, compressed, "fourth archive");
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.append(FULL_FILE);
            appender.append("last");
        }
        assertEquals("previous", readArchive(TODAY, 5, compressed));
        assertEquals(FULL_FILE, readArchive(TODAY, 6, compressed));
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.append("next restart");
        }
        assertEquals("first archive", readArchive(TODAY, 0, compressed));
        assertEquals("fourth archive", readArchive(TODAY, 4, compressed));
        assertEquals("last", readArchive(TODAY, 7, compressed));
        assertEquals("next restart", Files.readString(_activeFile));
        assertEquals(6, files().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRestartDisabledStillCountsExistingBytesAndEncodedBytes(final boolean compressed) throws Exception
    {
        final String previous = "x".repeat(MAX_FILE_SIZE - 1);
        writeActive(previous, NOW);
        try (final TestAppender appender = new TestAppender(false, compressed))
        {
            appender.append("é");
            assertEquals(previous + "é", Files.readString(_activeFile));
            assertEquals(List.of(_activeFile), files());
            appender.append("new file");
        }
        assertEquals(previous + "é", readArchive(TODAY, 0, compressed));
        assertEquals("new file", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testMissingAndOldEmptyFilesDoNotCreateArchives(final boolean existing, final boolean compressed)
            throws Exception
    {
        if (existing)
        {
            writeActive("", NOW.minus(2, ChronoUnit.DAYS));
        }
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender._triggeringPolicy.setCurrentTime(NOW.plus(1, ChronoUnit.DAYS).toEpochMilli());
            appender.append("first message");
        }
        assertEquals("first message", Files.readString(_activeFile));
        assertEquals(List.of(_activeFile), files());
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testPreviousDayUsesItsOwnSequence(final boolean restart, final boolean compressed) throws Exception
    {
        final LocalDate yesterday = TODAY.minusDays(1);
        writeArchive(yesterday, 2, compressed, "yesterday archive");
        writeArchive(TODAY, 3, compressed, "today archive");
        writeActive("yesterday active", NOW.minus(1, ChronoUnit.DAYS));
        try (final TestAppender appender = new TestAppender(restart, compressed))
        {
            appender.append(FULL_FILE);
            appender.append("today active");
        }
        assertEquals("yesterday archive", readArchive(yesterday, 2, compressed));
        assertEquals("yesterday active", readArchive(yesterday, 3, compressed));
        assertEquals("today archive", readArchive(TODAY, 3, compressed));
        assertEquals(FULL_FILE, readArchive(TODAY, 4, compressed));
        assertEquals("today active", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDayBoundaryAfterRestart(final boolean compressed) throws Exception
    {
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.append("today");
            appender._triggeringPolicy.setCurrentTime(NOW.plus(1, ChronoUnit.DAYS).toEpochMilli());
            appender.append("tomorrow");
        }
        assertEquals("previous", readArchive(TODAY, 0, compressed));
        assertEquals("today", readArchive(TODAY, 1, compressed));
        assertEquals("tomorrow", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testArchiveNumberingWithSpecialCharactersInFileName(final boolean compressed) throws Exception
    {
        _activeFile = _directory.resolve("broker[1]+(test).log");
        writeArchive(TODAY, 10, compressed, "existing");
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.append("current");
        }
        assertEquals("existing", readArchive(TODAY, 10, compressed));
        assertEquals("previous", readArchive(TODAY, 11, compressed));
    }

    @Test
    void testUncompressedArchiveReservesItsIndexDuringCompression() throws Exception
    {
        writeArchive(TODAY, 2, true, "compressed");
        writeArchive(TODAY, 4, false, "pending compression");
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, true))
        {
            appender.append("current");
        }
        assertEquals("compressed", readArchive(TODAY, 2, true));
        assertEquals("pending compression", readArchive(TODAY, 4, false));
        assertEquals("previous", readArchive(TODAY, 5, true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRestartRetainsLogbackHistoryCleanup(final boolean compressed) throws Exception
    {
        final LocalDate expired = TODAY.minusDays(3);
        writeArchive(expired, 0, compressed, "expired");
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed, 1))
        {
            appender.append("current");
        }
        assertFalse(Files.exists(archive(expired, 0, compressed)));
        assertEquals("previous", readArchive(TODAY, 0, compressed));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testStartupWithDirectoryScanFailure(final boolean restart, final boolean compressed) throws Exception
    {
        writeArchive(TODAY, 0, compressed, "first archive");
        writeArchive(TODAY, 2, false, "uncompressed archive");
        writeActive("previous", NOW);
        try (final MockedStatic<Files> ignored = denyArchiveAccess(null);
             final TestAppender appender = new TestAppender(restart, compressed))
        {
            appender.append(FULL_FILE);
            appender.append("current");
        }
        assertEquals("first archive", readArchive(TODAY, 0, compressed));
        assertEquals("uncompressed archive", readArchive(TODAY, 2, false));
        assertEquals(restart ? "previous" : "previous" + FULL_FILE, readArchive(TODAY, 1, compressed));
        if (restart)
        {
            assertEquals(FULL_FILE, readArchive(TODAY, 3, compressed));
        }
        assertEquals("current", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDayBoundaryAndSizeRolloverWithDirectoryScanFailure(final boolean compressed) throws Exception
    {
        final LocalDate tomorrow = TODAY.plusDays(1);
        writeArchive(tomorrow, 0, compressed, "first archive");
        writeArchive(tomorrow, 2, compressed, "second archive");
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.append("today");
            try (final MockedStatic<Files> ignored = denyArchiveAccess(null))
            {
                appender._triggeringPolicy.setCurrentTime(NOW.plus(1, ChronoUnit.DAYS).toEpochMilli());
                appender.append(FULL_FILE);
                appender.append("tomorrow");
            }
        }
        assertEquals("previous", readArchive(TODAY, 0, compressed));
        assertEquals("today", readArchive(TODAY, 1, compressed));
        assertEquals("first archive", readArchive(tomorrow, 0, compressed));
        assertEquals(FULL_FILE, readArchive(tomorrow, 1, compressed));
        assertEquals("second archive", readArchive(tomorrow, 2, compressed));
        assertEquals("tomorrow", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDirectoryScanRecoveryResumesAfterHighestIndex(final boolean compressed) throws Exception
    {
        writeArchive(TODAY, 0, compressed, "first archive");
        writeArchive(TODAY, 4, compressed, "fourth archive");
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            try (final MockedStatic<Files> ignored = denyArchiveAccess(null))
            {
                appender.append(FULL_FILE);
            }
            appender._triggeringPolicy.setCurrentTime(NOW.plus(1, ChronoUnit.MINUTES).toEpochMilli());
            appender.append("after recovery");
        }
        assertEquals("first archive", readArchive(TODAY, 0, compressed));
        assertEquals("previous", readArchive(TODAY, 1, compressed));
        assertEquals("fourth archive", readArchive(TODAY, 4, compressed));
        assertEquals(FULL_FILE, readArchive(TODAY, 5, compressed));
        assertEquals("after recovery", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @CsvSource({"true,false,false", "true,false,true", "false,false,false", "false,false,true", "false,true,false",
            "false,true,true"})
    void testOversizedArchiveIndexUsesSafeArchiveNames(final boolean restart, final boolean newDay,
                                                     final boolean compressed) throws Exception
    {
        final String previous = restart || newDay ? "previous" : FULL_FILE;
        writeArchive(TODAY, 0, compressed, "first archive");
        writeArchive(TODAY, 2, false, "uncompressed archive");
        final Path oversizedArchive = writeOversizedArchive(compressed);
        final byte[] oversizedContent = Files.readAllBytes(oversizedArchive);
        writeActive(previous, NOW);
        try (final TestAppender appender = new TestAppender(restart, compressed))
        {
            if (newDay)
            {
                appender._triggeringPolicy.setCurrentTime(NOW.plus(1, ChronoUnit.DAYS).toEpochMilli());
            }
            appender.append("current");
        }
        assertEquals("first archive", readArchive(TODAY, 0, compressed));
        assertEquals(previous, readArchive(TODAY, 1, compressed));
        assertEquals("uncompressed archive", readArchive(TODAY, 2, false));
        assertArrayEquals(oversizedContent, Files.readAllBytes(oversizedArchive));
        assertEquals("current", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOversizedArchiveIndexRetriesScanningAfterDelay(final boolean compressed) throws Exception
    {
        writeArchive(TODAY, 0, compressed, "first archive");
        writeArchive(TODAY, 10, compressed, "tenth archive");
        final Path oversizedArchive = writeOversizedArchive(compressed);
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.append(FULL_FILE);
            final long warnings = appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .filter(status -> status.getEffectiveLevel() == Status.WARN).count();
            appender._triggeringPolicy.setCurrentTime(NOW.plusMillis(29_999).toEpochMilli());
            appender.append(FULL_FILE);
            assertEquals(warnings, appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .filter(status -> status.getEffectiveLevel() == Status.WARN).count());
            appender._triggeringPolicy.setCurrentTime(NOW.plusMillis(30_000).toEpochMilli());
            appender.append(FULL_FILE);
            assertEquals(warnings + 1, appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .filter(status -> status.getEffectiveLevel() == Status.WARN).count());
            Files.delete(oversizedArchive);
            appender._triggeringPolicy.setCurrentTime(NOW.plusMillis(59_999).toEpochMilli());
            appender.append(FULL_FILE);
            assertEquals(warnings + 1, appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .filter(status -> status.getEffectiveLevel() == Status.WARN).count());
            appender._triggeringPolicy.setCurrentTime(NOW.plusMillis(60_000).toEpochMilli());
            appender.append("after recovery");
            assertEquals(warnings + 1, appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .filter(status -> status.getEffectiveLevel() == Status.WARN).count());
        }
        assertEquals("first archive", readArchive(TODAY, 0, compressed));
        assertEquals("previous", readArchive(TODAY, 1, compressed));
        for (int index = 2; index <= 4; index++)
        {
            assertEquals(FULL_FILE, readArchive(TODAY, index, compressed));
        }
        assertEquals("tenth archive", readArchive(TODAY, 10, compressed));
        assertEquals(FULL_FILE, readArchive(TODAY, 11, compressed));
        assertEquals("after recovery", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIndexOverflowKeepsAppendingAndRetries(final boolean compressed) throws Exception
    {
        writeArchive(TODAY, Integer.MAX_VALUE, compressed, "last archive");
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            appender.append("first");
            final long warnings = appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .filter(status -> status.getEffectiveLevel() == Status.WARN).count();
            appender._triggeringPolicy.setCurrentTime(NOW.plusMillis(29_999).toEpochMilli());
            appender.append("second");
            assertEquals("previousfirstsecond", Files.readString(_activeFile));
            assertEquals("last archive", readArchive(TODAY, Integer.MAX_VALUE, compressed));
            assertEquals(warnings, appender._context.getStatusManager().getCopyOfStatusList().stream()
                    .filter(status -> status.getEffectiveLevel() == Status.WARN).count());
            Files.delete(archive(TODAY, Integer.MAX_VALUE, compressed));
            appender._triggeringPolicy.setCurrentTime(NOW.plusMillis(30_000).toEpochMilli());
            appender.append("after recovery");
        }
        assertEquals("previousfirstsecond", readArchive(TODAY, 0, compressed));
        assertEquals("after recovery", Files.readString(_activeFile));
        assertEquals(2, files().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnknownArchivePreservesWritesAndRetriesRestartRollover(final boolean compressed) throws Exception
    {
        writeArchive(TODAY, 0, compressed, "existing archive");
        writeActive("previous", NOW);
        try (final TestAppender appender = new TestAppender(true, compressed))
        {
            try (final MockedStatic<Files> ignored = denyArchiveAccess(archive(TODAY, 0, compressed)))
            {
                appender.append("first");
                final long warnings = appender._context.getStatusManager().getCopyOfStatusList().stream()
                        .filter(status -> status.getEffectiveLevel() == Status.WARN).count();
                appender.append("second");
                assertEquals(warnings, appender._context.getStatusManager().getCopyOfStatusList().stream()
                        .filter(status -> status.getEffectiveLevel() == Status.WARN).count());
                assertEquals("previousfirstsecond", Files.readString(_activeFile));
            }
            appender.append("waiting");
            assertEquals("previousfirstsecondwaiting", Files.readString(_activeFile));
            appender._triggeringPolicy.setCurrentTime(NOW.plus(1, ChronoUnit.MINUTES).toEpochMilli());
            appender.append("after recovery");
        }
        assertEquals("existing archive", readArchive(TODAY, 0, compressed));
        assertEquals("previousfirstsecondwaiting", readArchive(TODAY, 1, compressed));
        assertEquals("after recovery", Files.readString(_activeFile));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testUnknownArchiveKeepsSizeOrDailyRolloverPending(final boolean newDay, final boolean compressed)
            throws Exception
    {
        final String previous = newDay ? "previous" : FULL_FILE;
        final Instant now = newDay ? NOW.plus(1, ChronoUnit.DAYS) : NOW;
        writeArchive(TODAY, 0, compressed, "existing archive");
        writeActive(previous, NOW);
        try (final TestAppender appender = new TestAppender(false, compressed))
        {
            appender._triggeringPolicy.setCurrentTime(now.toEpochMilli());
            try (final MockedStatic<Files> ignored = denyArchiveAccess(archive(TODAY, 0, compressed)))
            {
                appender.append("pending");
                assertEquals(previous + "pending", Files.readString(_activeFile));
            }
            appender._triggeringPolicy.setCurrentTime(now.plus(1, ChronoUnit.MINUTES).toEpochMilli());
            appender.append("after recovery");
        }
        assertEquals("existing archive", readArchive(TODAY, 0, compressed));
        assertEquals(previous + "pending", readArchive(TODAY, 1, compressed));
        assertEquals("after recovery", Files.readString(_activeFile));
    }

    private MockedStatic<Files> denyArchiveAccess(final Path inaccessibleArchive)
    {
        final Path directory = _directory.toAbsolutePath();
        return mockStatic(Files.class, invocation ->
        {
            final String methodName = invocation.getMethod().getName();
            if ("newDirectoryStream".equals(methodName) && invocation.getArguments().length == 1 &&
                    directory.equals(invocation.getArgument(0)))
            {
                throw new AccessDeniedException(directory.toString());
            }
            if ("readAttributes".equals(methodName) && inaccessibleArchive != null &&
                    inaccessibleArchive.equals(invocation.getArgument(0)))
            {
                throw new AccessDeniedException(inaccessibleArchive.toString());
            }
            return invocation.callRealMethod();
        });
    }

    private void writeActive(final String content, final Instant modified) throws IOException
    {
        Files.writeString(_activeFile, content);
        Files.setLastModifiedTime(_activeFile, FileTime.from(modified));
    }

    private Path archive(final LocalDate date, final int index, final boolean compressed)
    {
        return _activeFile.resolveSibling(_activeFile.getFileName() + "." + date + "." + index +
                (compressed ? ".gz" : ""));
    }

    private void writeArchive(final LocalDate date, final int index, final boolean compressed, final String content)
            throws IOException
    {
        writeArchive(archive(date, index, compressed), compressed, content);
    }

    private Path writeOversizedArchive(final boolean compressed) throws IOException
    {
        final Path archive = Path.of(_activeFile + "." + TODAY + ".999999999999999999999" +
                (compressed ? ".gz" : ""));
        writeArchive(archive, compressed, "oversized archive");
        return archive;
    }

    private void writeArchive(final Path archive, final boolean compressed, final String content) throws IOException
    {
        if (compressed)
        {
            try (final GZIPOutputStream output = new GZIPOutputStream(Files.newOutputStream(archive)))
            {
                output.write(content.getBytes(StandardCharsets.UTF_8));
            }
        }
        else
        {
            Files.writeString(archive, content);
        }
    }

    private String readArchive(final LocalDate date, final int index, final boolean compressed) throws IOException
    {
        final Path archive = archive(date, index, compressed);
        if (compressed)
        {
            try (final GZIPInputStream input = new GZIPInputStream(Files.newInputStream(archive)))
            {
                return new String(input.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        return Files.readString(archive);
    }

    private List<Path> files() throws IOException
    {
        try (final Stream<Path> files = Files.list(_directory))
        {
            return files.toList();
        }
    }

    private final class TestAppender implements AutoCloseable
    {
        private final LoggerContext _context = new LoggerContext();
        private final RollingFileAppender<ILoggingEvent> _appender = new RollingFileAppender<>();
        private final DailyTriggeringPolicy<ILoggingEvent> _triggeringPolicy;
        private final Logger _logger;

        private TestAppender(final boolean restart, final boolean compressed)
        {
            this(restart, compressed, 0);
        }

        private TestAppender(final boolean restart, final boolean compressed, final int history)
        {
            _context.setMDCAdapter(new LogbackMDCAdapter());
            _context.start();
            _appender.setContext(_context);
            _appender.setName(getTestName());
            _appender.setFile(_activeFile.toString());
            _triggeringPolicy = new DailyTriggeringPolicy<>(new FileSize(MAX_FILE_SIZE), restart);
            _triggeringPolicy.setCurrentTime(NOW.toEpochMilli());
            final TimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new TimeBasedRollingPolicy<>();
            rollingPolicy.setContext(_context);
            rollingPolicy.setParent(_appender);
            rollingPolicy.setFileNamePattern(_activeFile + ".%d{yyyy-MM-dd,UTC}.%i" + (compressed ? ".gz" : ""));
            rollingPolicy.setMaxHistory(history);
            rollingPolicy.setTimeBasedFileNamingAndTriggeringPolicy(_triggeringPolicy);
            _appender.setRollingPolicy(rollingPolicy);
            _appender.setTriggeringPolicy(_triggeringPolicy);
            rollingPolicy.start();
            final PatternLayoutEncoder encoder = new PatternLayoutEncoder();
            encoder.setContext(_context);
            encoder.setPattern("%msg");
            encoder.setCharset(StandardCharsets.UTF_8);
            encoder.start();
            _appender.setEncoder(encoder);
            _appender.start();
            assertTrue(_appender.isStarted(), _context.getStatusManager().getCopyOfStatusList().toString());
            _logger = _context.getLogger(getTestName());
            _logger.setAdditive(false);
            _logger.addAppender(_appender);
        }

        private void append(final String message) throws IOException
        {
            _logger.info(message);
            Files.setLastModifiedTime(_activeFile, FileTime.fromMillis(_triggeringPolicy.getCurrentTime()));
        }

        @Override
        public void close() throws InterruptedException
        {
            try
            {
                _appender.stop();
                // Logback waits only for the most recent compression job when stopping the appender.
                final ExecutorService executor = _context.getExecutorService();
                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                        "Timed out waiting for log archive compression");
                final List<Status> errors = _context.getStatusManager().getCopyOfStatusList().stream()
                        .filter(status -> status.getEffectiveLevel() >= Status.ERROR).toList();
                assertTrue(errors.isEmpty(), errors.toString());
            }
            finally
            {
                _context.stop();
            }
        }
    }
}
