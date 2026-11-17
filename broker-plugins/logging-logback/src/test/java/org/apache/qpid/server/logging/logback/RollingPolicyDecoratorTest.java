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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import ch.qos.logback.core.Context;
import ch.qos.logback.core.ContextBase;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.RollingPolicyBase;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import ch.qos.logback.core.status.Status;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.mockito.ArgumentMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class RollingPolicyDecoratorTest extends UnitTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RollingPolicyDecoratorTest.class);

    private RollingPolicyBase _delegate;
    private RollingPolicyDecorator _policy;
    private RollingPolicyDecorator.RolloverListener _listener;
    private File _baseFolder;
    private File _testFile;

    @BeforeEach
    public void setUp() throws Exception
    {
        _baseFolder = TestFileUtils.createTestDirectory("rollover.with.dots", true);
        _testFile = createTestFile("test.2015-06-25.0.gz");
        final Context mockContext = mock(Context.class);
        _delegate = mock(RollingPolicyBase.class);
        final String fileNamePattern = _baseFolder.getAbsolutePath() + "/" + "test.%d{yyyy-MM-dd}.%i.gz";
        when(_delegate.getContext()).thenReturn(mockContext);
        _listener = mock(RollingPolicyDecorator.RolloverListener.class);

        _policy = createPolicy(fileNamePattern);

        final Pattern rolledFileRegExp = Pattern.compile(new FileNamePattern(fileNamePattern, mockContext).toRegex());

        LOGGER.debug("Rolled file reg exp: {} ", rolledFileRegExp);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_baseFolder.exists())
        {
            FileUtils.delete(_baseFolder, true);
        }
    }

    public File createTestFile(final String fileName) throws IOException
    {
        final File testFile = new File(_baseFolder, fileName);
        assertTrue(testFile.createNewFile(), "Cannot create a new file " + testFile.getPath());
        return testFile;
    }

    private RollingPolicyDecorator createPolicy(final String fileNamePattern)
    {
        when(_delegate.getFileNamePattern()).thenReturn(fileNamePattern);
        return new RollingPolicyDecorator(_delegate, _listener, createMockExecutorService());
    }

    private ScheduledExecutorService createMockExecutorService()
    {
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        doAnswer(invocation ->
        {
            final Object[] args = invocation.getArguments();
            ((Runnable)args[0]).run();
            return null;
        }).when(executorService).schedule(any(Runnable.class), any(long.class), any(TimeUnit.class));
        doAnswer(invocation ->
        {
            final Object[] args = invocation.getArguments();
            ((Runnable)args[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        return executorService;
    }

    @Test
    public void testRollover()
    {
        _policy.rollover();
        verify(_delegate).rollover();
    }

    @Test
    public void testRolloverListener()
    {
        _policy.rollover();
        verify(_listener).onRollover(any(Path.class), any(String[].class));
    }

    @Test
    public void testRolloverWithFile()
    {
        _policy.rollover();
        verify(_delegate).rollover();

        final ArgumentMatcher<String[]> matcher = getMatcher(new String[]{_testFile.getName()});
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(matcher));
    }

    @ParameterizedTest
    @ValueSource(strings = {"java-mms.1-0", "logs[archive]", "logs(archive)", "logs(archive", "logs+archive",
            "logs$archive", "logs^archive", "logs{archive}"})
    public void testRolloverWithSpecialCharactersInDirectory(final String directoryName) throws IOException
    {
        final Path baseFolder = Files.createDirectory(_baseFolder.toPath().resolve(directoryName));
        Files.createFile(baseFolder.resolve(_testFile.getName()));
        final RollingPolicyDecorator policy = createPolicy(baseFolder.resolve("test.%d{yyyy-MM-dd}.%i.gz").toString());

        policy.rollover();

        verify(_listener).onRollover(eq(baseFolder), argThat(getMatcher(new String[]{_testFile.getName()})));
    }

    @ParameterizedTest
    @ValueSource(strings = {".%i", ".%i.gz", ".%d{yyyy-MM-dd}.%i", ".%d{yyyy-MM-dd}.%i.gz"})
    public void testBrokerPatternsDiscoverAndDownloadLogbackArchives(final String suffix) throws IOException
    {
        final Path baseFolder = Files.createDirectory(_baseFolder.toPath().resolve("logs[old](copy)+$"));
        final String pattern = baseFolder.resolve("qpid[old](copy)+$.log" + suffix).toString();
        final FileNamePattern fileNamePattern = new FileNamePattern(pattern, _delegate.getContext());
        final Instant instant = Instant.parse("2026-11-05T12:00:00Z");
        final Path archive = Path.of(fileNamePattern.convertMultipleArguments(instant, 12));
        Files.writeString(archive, "archive content");
        Files.createFile(archive.resolveSibling("other." + archive.getFileName()));
        final RolloverWatcher watcher = new RolloverWatcher(baseFolder.resolve("qpid[old](copy)+$.log").toString());
        when(_delegate.getFileNamePattern()).thenReturn(pattern);
        final ScheduledExecutorService executor = createMockExecutorService();
        final RollingPolicyDecorator policy = new RollingPolicyDecorator(_delegate, watcher, executor);

        policy.start();

        final String name = archive.getFileName().toString();
        assertEquals(List.of(name), List.copyOf(watcher.getRolledFiles()));
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        watcher.getFileContent(name).write(output);
        assertEquals("archive content", output.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testRolloverWithRelativeDirectory() throws IOException
    {
        final Path baseFolder = Files.createTempDirectory(Path.of("target"), "rollover.relative.");
        try
        {
            Files.createFile(baseFolder.resolve("archive.0.gz"));
            final RollingPolicyDecorator policy = createPolicy(baseFolder.resolve("archive.%i.gz").toString());

            policy.rollover();

            verify(_listener).onRollover(eq(baseFolder.toAbsolutePath()),
                    argThat(getMatcher(new String[]{"archive.0.gz"})));
        }
        finally
        {
            FileUtils.delete(baseFolder.toFile(), true);
        }
    }

    @Test
    public void testRolloverWithoutDirectory() throws IOException
    {
        final Path baseFolder = Files.createDirectory(_baseFolder.toPath().resolve("logs%literal"));
        Files.createFile(baseFolder.resolve(_testFile.getName()));
        setTestSystemProperty("user.dir", baseFolder.toString());
        final Context context = new ContextBase();
        when(_delegate.getContext()).thenReturn(context);

        createPolicy("test.%d{yyyy-MM-dd}.%i.gz").rollover();

        verify(_listener).onRollover(eq(baseFolder), argThat(getMatcher(new String[]{_testFile.getName()})));
        assertTrue(context.getStatusManager().getCopyOfStatusList().stream()
                .noneMatch(status -> status.getLevel() == Status.ERROR),
                "Logback should not report errors for a filename without a directory");
    }

    @Test
    public void testRolloverWithDateInDirectory() throws IOException
    {
        final String pattern = _baseFolder.toPath().resolve("%d{yyyy-MM-dd,aux}/test.%d{yyyy-MM-dd}.%i.gz").toString();
        final FileNamePattern fileNamePattern = new FileNamePattern(pattern, _delegate.getContext());
        final Instant instant = Instant.parse("2026-11-05T12:00:00Z");
        final Path archive = Path.of(fileNamePattern.convertMultipleArguments(instant, 12));
        Files.createDirectory(archive.getParent());
        Files.createFile(archive);

        createPolicy(pattern).rollover();

        final String name = _baseFolder.toPath().relativize(archive).toString();
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(getMatcher(new String[]{name})));
    }

    @Test
    public void testRolloverRescanLimit()
    {
        _policy.rollover();
        verify(_delegate).rollover();
        final ArgumentMatcher<String[]> matcher = getMatcher(new String[]{_testFile.getName()});
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(matcher));
        _policy.rollover();
        verify(_delegate, times(2)).rollover();
        verify(_listener).onNoRolloverDetected(eq(_baseFolder.toPath()), argThat(matcher));
    }

    @Test
    public void testSequentialRollover() throws IOException
    {
        _policy.rollover();
        verify(_delegate).rollover();

        final ArgumentMatcher<String[]> matcher = getMatcher(new String[]{ _testFile.getName() });
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(matcher));

        final File secondFile = createTestFile("test.2015-06-25.1.gz");
        _policy.rollover();
        verify(_delegate, times(2)).rollover();
        final ArgumentMatcher<String[]> matcher2 = getMatcher(new String[]{_testFile.getName(), secondFile.getName()});
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(matcher2));
    }

    private ArgumentMatcher<String[]> getMatcher(final String[] expected)
    {
        return item -> Arrays.equals(expected, item);
    }

    @Test
    public void testGetActiveFileName()
    {
        _policy.getActiveFileName();
        verify(_delegate).getActiveFileName();
    }

    @Test
    public void testGetCompressionMode()
    {
        _policy.getCompressionMode();
        verify(_delegate).getCompressionMode();
    }

    @Test
    public void testSetParent()
    {
        final FileAppender<?> appender = mock(FileAppender.class);
        _policy.setParent(appender);
        verify(_delegate).setParent(appender);
    }

    @Test
    public void testStart()
    {
        _policy.start();
        verify(_delegate).start();
    }

    @Test
    public void testStop()
    {
        _policy.stop();
        verify(_delegate).stop();
    }

    @Test
    public void testIsStarted()
    {
        _policy.isStarted();
        verify(_delegate).isStarted();
    }
}
