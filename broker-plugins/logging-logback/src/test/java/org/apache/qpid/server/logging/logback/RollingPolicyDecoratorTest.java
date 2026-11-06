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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import ch.qos.logback.core.Context;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.RollingPolicyBase;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
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
        _baseFolder = TestFileUtils.createTestDirectory("rollover", true);
        _testFile = createTestFile("test.2015-06-25.0.gz");
        final Context mockContext = mock(Context.class);
        _delegate = mock(RollingPolicyBase.class);
        final String fileNamePattern = _baseFolder.getAbsolutePath() + "/" + "test.%d{yyyy-MM-dd}.%i.gz";
        when(_delegate.getFileNamePattern()).thenReturn(fileNamePattern);
        when(_delegate.getContext()).thenReturn(mockContext);
        _listener = mock(RollingPolicyDecorator.RolloverListener.class);

        _policy = new RollingPolicyDecorator(_delegate, _listener, createMockExecutorService());

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFirstRolloverRescansUntilArchiveAppears(final boolean existingArchive) throws IOException
    {
        if (!existingArchive)
        {
            Files.delete(_testFile.toPath());
        }
        final Queue<Runnable> scans = new ArrayDeque<>();
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        doAnswer(invocation ->
        {
            scans.add(invocation.getArgument(0));
            return null;
        }).when(executor).execute(any(Runnable.class));
        doAnswer(invocation ->
        {
            scans.add(invocation.getArgument(0));
            return null;
        }).when(executor).schedule(any(Runnable.class), any(long.class), any(TimeUnit.class));
        _policy = new RollingPolicyDecorator(_delegate, _listener, executor);
        _policy.start();
        _policy.rollover();

        // The archive is still being compressed when the first rollover scan runs.
        final int initialScanCount = scans.size();
        for (int i = 0; i < initialScanCount; i++)
        {
            scans.remove().run();
        }
        assertEquals(1, scans.size(), "The first rollover must keep scanning until its archive appears");

        final File archive = createTestFile("test.2015-06-25.1.gz");
        scans.remove().run();
        final String[] expected = existingArchive ? new String[]{_testFile.getName(), archive.getName()} :
                new String[]{archive.getName()};
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(getMatcher(expected)));
        assertTrue(scans.isEmpty(), "A detected rollover must stop rescanning");
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

    @Test
    public void testStartupIgnoresDirectoriesNamedAsArchives() throws IOException
    {
        Files.createDirectory(_baseFolder.toPath().resolve("test.2015-06-25.1.gz"));

        _policy.start();

        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(getMatcher(new String[]{_testFile.getName()})));
    }

    @Test
    public void testScanDepthIncludesDirectoriesInArchivePattern() throws IOException
    {
        final String fileNamePattern = _baseFolder.getAbsolutePath() + "/test.%d{yyyy/MM/dd}.%i.gz";
        when(_delegate.getFileNamePattern()).thenReturn(fileNamePattern);
        final Path archiveDirectory = Files.createDirectories(_baseFolder.toPath().resolve("test.2015").resolve("06"));
        final Path archive = Files.createFile(archiveDirectory.resolve("25.0.gz"));
        _policy = new RollingPolicyDecorator(_delegate, _listener, createMockExecutorService());

        _policy.start();

        final String relativeName = _baseFolder.toPath().relativize(archive).toString();
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(getMatcher(new String[]{relativeName})));
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
