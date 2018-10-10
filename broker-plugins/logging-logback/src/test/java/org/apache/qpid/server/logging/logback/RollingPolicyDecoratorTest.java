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

import static org.junit.Assert.assertTrue;
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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import ch.qos.logback.core.Context;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.RollingPolicyBase;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
    private Pattern _rolledFileRegExp;

    @Before
    public void setUp() throws Exception
    {

        _baseFolder = TestFileUtils.createTestDirectory("rollover", true);
        _testFile = createTestFile("test.2015-06-25.0.gz");
        Context mockContext = mock(Context.class);
        _delegate = mock(RollingPolicyBase.class);
        String fileNamePattern = _baseFolder.getAbsolutePath() + "/" + "test.%d{yyyy-MM-dd}.%i.gz";
        when(_delegate.getFileNamePattern()).thenReturn(fileNamePattern);
        when(_delegate.getContext()).thenReturn(mockContext);
        _listener = mock(RollingPolicyDecorator.RolloverListener.class);

        _policy = new RollingPolicyDecorator(_delegate, _listener, createMockExecutorService());

        _rolledFileRegExp = Pattern.compile(new FileNamePattern(fileNamePattern, mockContext).toRegex());

        LOGGER.debug("Rolled file reg exp: {} ", _rolledFileRegExp);
    }

    @After
    public void tearDown() throws Exception
    {
        if (_baseFolder.exists())
        {
            FileUtils.delete(_baseFolder, true);
        }
    }

    public File createTestFile(String fileName) throws IOException
    {
        File testFile = new File(_baseFolder, fileName);
        assertTrue("Cannot create a new file " + testFile.getPath(), testFile.createNewFile());
        return testFile;
    }

    private ScheduledExecutorService createMockExecutorService()
    {
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                ((Runnable)args[0]).run();
                return null;
            }}).when(executorService).schedule(any(Runnable.class), any(long.class), any(TimeUnit.class));
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                ((Runnable)args[0]).run();
                return null;
            }}).when(executorService).execute(any(Runnable.class));
        return executorService;
    }

    @Test
    public void testRollover()
    {
        _policy.rollover();
        verify(_delegate).rollover();
    }

    @Test
    public void testRolloverListener() throws InterruptedException
    {
        _policy.rollover();
        verify(_listener).onRollover(any(Path.class), any(String[].class));
    }

    @Test
    public void testRolloverWithFile() throws IOException
    {
        _policy.rollover();
        verify(_delegate).rollover();

        ArgumentMatcher<String[]> matcher = getMatcher(new String[]{_testFile.getName()});
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(matcher));
    }

    @Test
    public void testRolloverRescanLimit() throws IOException
    {
        _policy.rollover();
        verify(_delegate).rollover();
        ArgumentMatcher<String[]> matcher = getMatcher(new String[]{_testFile.getName()});
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

        ArgumentMatcher<String[]> matcher = getMatcher(new String[]{ _testFile.getName() });
        verify(_listener).onRollover(eq(_baseFolder.toPath()), argThat(matcher));

        File secondFile = createTestFile("test.2015-06-25.1.gz");
        _policy.rollover();
        verify(_delegate, times(2)).rollover();
        ArgumentMatcher<String[]> matcher2 = getMatcher(new String[]{_testFile.getName(), secondFile.getName()});
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
        FileAppender appender = mock(FileAppender.class);
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
