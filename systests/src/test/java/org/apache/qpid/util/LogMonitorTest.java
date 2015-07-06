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
package org.apache.qpid.util;

import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class LogMonitorTest extends QpidTestCase
{

    private LogMonitor _monitor;
    private File _testFile;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _testFile = TestFileUtils.createTempFile(this);
        _monitor = new LogMonitor(_testFile);
        _monitor.getMonitoredFile().deleteOnExit(); // Make sure we clean up
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        _testFile.delete();
    }

    public void testMonitorNormalFile() throws IOException
    {
        assertEquals(_testFile, _monitor.getMonitoredFile());
    }

    public void testMonitorNullFileThrows()
    {
        try
        {
            new LogMonitor(null);
            fail("It should not be possible to monitor null.");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testMonitorNonExistentFileThrows() throws IOException
    {
        assertTrue("Unable to delete file for our test", _testFile.delete());
        assertFalse("Unable to test as our test file exists.", _testFile.exists());

        try
        {
            new LogMonitor(_testFile);
            fail("It should not be possible to monitor non existing file.");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testFindMatches_Match() throws IOException
    {
        String message = getName() + ": Test Message";
        appendTestMessage(message);
        validateLogContainsMessage(_monitor, message);
    }

    public void testFindMatches_NoMatch() throws IOException
    {
        String message = getName() + ": Test Message";
        String notLogged = "This text was not logged";
        appendTestMessage(message);
        validateLogDoesNotContainMessage(_monitor, notLogged);
    }

    public void testWaitForMessage_Timeout() throws IOException
    {
        String message = getName() + ": Test Message";

        long TIME_OUT = 2000;

        logMessageWithDelay(message, TIME_OUT);

        // Verify that we can time out waiting for a message
        assertFalse("Message was logged ",
                    _monitor.waitForMessage(message, 100));

        // Verify that the message did eventually get logged.
        assertTrue("Message was never logged.",
                    _monitor.waitForMessage(message, TIME_OUT * 2));
    }

    public void testDiscardPoint() throws IOException
    {
        String firstMessage = getName() + ": Test Message1";
        appendTestMessage(firstMessage);

        validateLogContainsMessage(_monitor, firstMessage);

        _monitor.markDiscardPoint();

        validateLogDoesNotContainMessage(_monitor, firstMessage);

        String secondMessage = getName() + ": Test Message2";
        appendTestMessage(secondMessage);
        validateLogContainsMessage(_monitor, secondMessage);
    }

    public void testRead() throws IOException
    {
        String message = getName() + ": Test Message";
        appendTestMessage(message);
        String fileContents = _monitor.readFile();

        assertTrue("Logged message not found when reading file.",
                   fileContents.contains(message));
    }

    /****************** Helpers ******************/

    /**
     * Validate that the LogMonitor does not match the given string in the log
     *
     * @param log     The LogMonitor to check
     * @param message The message to check for
     *
     * @throws IOException if a problems occurs
     */
    protected void validateLogDoesNotContainMessage(LogMonitor log, String message)
            throws IOException
    {
        List<String> results = log.findMatches(message);

        assertNotNull("Null results returned.", results);

        assertEquals("Incorrect result set size", 0, results.size());
    }

    /**                                                                                                                                                 
     * Validate that the LogMonitor can match the given string in the log
     *
     * @param log     The LogMonitor to check
     * @param message The message to check for
     *
     * @throws IOException if a problems occurs
     */
    protected void validateLogContainsMessage(LogMonitor log, String message)
            throws IOException
    {
        List<String> results = log.findMatches(message);

        assertNotNull("Null results returned.", results);

        assertEquals("Incorrect result set size", 1, results.size());

        assertTrue("Logged Message'" + message + "' not present in results:"
                + results.get(0), results.get(0).contains(message));
    }

    /**
     * Create a new thread to log the given message after the set delay
     *
     * @param message the messasge to log
     * @param delay   the delay (ms) to wait before logging
     */
    private void logMessageWithDelay(final String message, final long delay)
    {
        new Thread(new Runnable()
        {

            public void run()
            {
                try
                {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e)
                {
                    //ignore
                }

                appendTestMessage(message);
            }
        }).start();
    }


    private void appendTestMessage(String msg)
    {
        try (OutputStream outputStream = new FileOutputStream(_testFile, true))
        {
            outputStream.write(String.format("%s%n", msg).getBytes());
        }
        catch (IOException e)
        {
            fail("Cannot write to test file '" + _testFile.getAbsolutePath() + "'");
        }
    }
}
