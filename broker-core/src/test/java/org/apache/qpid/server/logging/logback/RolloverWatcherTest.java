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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.qpid.server.model.TypedContent;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.util.FileUtils;

public class RolloverWatcherTest extends QpidTestCase
{
    private RolloverWatcher _rolloverWatcher;
    private File _baseFolder;

    public void setUp() throws Exception
    {
        super.setUp();
        _rolloverWatcher = new RolloverWatcher();
        _baseFolder = TestFileUtils.createTestDirectory("rollover", true);
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        if (_baseFolder.exists())
        {
            FileUtils.delete(_baseFolder, true);
        }
    }

    public void testOnRollover() throws Exception
    {
        String[] files = {"test1", "test2"};
        _rolloverWatcher.onRollover(_baseFolder.toPath(), files);

        assertEquals("Unexpected rolled files. Expected " + Arrays.toString(files) + " but got "
                        + _rolloverWatcher.getRolledFiles(), new HashSet<>(Arrays.asList(files)),
                new HashSet<>(_rolloverWatcher.getRolledFiles()));
    }

    public void testGetTypedContentForActiveFile() throws Exception
    {
        _rolloverWatcher.onRollover(_baseFolder.toPath(), new String[]{});

        final File activeFile = new File(_baseFolder, "test.log");
        TestFileUtils.saveTextContentInFile("test", activeFile);
        TypedContent content = _rolloverWatcher.getTypedContent(activeFile.getAbsolutePath(), true);

        assertEquals("Unexpected content type", "text/plain", content.getContentType());
        assertEquals("Unexpected data", "test", readStream(content.openInputStream()));
        assertEquals("Unexpected size", 4, content.getSize());
    }

    public void testGetTypedForNullFile() throws Exception
    {
        try
        {
            _rolloverWatcher.getTypedContent(null, true);
            fail("IllegalArgumentException is expected for null file name");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testGetTypedContentForExistingRolledFile() throws Exception
    {
        String[] files = {"test1.gz", "test2.gz"};
        _rolloverWatcher.onRollover(_baseFolder.toPath(), files);

        TestFileUtils.saveTextContentInFile("test.gz", new File(_baseFolder, "test1.gz"));

        TypedContent content = _rolloverWatcher.getTypedContent("test1.gz", false);

        assertEquals("Unexpected content type", "application/x-gzip", content.getContentType());
        assertEquals("Unexpected data", "test.gz", readStream(content.openInputStream()));
        assertEquals("Unexpected size", 7, content.getSize());
    }

    public void testGetTypedContentForNonExistingRolledFile() throws Exception
    {
        String[] files = {"test1.gz", "test2.gz"};
        Path baseFolder = new File(getTestName()).toPath();
        _rolloverWatcher.onRollover(baseFolder, files);

        TypedContent content = _rolloverWatcher.getTypedContent("test3.zip", false);

        assertEquals("Unexpected content type", "application/x-zip", content.getContentType());
        assertNull("Unexpected content stream", content.openInputStream());
        assertEquals("Unexpected size", 0, content.getSize());
    }

    public void testGetContentType() throws Exception
    {
        assertEquals("Unexpected content type for log file", "text/plain", _rolloverWatcher.getContentType("test.log"));
        assertEquals("Unexpected content type for gzip file", "application/x-gzip", _rolloverWatcher.getContentType("test.gz"));
        assertEquals("Unexpected content type for zip file", "application/x-zip", _rolloverWatcher.getContentType("test.zip"));
    }

    private String readStream(InputStream contentStream) throws IOException
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = contentStream.read(buffer)) > 0)
        {
            os.write(buffer, 0, length);
        }
        return new String(os.toByteArray());
    }
}
