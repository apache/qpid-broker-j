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

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.qpid.server.logging.LogFileDetails;
import org.apache.qpid.server.logging.PathContent;
import org.apache.qpid.server.logging.ZippedContent;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.util.FileUtils;

public class RolloverWatcherTest extends QpidTestCase
{
    private RolloverWatcher _rolloverWatcher;
    private File _baseFolder;
    private File _activeFile;

    public void setUp() throws Exception
    {
        super.setUp();
        _baseFolder = TestFileUtils.createTestDirectory("rollover", true);
        _activeFile = new File(_baseFolder, "test.log");
        _rolloverWatcher = new RolloverWatcher(_activeFile.toString());
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

        TestFileUtils.saveTextContentInFile("test", _activeFile);
        PathContent content = _rolloverWatcher.getFileContent(_activeFile.getName());

        assertEquals("Unexpected content type", "text/plain", content.getContentType());
        assertEquals("Unexpected data", "test", readContent(content));
        assertEquals("Unexpected size", 4, content.getContentLength());
        assertEquals("Unexpected content disposition", "attachment; filename=\"" + _activeFile.getName().toString() + "\"", content.getContentDisposition());
    }

    public void testGetTypedForNullFile() throws Exception
    {
        try
        {
            _rolloverWatcher.getFileContent(null);
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

        PathContent content = _rolloverWatcher.getFileContent("test1.gz");

        assertEquals("Unexpected content type", "application/x-gzip", content.getContentType());
        assertEquals("Unexpected data", "test.gz", readContent(content));
        assertEquals("Unexpected size", 7, content.getContentLength());
        assertEquals("Unexpected content disposition", "attachment; filename=\"test1.gz\"", content.getContentDisposition());
    }

    public void testGetTypedContentForNonExistingRolledFile() throws Exception
    {
        String[] files = {"test1.gz", "test2.gz"};
        Path baseFolder = new File(getTestName()).toPath();
        _rolloverWatcher.onRollover(baseFolder, files);

        PathContent content = _rolloverWatcher.getFileContent("test3.zip");

        assertEquals("Unexpected content type", "application/x-zip", content.getContentType());
        assertEquals("Unexpected content disposition", "attachment", content.getContentDisposition());
        assertEquals("Unexpected size", 0, content.getContentLength());
        try
        {
            readContent(content);
            fail("FileNotFoundException is expected");
        }
        catch(FileNotFoundException e)
        {
            //pass
        }
    }

    public void testGetContentType() throws Exception
    {
        assertEquals("Unexpected content type for log file", "text/plain", _rolloverWatcher.getContentType("test.log"));
        assertEquals("Unexpected content type for gzip file", "application/x-gzip", _rolloverWatcher.getContentType("test.gz"));
        assertEquals("Unexpected content type for zip file", "application/x-zip", _rolloverWatcher.getContentType("test.zip"));
    }

    public void testGetLogFileDetails() throws Exception
    {
        String[] files = createTestRolledFilesAndNotifyWatcher();

        List<LogFileDetails> logFileDetails = _rolloverWatcher.getLogFileDetails();
        final int expectedNumberOfEntries = files.length + 1; // add one for the active file
        assertEquals("getLogFileDetails returned unexpected number of entries", expectedNumberOfEntries, logFileDetails.size());

        List<String> expectedFiles = new ArrayList<>(Arrays.asList(files));
        expectedFiles.add(_activeFile.getName());

        for (String expectedFileName : expectedFiles)
        {
            boolean found = false;
            for (LogFileDetails details : logFileDetails)
            {
                if (details.getName().equals(expectedFileName))
                {
                    found = true;
                    final File file = new File(_baseFolder, expectedFileName);
                    assertEquals("FileDetail for \"" + expectedFileName + "\" has unexpected lastModified time", file.lastModified(), details.getLastModified());
                    assertEquals("FileDetail for \"" + expectedFileName + "\" has unexpected size", file.length(), details.getSize());
                    break;
                }
            }
            assertTrue("File \"" + expectedFileName + "\" expected but not found in logFileDetails", found);
        }
    }

    public void testGetFilesAsZippedContentWithNonLogFile() throws Exception
    {
        String[] fileNames = createTestRolledFilesAndNotifyWatcher();
        String nonLogFileName = "nonLogFile.txt";
        TestFileUtils.saveTextContentInFile(nonLogFileName, new File(_baseFolder, nonLogFileName));

        String[] requestedFiles = new String[]{ fileNames[0], fileNames[2], nonLogFileName };
        String[] expectedFiles = new String[]{ fileNames[0], fileNames[2] };

        ZippedContent content = _rolloverWatcher.getFilesAsZippedContent(new HashSet<>(Arrays.asList(requestedFiles)));
        assertZippedContent(expectedFiles, content);
    }

    public void testGetFilesAsZippedContent() throws Exception
    {
        String[] fileNames = createTestRolledFilesAndNotifyWatcher();
        String[] requestedFiles = new String[]{ fileNames[0], fileNames[2] };

        ZippedContent content = _rolloverWatcher.getFilesAsZippedContent(new HashSet<>(Arrays.asList(requestedFiles)));
        assertZippedContent(requestedFiles, content);
    }

    public void testGetFilesAsZippedContentWithNonExistingFile() throws Exception
    {
        String[] fileNames = createTestRolledFilesAndNotifyWatcher();
        new File(_baseFolder, fileNames[2]).delete();
        String[] requestedFiles = new String[]{ fileNames[0], fileNames[2] };
        String[] expectedFiles = new String[]{ fileNames[0] };

        ZippedContent content = _rolloverWatcher.getFilesAsZippedContent(new HashSet<>(Arrays.asList(requestedFiles)));
        assertZippedContent(expectedFiles, content);
    }

    public void testGetAllFilesAsZippedContent() throws Exception
    {
        String[] fileNames = createTestRolledFilesAndNotifyWatcher();

        ZippedContent content = _rolloverWatcher.getAllFilesAsZippedContent();
        assertZippedContent(fileNames, content);
    }

    private String[] createTestRolledFilesAndNotifyWatcher()
    {
        String[] fileNames = {"test1.gz", "test2.gz", "test3.gz"};
        for (String fileName : fileNames)
        {
            TestFileUtils.saveTextContentInFile(fileName, new File(_baseFolder, fileName));
        }
        _rolloverWatcher.onRollover(_baseFolder.toPath(), fileNames);
        return fileNames;
    }

    private void assertZippedContent(String[] expectedZipEntries, ZippedContent zippedContent) throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        zippedContent.write(outputStream);

        byte[] zipFileContent = outputStream.toByteArray();
        ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(zipFileContent));

        byte[] buffer = new byte[10];
        Map<String, String> unzippedContent = new HashMap<>();
        ZipEntry zipEntry;
        while ((zipEntry = zipInputStream.getNextEntry()) != null)
        {
            int len = 0;
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            while ((len = zipInputStream.read(buffer)) > 0)
            {
                output.write(buffer, 0, len);
            }
            unzippedContent.put(zipEntry.getName(), new String(output.toByteArray()));
        }

        assertEquals("ZippedContent has unexpected number of entries.", expectedZipEntries.length, unzippedContent.size());
        for (String fileName : expectedZipEntries)
        {
            assertTrue("ZippedContent does not contain expected file \"" + fileName + "\".", unzippedContent.containsKey(fileName));
            assertEquals("ZippedContent entry \"" + fileName + "\" has unexpected content.", fileName, unzippedContent.get(fileName));
        }
    }

    private String readContent(Content content) throws IOException
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        content.write(os);
        return new String(os.toByteArray());
    }
}
