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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.LogFileDetails;
import org.apache.qpid.server.logging.PathContent;
import org.apache.qpid.server.logging.ZippedContent;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class RolloverWatcherTest extends UnitTestBase
{
    private RolloverWatcher _rolloverWatcher;
    private File _baseFolder;
    private File _activeFile;

    @BeforeEach
    public void setUp() throws Exception
    {
        _baseFolder = TestFileUtils.createTestDirectory("rollover", true);
        _activeFile = new File(_baseFolder, "test.log");
        _rolloverWatcher = new RolloverWatcher(_activeFile.toString());
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_baseFolder.exists())
        {
            FileUtils.delete(_baseFolder, true);
        }
    }

    @Test
    public void testOnRollover()
    {
        final String[] files = {"test1", "test2"};
        _rolloverWatcher.onRollover(_baseFolder.toPath(), files);

        assertEquals(Set.of(files), Set.copyOf(_rolloverWatcher.getRolledFiles()),
                "Unexpected rolled files. Expected " + Arrays.toString(files) + " but got " +
                _rolloverWatcher.getRolledFiles());
    }

    @Test
    public void testGetLogFileDetailsIsOrdered() throws Exception
    {
        final String[] files = {"test1", "test2"};
        final List<String> expectedOrder = new ArrayList<>();

        _activeFile.createNewFile();
        expectedOrder.add(_activeFile.toPath().getFileName().toString());

        long lastModified = System.currentTimeMillis() - (1000 * 60 * 5);
        for (final String filename : files)
        {
            final File file = new File(_baseFolder, filename);
            file.createNewFile();
            file.setLastModified(lastModified);
            lastModified += 1000 * 30;
            expectedOrder.add(1, filename);
        }

        _rolloverWatcher.onRollover(_baseFolder.toPath(), files);

        final List<LogFileDetails> orderedDetails = _rolloverWatcher.getLogFileDetails();

        assertEquals(expectedOrder.size(), (long) orderedDetails.size(), "Unexpected number of log file details");

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            final String expectedFilename = expectedOrder.get(i);
            final String actualFilename = orderedDetails.get(i).getName();
            assertEquals(expectedFilename, actualFilename, "Unexpected filename at position " + i);
        }
    }

    @Test
    public void testGetTypedContentForActiveFile() throws Exception
    {
        _rolloverWatcher.onRollover(_baseFolder.toPath(), new String[]{});

        TestFileUtils.saveTextContentInFile("test", _activeFile);
        final PathContent content = _rolloverWatcher.getFileContent(_activeFile.getName());

        assertEquals("text/plain", content.getContentType(), "Unexpected content type");
        assertEquals("test", readContent(content), "Unexpected data");
        assertEquals("attachment; filename=\"" + _activeFile.getName() + "\"", content.getContentDisposition(),
                "Unexpected content disposition");
    }

    @Test
    public void testGetTypedForNullFile()
    {
        assertThrows(IllegalArgumentException.class,
                () -> _rolloverWatcher.getFileContent(null),
                "IllegalArgumentException is expected for null file name");
    }

    @Test
    public void testGetTypedContentForExistingRolledFile() throws Exception
    {
        final String[] files = {"test1.gz", "test2.gz"};
        _rolloverWatcher.onRollover(_baseFolder.toPath(), files);

        TestFileUtils.saveTextContentInFile("test.gz", new File(_baseFolder, "test1.gz"));

        final PathContent content = _rolloverWatcher.getFileContent("test1.gz");

        assertEquals("application/x-gzip", content.getContentType(), "Unexpected content type");
        assertEquals("test.gz", readContent(content), "Unexpected data");
        assertEquals("attachment; filename=\"test1.gz\"", content.getContentDisposition(),
                "Unexpected content disposition");
    }

    @Test
    public void testGetTypedContentForNonExistingRolledFile()
    {
        final String[] files = {"test1.gz", "test2.gz"};
        final Path baseFolder = new File(getTestName()).toPath();
        _rolloverWatcher.onRollover(baseFolder, files);

        final PathContent content = _rolloverWatcher.getFileContent("test3.zip");

        assertEquals("application/x-zip", content.getContentType(), "Unexpected content type");
        assertEquals("attachment", content.getContentDisposition(), "Unexpected content disposition");
        assertThrows(FileNotFoundException.class,
                () -> readContent(content),
                "FileNotFoundException is expected");
    }

    @Test
    public void testGetContentType()
    {
        assertEquals("text/plain", _rolloverWatcher.getContentType("test.log"),
                "Unexpected content type for log file");
        assertEquals("application/x-gzip", _rolloverWatcher.getContentType("test.gz"),
                "Unexpected content type for gzip file");
        assertEquals("application/x-zip", _rolloverWatcher.getContentType("test.zip"),
                "Unexpected content type for zip file");
    }

    @Test
    public void testGetLogFileDetails()
    {
        final String[] files = createTestRolledFilesAndNotifyWatcher();

        final List<LogFileDetails> logFileDetails = _rolloverWatcher.getLogFileDetails();
        final int expectedNumberOfEntries = files.length + 1; // add one for the active file
        assertEquals(expectedNumberOfEntries, (long) logFileDetails.size(),
                "getLogFileDetails returned unexpected number of entries");

        final List<String> expectedFiles = new ArrayList<>(Arrays.asList(files));
        expectedFiles.add(_activeFile.getName());

        for (final String expectedFileName : expectedFiles)
        {
            boolean found = false;
            for (final LogFileDetails details : logFileDetails)
            {
                if (details.getName().equals(expectedFileName))
                {
                    found = true;
                    final File file = new File(_baseFolder, expectedFileName);
                    assertEquals(file.lastModified(), details.getLastModified(),
                            "FileDetail for \"" + expectedFileName + "\" has unexpected lastModified time");
                    assertEquals(file.length(), details.getSize(),
                            "FileDetail for \"" + expectedFileName + "\" has unexpected size");
                    break;
                }
            }
            assertTrue(found, "File \"" + expectedFileName + "\" expected but not found in logFileDetails");
        }
    }

    @Test
    public void testGetFilesAsZippedContentWithNonLogFile() throws Exception
    {
        final String[] fileNames = createTestRolledFilesAndNotifyWatcher();
        final String nonLogFileName = "nonLogFile.txt";
        TestFileUtils.saveTextContentInFile(nonLogFileName, new File(_baseFolder, nonLogFileName));

        final String[] requestedFiles = new String[]{ fileNames[0], fileNames[2], nonLogFileName };
        final String[] expectedFiles = new String[]{ fileNames[0], fileNames[2] };

        final ZippedContent content = _rolloverWatcher.getFilesAsZippedContent(Set.of(requestedFiles));
        assertZippedContent(expectedFiles, content);
    }

    @Test
    public void testGetFilesAsZippedContent() throws Exception
    {
        final String[] fileNames = createTestRolledFilesAndNotifyWatcher();
        final String[] requestedFiles = new String[]{ fileNames[0], fileNames[2] };

        final ZippedContent content = _rolloverWatcher.getFilesAsZippedContent(Set.of(requestedFiles));
        assertZippedContent(requestedFiles, content);
    }

    @Test
    public void testGetFilesAsZippedContentWithNonExistingFile() throws Exception
    {
        final String[] fileNames = createTestRolledFilesAndNotifyWatcher();
        new File(_baseFolder, fileNames[2]).delete();
        final String[] requestedFiles = new String[]{ fileNames[0], fileNames[2] };
        final String[] expectedFiles = new String[]{ fileNames[0] };

        final ZippedContent content = _rolloverWatcher.getFilesAsZippedContent(Set.of(requestedFiles));
        assertZippedContent(expectedFiles, content);
    }

    @Test
    public void testGetAllFilesAsZippedContent() throws Exception
    {
        final String[] fileNames = createTestRolledFilesAndNotifyWatcher();

        final ZippedContent content = _rolloverWatcher.getAllFilesAsZippedContent();
        assertZippedContent(fileNames, content);
    }

    private String[] createTestRolledFilesAndNotifyWatcher()
    {
        final String[] fileNames = {"test1.gz", "test2.gz", "test3.gz"};
        for (final String fileName : fileNames)
        {
            TestFileUtils.saveTextContentInFile(fileName, new File(_baseFolder, fileName));
        }
        _rolloverWatcher.onRollover(_baseFolder.toPath(), fileNames);
        return fileNames;
    }

    private void assertZippedContent(final String[] expectedZipEntries, final ZippedContent zippedContent) throws IOException
    {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        zippedContent.write(outputStream);

        final byte[] zipFileContent = outputStream.toByteArray();
        final ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(zipFileContent));

        final byte[] buffer = new byte[10];
        final Map<String, String> unzippedContent = new HashMap<>();
        ZipEntry zipEntry;
        while ((zipEntry = zipInputStream.getNextEntry()) != null)
        {
            int len;
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            while ((len = zipInputStream.read(buffer)) > 0)
            {
                output.write(buffer, 0, len);
            }
            unzippedContent.put(zipEntry.getName(), output.toString());
        }

        assertEquals(expectedZipEntries.length, (long) unzippedContent.size(),
                "ZippedContent has unexpected number of entries.");
        for (final String fileName : expectedZipEntries)
        {
            assertTrue(unzippedContent.containsKey(fileName),
                    "ZippedContent does not contain expected file \"" + fileName + "\".");
            assertEquals(fileName, unzippedContent.get(fileName),
                    "ZippedContent entry \"" + fileName + "\" has unexpected content.");
        }
    }

    private String readContent(final Content content) throws IOException
    {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        content.write(os);
        return os.toString();
    }
}
