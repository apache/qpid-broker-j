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
package org.apache.qpid.server.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class FileUtilsTest extends UnitTestBase
{
    private static final String COPY = "-Copy";
    private static final String SUB = "-Sub";
    private static final String SEARCH_STRING = "testSearch";

    /**
     * Additional test for the copy method.
     * Ensures that the directory count did increase by more than 1 after the copy.
     */
    @Test
    public void testCopyFile()
    {
        final String TEST_DATA = "FileUtilsTest-testCopy-TestDataTestDataTestDataTestDataTestDataTestData";
        final String fileName = "FileUtilsTest-testCopy";
        final String fileNameCopy = fileName + COPY;

        File[] beforeCopyFileList = null;

        // Create initial file
        final File test = createTestFile(fileName, TEST_DATA);

        try
        {
            // Check number of files before copy
            beforeCopyFileList = test.getAbsoluteFile().getParentFile().listFiles();
            final int beforeCopy = beforeCopyFileList.length;

            // Perform Copy
            final File destination = new File(fileNameCopy);
            FileUtils.copy(test, destination);
            // Ensure the JVM cleans up if cleanup failues
            destination.deleteOnExit();

            // Retrieve counts after copy
            final int afterCopy = test.getAbsoluteFile().getParentFile().listFiles().length;

            final int afterCopyFromCopy = new File(fileNameCopy).getAbsoluteFile().getParentFile().listFiles().length;

            // Validate the copy counts
            assertEquals(afterCopy, (long) afterCopyFromCopy,
                    "The file listing from the original and the copy differ in length.");

            assertEquals(beforeCopy + 1, (long) afterCopy, "The number of files did not increase.");
            assertEquals(beforeCopy + 1, (long) afterCopyFromCopy,
                    "The number of files did not increase.");

            // Validate copy
            // Load content
            final String copiedFileContent = FileUtils.readFileAsString(fileNameCopy);
            assertEquals(TEST_DATA, copiedFileContent);
        }
        finally // Ensure clean
        {
            // Clean up
            assertTrue(FileUtils.deleteFile(fileNameCopy), "Unable to cleanup");

            // Check file list after cleanup
            final File[] afterCleanup = new File(test.getAbsoluteFile().getParent()).listFiles();
            checkFileLists(beforeCopyFileList, afterCleanup);

            // Remove original file
            assertTrue(test.delete(), "Unable to cleanup");
        }
    }

    /**
     * Create and Copy the following structure:
     *
     * testDirectory --+
     * +-- testSubDirectory --+
     * +-- testSubFile
     * +-- File
     *
     * to testDirectory-Copy
     *
     * Validate that the file count in the copy is correct and contents of the copied files is correct.
     */
    @Test
    public void testCopyRecursive()
    {
        final String TEST_DATA = "FileUtilsTest-testDirectoryCopy-TestDataTestDataTestDataTestDataTestDataTestData";
        final String fileName = "FileUtilsTest-testCopy";
        final String TEST_DIR = "testDirectoryCopy";

        // Create Initial Structure
        final File testDir = new File(TEST_DIR);

        // Check number of files before copy
        final File[] beforeCopyFileList = testDir.getAbsoluteFile().getParentFile().listFiles();

        try
        {
            // Create Directories
            final boolean condition = !testDir.exists();
            assertTrue(condition, "Test directory already exists cannot test.");

            if (!testDir.mkdir())
            {
                fail("Unable to make test Directory");
            }

            final File testSubDir = new File(TEST_DIR + File.separator + TEST_DIR + SUB);
            if (!testSubDir.mkdir())
            {
                fail("Unable to make test sub Directory");
            }

            // Create Files
            createTestFile(testDir + File.separator + fileName, TEST_DATA);
            createTestFile(testSubDir + File.separator + fileName + SUB, TEST_DATA);

            // Ensure the JVM cleans up if cleanup failues
            testSubDir.deleteOnExit();
            testDir.deleteOnExit();

            // Perform Copy
            final File copyDir = new File(testDir + COPY);
            try
            {
                FileUtils.copyRecursive(testDir, copyDir);
            }
            catch (FileNotFoundException | FileUtils.UnableToCopyException e)
            {
                fail(e.getMessage());
            }

            // Validate Copy
            assertEquals(2, (long) copyDir.listFiles().length,
                    "Copied directory should only have one file and one directory in it.");

            // Validate Copy File Contents
            String copiedFileContent = FileUtils.readFileAsString(copyDir + File.separator + fileName);
            assertEquals(TEST_DATA, copiedFileContent);

            // Validate Name of Sub Directory
            assertTrue(new File(copyDir + File.separator + TEST_DIR + SUB).isDirectory(),
                    "Expected subdirectory is not a directory");

            // Assert that it contains only one item
            assertEquals(1,  (long) new File(copyDir + File.separator + TEST_DIR  + SUB).listFiles().length,
                    "Copied sub directory should only have one directory in it.");

            // Validate content of Sub file
            copiedFileContent = FileUtils.readFileAsString(copyDir + File.separator + TEST_DIR + SUB +
                    File.separator + fileName + SUB);
            assertEquals(TEST_DATA, copiedFileContent);
        }
        finally
        {
            // Clean up source and copy directory.
            assertTrue(FileUtils.delete(testDir, true), "Unable to cleanup");
            assertTrue(FileUtils.delete(new File(TEST_DIR + COPY), true), "Unable to cleanup");

            // Check file list after cleanup
            final File[] afterCleanup = testDir.getAbsoluteFile().getParentFile().listFiles();
            checkFileLists(beforeCopyFileList, afterCleanup);
        }
    }

    /**
     * Helper method to create a temporary file with test content.
     *
     * @param testData The data to store in the file
     *
     * @return The File reference
     */
    private File createTestFileInTmpDir(final String testData) throws Exception 
    {
        final File tmpFile = File.createTempFile("test", "tmp");
        return createTestFile(tmpFile.getCanonicalPath(), testData);
    }
    /**
     * Helper method to create a test file with a string content
     *
     * @param fileName  The fileName to use in the creation
     * @param test_data The data to store in the file
     *
     * @return The File reference
     */
    private File createTestFile(final String fileName, final String test_data)
    {
        final File test = new File(fileName);

        try
        {
            test.createNewFile();
            //Ensure the JVM cleans up if cleanup failures
            test.deleteOnExit();
        }
        catch (IOException e)
        {
            fail(e.getMessage());
        }

        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(test)))
        {
            try
            {
                writer.write(test_data);
            }
            catch (IOException e)
            {
                fail(e.getMessage());
            }
        }
        catch (IOException e)
        {
            fail(e.getMessage());
        }

        return test;
    }

    /** Test that deleteFile only deletes the specified file */
    @Test
    public void testDeleteFile()
    {
        final File test = new File("FileUtilsTest-testDelete");
        // Record file count in parent directory to check it is not changed by delete
        final String path = test.getAbsolutePath();
        final File[] filesBefore = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        int fileCountBefore = filesBefore.length;

        try
        {
            test.createNewFile();
            // Ensure the JVM cleans up if cleanup failues
            test.deleteOnExit();
        }
        catch (IOException e)
        {
            fail(e.getMessage());
        }

        assertTrue(test.exists(), "File does not exists");
        assertTrue(test.isFile(), "File is not a file");

        // Check that file creation can be seen on disk
        final int fileCountCreated = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles().length;
        assertEquals(fileCountBefore + 1, (long) fileCountCreated, "File creation was no registered");

        // Perform Delete
        assertTrue(FileUtils.deleteFile("FileUtilsTest-testDelete"), "Unable to cleanup");

        final boolean condition = !test.exists();
        assertTrue(condition, "File exists after delete");

        // Check that after deletion the file count is now accurate
        final File[] filesAfter = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        final int fileCountAfter = filesAfter.length;
        assertEquals(fileCountBefore, (long) fileCountAfter, "File creation was no registered");

        checkFileLists(filesBefore, filesAfter);
    }

    @Test
    public void testDeleteNonExistentFile()
    {
        final File test = new File("FileUtilsTest-testDelete-" + System.currentTimeMillis());

        final boolean condition1 = !test.exists();
        assertTrue(condition1, "File exists");
        assertFalse(test.isDirectory(), "File is a directory");

        final boolean condition = !FileUtils.delete(test, true);
        assertTrue(condition, "Delete Succeeded ");
    }

    @Test
    public void testDeleteNull()
    {
        try
        {
            FileUtils.delete(null, true);
            fail("Delete with null value should throw NPE.");
        }
        catch (NullPointerException npe)
        {
            // expected path
        }
    }
    
    /**
     * Tests that openFileOrDefaultResource can open a file on the filesystem.
     *
     */
    @Test
    public void testOpenFileOrDefaultResourceOpensFileOnFileSystem() throws Exception
    {
        final File testFile = createTestFileInTmpDir("src=tmpfile");
        final String filenameOnFilesystem = testFile.getCanonicalPath();
        final String defaultResource = "org/apache/qpid/util/default.properties";
        final InputStream is = FileUtils.openFileOrDefaultResource(filenameOnFilesystem, defaultResource, this.getClass().getClassLoader());

        assertNotNull(is, "Stream must not be null");
        final Properties p = new Properties();
        p.load(is);
        assertEquals("tmpfile", p.getProperty("src"));
    }

    /**
     * Tests that openFileOrDefaultResource can open a file on the classpath.
     *
     */
    @Test
    public void testOpenFileOrDefaultResourceOpensFileOnClasspath() throws Exception
    {
        final String mydefaultsResource = "org/apache/qpid/server/util/mydefaults.properties";
        final String defaultResource = "org/apache/qpid/server/util/default.properties";
        final InputStream is = FileUtils.openFileOrDefaultResource(mydefaultsResource, defaultResource, this.getClass().getClassLoader());
        assertNotNull(is, "Stream must not be null");
        final Properties p = new Properties();
        p.load(is);
        assertEquals("mydefaults", p.getProperty("src"));
    }

    /**
     * Tests that openFileOrDefaultResource returns the default resource when file cannot be found.
     */
    @Test
    public void testOpenFileOrDefaultResourceOpensDefaultResource() throws Exception
    {
        final File fileThatDoesNotExist = new File("/does/not/exist.properties");
        assertFalse(fileThatDoesNotExist.exists(), "Test must not exist");

        final String defaultResource = "org/apache/qpid/server/util/default.properties";
        
        final InputStream is = FileUtils.openFileOrDefaultResource(fileThatDoesNotExist.getCanonicalPath(), defaultResource, this.getClass().getClassLoader());
        assertNotNull(is, "Stream must not be null");
        final Properties p = new Properties();
        p.load(is);
        assertEquals("default.properties", p.getProperty("src"));
    }
    
    /**
     * Tests that openFileOrDefaultResource returns null if neither the file nor
     * the default resource can be found..
     */
    @Test
    public void testOpenFileOrDefaultResourceReturnsNullWhenNeitherCanBeFound() throws Exception
    {
        final String mydefaultsResource = "org/apache/qpid/server/util/doesnotexisteiether.properties";
        final String defaultResource = "org/apache/qpid/server/util/doesnotexisteiether.properties";
        final InputStream is = FileUtils.openFileOrDefaultResource(mydefaultsResource, defaultResource, this.getClass().getClassLoader());

        assertNull(is, "Stream must  be null");
    }
    
    /**
     * Given two lists of File arrays ensure they are the same length and all entries in Before are in After
     *
     * @param filesBefore File[]
     * @param filesAfter  File[]
     */
    private void checkFileLists(File[] filesBefore, File[] filesAfter)
    {
        assertNotNull(filesBefore, "Before file list cannot be null");
        assertNotNull(filesAfter, "After file list cannot be null");

        assertEquals(filesBefore.length, (long) filesAfter.length, "File lists are unequal");

        for (final File fileBefore : filesBefore)
        {
            boolean found = false;

            for (final File fileAfter : filesAfter)
            {
                if (fileBefore.getAbsolutePath().equals(fileAfter.getAbsolutePath()))
                {
                    found = true;
                    break;
                }
            }

            assertTrue(found, "File'" + fileBefore.getName() + "' was not in directory afterwards");
        }
    }

    @Test
    public void testNonRecursiveNonEmptyDirectoryDeleteFails()
    {
        final String directoryName = "FileUtilsTest-testRecursiveDelete";
        final File test = new File(directoryName);

        // Record file count in parent directory to check it is not changed by delete
        final String path = test.getAbsolutePath();
        final File[] filesBefore = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        int fileCountBefore = filesBefore.length;

        final boolean condition = !test.exists();
        assertTrue(condition, "Directory exists");

        test.mkdir();

        // Create a file in the directory
        final String fileName = test.getAbsolutePath() + File.separatorChar + "testFile";
        final File subFile = new File(fileName);
        try
        {
            subFile.createNewFile();
            // Ensure the JVM cleans up if cleanup failues
            subFile.deleteOnExit();
        }
        catch (IOException e)
        {
            fail(e.getMessage());
        }
        // Ensure the JVM cleans up if cleanup failues
        // This must be after the subFile as the directory must be empty before
        // the delete is performed
        test.deleteOnExit();

        // Try and delete the non-empty directory
        assertFalse(FileUtils.deleteDirectory(directoryName), "Non Empty Directory was successfully deleted.");

        // Check directory is still there
        assertTrue(test.exists(), "Directory was deleted.");

        // Clean up
        assertTrue(FileUtils.delete(test, true), "Unable to cleanup");

        //Check that after deletion the file count is now accurate
        final File[] filesAfter = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        final int fileCountAfter = filesAfter.length;
        assertEquals(fileCountBefore, (long) fileCountAfter, "File creation was no registered");

        checkFileLists(filesBefore, filesAfter);
    }

    /** Test that an empty directory can be deleted with deleteDirectory */
    @Test
    public void testEmptyDirectoryDelete()
    {
        final String directoryName = "FileUtilsTest-testRecursiveDelete";
        final File test = new File(directoryName);

        // Record file count in parent directory to check it is not changed by delete
        final String path = test.getAbsolutePath();
        final File[] filesBefore = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        final int fileCountBefore = filesBefore.length;

        final boolean condition1 = !test.exists();
        assertTrue(condition1, "Directory exists");

        test.mkdir();
        // Ensure the JVM cleans up if cleanup failues
        test.deleteOnExit();

        // Try and delete the empty directory
        assertTrue(FileUtils.deleteDirectory(directoryName), "Non Empty Directory was successfully deleted.");

        // Check directory is still there
        final boolean condition = !test.exists();
        assertTrue(condition, "Directory was deleted.");

        // Check that after deletion the file count is now accurate
        final File[] filesAfter = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        final int fileCountAfter = filesAfter.length;
        assertEquals(fileCountBefore, (long) fileCountAfter, "File creation was no registered");

        checkFileLists(filesBefore, filesAfter);

    }

    /** Test that deleteDirectory on a non empty directory to complete */
    @Test
    public void testNonEmptyDirectoryDelete()
    {
        final String directoryName = "FileUtilsTest-testRecursiveDelete";
        final File test = new File(directoryName);

        final boolean condition = !test.exists();
        assertTrue(condition, "Directory exists");

        // Record file count in parent directory to check it is not changed by delete
        final String path = test.getAbsolutePath();
        final File[] filesBefore = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        final int fileCountBefore = filesBefore.length;

        test.mkdir();

        // Create a file in the directory
        final String fileName = test.getAbsolutePath() + File.separatorChar + "testFile";
        final File subFile = new File(fileName);
        try
        {
            subFile.createNewFile();
            // Ensure the JVM cleans up if cleanup failues
            subFile.deleteOnExit();
        }
        catch (IOException e)
        {
            fail(e.getMessage());
        }

        // Ensure the JVM cleans up if cleanup failues
        // This must be after the subFile as the directory must be empty before
        // the delete is performed
        test.deleteOnExit();

        // Try and delete the non-empty directory non-recursively
        assertFalse(FileUtils.delete(test, false), "Non Empty Directory was successfully deleted.");

        // Check directory is still there
        assertTrue(test.exists(), "Directory was deleted.");

        // Clean up
        assertTrue(FileUtils.delete(test, true), "Unable to cleanup");

        // Check that after deletion the file count is now accurate
        final File[] filesAfter = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        final int fileCountAfter = filesAfter.length;
        assertEquals(fileCountBefore, (long) fileCountAfter, "File creation was no registered");

        checkFileLists(filesBefore, filesAfter);

    }

    /** Test that a recursive delete successeds */
    @Test
    public void testRecursiveDelete()
    {
        final String directoryName = "FileUtilsTest-testRecursiveDelete";
        final File test = new File(directoryName);

        final boolean condition1 = !test.exists();
        assertTrue(condition1, "Directory exists");

        // Record file count in parent directory to check it is not changed by delete
        final String path = test.getAbsolutePath();
        final File[] filesBefore = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        final int fileCountBefore = filesBefore.length;

        test.mkdir();

        createSubDir(directoryName, 2, 4);

        // Ensure the JVM cleans up if cleanup failures
        // This must be after the sub dir creation as the delete order is
        // recorded and the directory must be empty to be deleted.
        test.deleteOnExit();

        assertFalse(FileUtils.delete(test, false), "Non recursive delete was able to directory");

        assertTrue(test.exists(), "File does not exist after non recursive delete");

        assertTrue(FileUtils.delete(test, true), "Unable to cleanup");

        final boolean condition = !test.exists();
        assertTrue(condition, "File  exist after recursive delete");

        // Check that after deletion the file count is now accurate
        final File[] filesAfter = new File(path.substring(0, path.lastIndexOf(File.separator))).listFiles();
        final int fileCountAfter = filesAfter.length;
        assertEquals(fileCountBefore, (long) fileCountAfter, "File creation was no registered");

        checkFileLists(filesBefore, filesAfter);
    }

    private void createSubDir(String path, int directories, int files)
    {
        final File directory = new File(path);

        assertTrue(directory.exists(), "Directory" + path + " does not exists");

        for (int dir = 0; dir < directories; dir++)
        {
            final String subDirName = path + File.separatorChar + "sub" + dir;
            final File subDir = new File(subDirName);

            subDir.mkdir();

            createSubDir(subDirName, directories - 1, files);
            // Ensure the JVM cleans up if cleanup failues
            // This must be after the sub dir creation as the delete order is
            // recorded and the directory must be empty to be deleted.
            subDir.deleteOnExit();
        }

        for (int file = 0; file < files; file++)
        {
            final String subDirName = path + File.separatorChar + "file" + file;
            final File subFile = new File(subDirName);
            try
            {
                subFile.createNewFile();
                // Ensure the JVM cleans up if cleanup failues
                subFile.deleteOnExit();
            }
            catch (IOException e)
            {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Test searchFile(File file, String search) will find a match when it
     * exists.
     *
     * @throws java.io.IOException if unable to perform test setup
     */
    @Test
    public void testSearchSucceed() throws IOException
    {
        final File logfile = File.createTempFile("FileUtilsTest-testSearchSucceed", ".out");
        logfile.deleteOnExit();

        try
        {
            prepareFileForSearchTest(logfile);

            final List<String> results = FileUtils.searchFile(logfile, SEARCH_STRING);

            assertNotNull(results, "Null result set returned");

            assertEquals(1, (long) results.size(), "Results do not contain expected count");
        }
        finally
        {
            logfile.delete();
        }
    }

    /**
     * Test searchFile(File file, String search) will not find a match when the
     * test string does not exist.
     *
     * @throws java.io.IOException if unable to perform test setup
     */
    @Test
    public void testSearchFail() throws IOException
    {
        final File logfile = File.createTempFile("FileUtilsTest-testSearchFail", ".out");
        logfile.deleteOnExit();

        try
        {
            prepareFileForSearchTest(logfile);

            final List<String> results = FileUtils.searchFile(logfile, "Hello");

            assertNotNull(results, "Null result set returned");

            // Validate we only got one message
            if (results.size() > 0)
            {
                System.err.println("Unexpected messages");

                for (final String msg : results)
                {
                    System.err.println(msg);
                }
            }

            assertEquals(0, (long) results.size(), "Results contains data when it was not expected");
        }
        finally
        {
            logfile.delete();
        }
    }

    /**
     * Write the SEARCH_STRING in to the given file.
     *
     * @param logfile The file to write the SEARCH_STRING into
     *
     * @throws IOException if an error occurs
     */
    private void prepareFileForSearchTest(final File logfile) throws IOException
    {
        final BufferedWriter writer = new BufferedWriter(new FileWriter(logfile));
        writer.append(SEARCH_STRING);
        writer.flush();
        writer.close();
    }
}
