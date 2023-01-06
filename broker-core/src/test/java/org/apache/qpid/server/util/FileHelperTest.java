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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class FileHelperTest extends UnitTestBase
{
    private static final String TEST_FILE_PERMISSIONS = "rwxr-x---";
    private File _testFile;
    private FileHelper _fileHelper;

    @BeforeEach
    public void setUp() throws Exception
    {
        _testFile = new File(TMP_FOLDER, "test-" + System.currentTimeMillis());
        _fileHelper = new FileHelper();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        Files.deleteIfExists(_testFile.toPath());
    }

    @Test
    public void testCreateNewFile() throws Exception
    {
        assertFalse(_testFile.exists(), "File should not exist");
        final Path path = _fileHelper.createNewFile(_testFile, TEST_FILE_PERMISSIONS);
        assertTrue(path.toFile().exists(), "File was not created");
        if (Files.getFileAttributeView(path, PosixFileAttributeView.class) != null)
        {
            assertPermissions(path);
        }
    }

    @Test
    public void testCreateNewFileUsingRelativePath() throws Exception
    {
        _testFile = new File("./tmp-" + System.currentTimeMillis());
        assertFalse(_testFile.exists(), "File should not exist");
        final Path path = _fileHelper.createNewFile(_testFile, TEST_FILE_PERMISSIONS);
        assertTrue(path.toFile().exists(), "File was not created");
        if (Files.getFileAttributeView(path, PosixFileAttributeView.class) != null)
        {
            assertPermissions(path);
        }
    }

    @Test
    public void testWriteFileSafely() throws Exception
    {
        final Path path = _fileHelper.createNewFile(_testFile, TEST_FILE_PERMISSIONS);
        _fileHelper.writeFileSafely(path, file ->
        {
            Files.write(file.toPath(), "test".getBytes(StandardCharsets.UTF_8));
            assertEquals(_testFile.getAbsolutePath() + ".tmp", file.getPath(), "Unexpected name");
        });

        assertTrue(path.toFile().exists(), "File was not created");

        if (Files.getFileAttributeView(path, PosixFileAttributeView.class) != null)
        {
            assertPermissions(path);
        }

        String content = Files.readString(path);
        assertEquals("test", content, "Unexpected file content");
    }

    @Test
    public void testAtomicFileMoveOrReplace() throws Exception
    {
        final Path path = _fileHelper.createNewFile(_testFile, TEST_FILE_PERMISSIONS);
        Files.write(path, "test".getBytes(StandardCharsets.UTF_8));
        _testFile = _fileHelper.atomicFileMoveOrReplace(path, path.resolveSibling(_testFile.getName() + ".target")).toFile();

        assertFalse(path.toFile().exists(), "File was not moved");
        assertTrue(_testFile.exists(), "Target file does not exist");

        if (Files.getFileAttributeView(_testFile.toPath(), PosixFileAttributeView.class) != null)
        {
            assertPermissions(_testFile.toPath());
        }
    }

    @Test
    public void testIsWritableDirectoryForFilePath() throws Exception
    {
        final File workDir = TestFileUtils.createTestDirectory("test", true);
        try
        {
            final File file = new File(workDir, getTestName());
            file.createNewFile();
            assertFalse(_fileHelper.isWritableDirectory(file.getAbsolutePath()), "Should return false for a file");
        }
        finally
        {
            FileUtils.delete(workDir, true);
        }
    }


    @Test
    public void testIsWritableDirectoryForNonWritablePath()
    {
        final File workDir = TestFileUtils.createTestDirectory("test", true);
        try
        {
            if (Files.getFileAttributeView(workDir.toPath(), PosixFileAttributeView.class) != null)
            {
                final File file = new File(workDir, getTestName());
                file.mkdirs();
                if (file.setWritable(false, false))
                {
                    assertFalse(_fileHelper.isWritableDirectory(new File(file, "test").getAbsolutePath()),
                            "Should return false for non writable folder");
                }
            }
        }
        finally
        {
            FileUtils.delete(workDir, true);
        }
    }

    private void assertPermissions(final Path path) throws IOException
    {
        final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ), "Unexpected owner read permission");
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE),
                "Unexpected owner write permission");
        assertTrue(permissions.contains(PosixFilePermission.OWNER_EXECUTE),
                "Unexpected owner exec permission");
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ), "Unexpected group read permission");
        assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE),
                "Unexpected group write permission");
        assertTrue(permissions.contains(PosixFilePermission.GROUP_EXECUTE),
                "Unexpected group exec permission");
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_READ),
                "Unexpected others read permission");
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE),
                "Unexpected others write permission");
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_EXECUTE),
                "Unexpected others exec permission");
    }
}
