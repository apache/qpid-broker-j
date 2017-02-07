/*
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
 */

package org.apache.qpid.server.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.util.BaseAction;
import org.apache.qpid.server.util.FileHelper;
import org.apache.qpid.server.util.FileUtils;

public abstract class AbstractJsonFileStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJsonFileStore.class);

    private final FileHelper _fileHelper;

    private String _directoryName;
    private FileLock _fileLock;
    private String _configFileName;
    private String _backupFileName;
    private String _tempFileName;
    private String _lockFileName;

    protected AbstractJsonFileStore()
    {
        _fileHelper = new FileHelper();
    }

    abstract protected ObjectMapper getSerialisationObjectMapper();
//    abstract protected ObjectMapper getDeserialisationObjectMapper();

    protected void setup(String name,
                         String storePath,
                         String posixFileAttributes,
                         Object initialData)
    {
        if(storePath == null)
        {
            throw new StoreException("Cannot determine path for configuration storage");
        }
        File fileFromSettings = new File(storePath);
        File parentFromSettings = fileFromSettings.getAbsoluteFile().getParentFile();
        boolean isFile = fileFromSettings.exists() && fileFromSettings.isFile();
        if(!isFile)
        {
            if(fileFromSettings.exists())
            {
                isFile = false;
            }
            else if(fileFromSettings.getName().endsWith(File.separator))
            {
                isFile = false;
            }
            else if(fileFromSettings.getName().endsWith(".json"))
            {
                isFile = true;
            }
            else if(parentFromSettings.isDirectory() && fileFromSettings.getName().contains("."))
            {
                isFile = true;
            }
        }
        if(isFile)
        {
            _directoryName = parentFromSettings.getAbsolutePath();
            _configFileName = fileFromSettings.getName();
            _backupFileName = fileFromSettings.getName() + ".bak";
            _tempFileName = fileFromSettings.getName() + ".tmp";

            _lockFileName = fileFromSettings.getName() + ".lck";
        }
        else
        {
            _directoryName = storePath;
            _configFileName = name + ".json";
            _backupFileName = name + ".bak";
            _tempFileName = name + ".tmp";

            _lockFileName = name + ".lck";
        }


        checkDirectoryIsWritable(_directoryName);
        getFileLock();

        Path storeFile = new File(_directoryName, _configFileName).toPath();
        Path backupFile = new File(_directoryName, _backupFileName).toPath();
        if(!Files.exists(storeFile))
        {
            if(!Files.exists(backupFile))
            {
                try
                {
                    storeFile = _fileHelper.createNewFile(storeFile, posixFileAttributes);
                    getSerialisationObjectMapper().writeValue(storeFile.toFile(), initialData);
                }
                catch (IOException e)
                {
                    throw new StoreException("Could not write configuration file " + storeFile, e);
                }
            }
            else
            {
                try
                {
                    _fileHelper.atomicFileMoveOrReplace(backupFile, storeFile);
                }
                catch (IOException e)
                {
                    throw new StoreException("Could not move backup to configuration file " + storeFile, e);
                }
            }
        }

        try
        {
            Files.deleteIfExists(backupFile);
        }
        catch (IOException e)
        {
            throw new StoreException("Could not delete backup file " + backupFile, e);
        }
    }

    protected void cleanup()
    {
        releaseFileLock();
    }

    private void getFileLock()
    {
        File lockFile = new File(_directoryName, _lockFileName);
        try
        {
            lockFile.createNewFile();
            lockFile.deleteOnExit();

            @SuppressWarnings("resource")
            FileOutputStream out = new FileOutputStream(lockFile);
            FileChannel channel = out.getChannel();
            _fileLock = channel.tryLock();
        }
        catch (IOException ioe)
        {
            throw new StoreException("Cannot create the lock file " + lockFile.getName(), ioe);
        }
        catch(OverlappingFileLockException e)
        {
            _fileLock = null;
        }

        if(_fileLock == null)
        {
            throw new StoreException("Cannot get lock on file " + lockFile.getAbsolutePath() + ". Is another instance running?");
        }
    }

    private void checkDirectoryIsWritable(String directoryName)
    {
        File dir = new File(directoryName);
        if(dir.exists())
        {
            if(dir.isDirectory())
            {
                if(!dir.canWrite())
                {
                    throw new StoreException("Configuration path " + directoryName + " exists, but is not writable");
                }

            }
            else
            {
                throw new StoreException("Configuration path " + directoryName + " exists, but is not a directory");
            }
        }
        else if(!dir.mkdirs())
        {
            throw new StoreException("Cannot create directory " + directoryName);
        }
    }

    protected void save(final Object data)
    {
        try
        {
            Path tmpFile = new File(_directoryName, _tempFileName).toPath();
            _fileHelper.writeFileSafely( new File(_directoryName, _configFileName).toPath(),
                                         new File(_directoryName, _backupFileName).toPath(),
                                         tmpFile,
                                         new BaseAction<File, IOException>()
                                         {
                                             @Override
                                             public void performAction(File file) throws IOException
                                             {
                                                 getSerialisationObjectMapper().writeValue(file, data);
                                             }
                                         });
        }
        catch (IOException e)
        {
            throw new StoreException("Cannot save to store", e);
        }
    }

    private void releaseFileLock()
    {
        if (_fileLock != null)
        {
            try
            {
                _fileLock.release();
                _fileLock.channel().close();
            }
            catch (IOException e)
            {
                throw new StoreException("Failed to release lock " + _fileLock, e);
            }
            finally
            {
                _fileLock = null;
            }
        }
    }

    protected File getConfigFile()
    {
        return new File(_directoryName, _configFileName);
    }

    protected void delete(final String storePath)
    {
        if (storePath != null)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Deleting store " + storePath);
            }

            File configFile = new File(storePath);
            if (!FileUtils.delete(configFile, true))
            {
                LOGGER.info("Failed to delete the store at location " + storePath);
            }
        }

        _configFileName = null;
        _directoryName = null;
    }
}
