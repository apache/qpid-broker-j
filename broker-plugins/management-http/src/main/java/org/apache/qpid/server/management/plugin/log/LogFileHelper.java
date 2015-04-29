/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.qpid.server.management.plugin.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public class LogFileHelper
{
    public static final String GZIP_MIME_TYPE = "application/x-gzip";
    public static final String TEXT_MIME_TYPE = "text/plain";
    public static final String ZIP_MIME_TYPE = "application/zip";
    public static final String GZIP_EXTENSION = ".gz";
    private static final int BUFFER_LENGTH = 1024 * 4;

    public LogFileHelper()
    {
        throw new UnsupportedOperationException();
    }

    public List<LogFileDetails> findLogFileDetails(String[] requestedFiles)
    {
        throw new UnsupportedOperationException();
    }

    public List<LogFileDetails> getLogFileDetails(boolean includeLogFileLocation)
    {
        throw new UnsupportedOperationException();
    }

    public void writeLogFiles(List<LogFileDetails> logFiles, OutputStream os) throws IOException
    {
        ZipOutputStream out = new ZipOutputStream(os);
        try
        {
            addLogFileEntries(logFiles, out);
        }
        finally
        {
            out.close();
        }
    }

    public void writeLogFile(File file, OutputStream os) throws IOException
    {
        FileInputStream fis = new FileInputStream(file);
        try
        {
            byte[] bytes = new byte[BUFFER_LENGTH];
            int length = 1;
            while ((length = fis.read(bytes)) != -1)
            {
                os.write(bytes, 0, length);
            }
        }
        finally
        {
            fis.close();
        }
    }

    private void addLogFileEntries(List<LogFileDetails> files, ZipOutputStream out) throws IOException
    {
        for (LogFileDetails logFileDetails : files)
        {
            File file = logFileDetails.getLocation();
            if (file.exists())
            {
                ZipEntry entry = new ZipEntry(logFileDetails.getAppenderName() + "/" + logFileDetails.getName());
                entry.setSize(file.length());
                out.putNextEntry(entry);
                writeLogFile(file, out);
                out.closeEntry();
            }
            out.flush();
        }
    }

}
