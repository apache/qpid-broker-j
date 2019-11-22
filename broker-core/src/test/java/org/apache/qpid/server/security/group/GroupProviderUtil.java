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
package org.apache.qpid.server.security.group;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

class GroupProviderUtil
{

    private final FileGroupDatabase _groupDatabase;
    private final String _groupFile;

    GroupProviderUtil(FileGroupDatabase groupDatabase) throws IOException
    {
        this._groupDatabase = groupDatabase;
        this._groupFile = createEmptyTestGroupFile();
    }

    void writeAndSetGroupFile(String... groupAndUsers)
            throws Exception
    {
        writeGroupFile(groupAndUsers);
        _groupDatabase.setGroupFile(_groupFile);
    }

    void writeGroupFile(String... groupAndUsers) throws Exception
    {
        if (groupAndUsers.length % 2 != 0)
        {
            throw new IllegalArgumentException("Number of groupAndUsers must be even");
        }

        Properties props = new Properties();
        for (int i = 0; i < groupAndUsers.length; i = i + 2)
        {
            String group = groupAndUsers[i];
            String users = groupAndUsers[i + 1];
            props.put(group, users);
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(_groupFile))
        {
            props.store(fileOutputStream, "test group file");
        }
    }

    String createEmptyTestGroupFile() throws IOException
    {
        File tmpGroupFile = File.createTempFile("groups", "grp");
        tmpGroupFile.deleteOnExit();

        return tmpGroupFile.getAbsolutePath();
    }

    String getGroupFile()
    {
        return _groupFile;
    }
}
