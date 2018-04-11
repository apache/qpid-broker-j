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
package org.apache.qpid.server.store.berkeleydb;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import javax.jms.Connection;

import org.junit.Before;

import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBVirtualHostNode;
import org.apache.qpid.systests.JmsTestBase;

public abstract class UpgradeTestBase extends JmsTestBase
{
    @Before
    public void restartWithOldStore() throws Exception
    {
        Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            connection.start();
            Map<String, Object> attributes =
                    readEntityUsingAmqpManagement(getVirtualHostName(), "org.apache.qpid.VirtualHostNode", false, connection);
            String storePath = (String) attributes.get(BDBVirtualHostNode.STORE_PATH);

            updateEntityUsingAmqpManagement(getVirtualHostName(),
                                            "org.apache.qpid.VirtualHostNode",
                                            Collections.singletonMap("desiredState", "STOPPED"), connection);
            copyStore(new File(storePath));
            updateEntityUsingAmqpManagement(getVirtualHostName(),
                                            "org.apache.qpid.VirtualHostNode",
                                            Collections.singletonMap("desiredState", "ACTIVE"), connection);
        }
        finally
        {
            connection.close();
        }
    }

    private void copyStore(final File directory) throws IOException
    {
        if (directory.exists() && directory.isDirectory())
        {
            FileUtils.delete(directory, true);
        }
        if (directory.mkdirs())
        {
            try (InputStream src = getClass().getClassLoader()
                                             .getResourceAsStream(getOldStoreResourcePath()))
            {
                FileUtils.copy(src, new File(directory, "00000000.jdb"));
            }
        }
        else
        {
            fail(String.format("Cannot copy store file into '%s'", directory.getAbsolutePath()));
        }
    }


    abstract String getOldStoreResourcePath();
}
