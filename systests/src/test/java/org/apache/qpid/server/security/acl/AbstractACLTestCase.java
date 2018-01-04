/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.security.acl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 * Abstract test case for ACLs.
 *
 * This base class contains convenience methods to manage ACL files and implements a mechanism that allows each
 * test method to run its own setup code before the broker starts.
 *
 * @see MessagingACLTest
 */
public abstract class AbstractACLTestCase extends QpidBrokerTestCase
{
    private Connection _adminConnection;
    private Session _adminSession;

    @Override
    public void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().addGroupFileConfiguration(QPID_HOME + "/etc/groups-systests");

        // run test specific setup
        String testSetup = getName().replace("test", "setUp");
        try
        {
            Method setup = getClass().getDeclaredMethod(testSetup);
            setup.invoke(this);
        }
        catch (NoSuchMethodException e)
        {
            // Ignore
        }
        catch (InvocationTargetException e)
        {
            throw (Exception) e.getTargetException();
        }

        super.setUp();

        _adminConnection = getConnection("test", "admin", "admin");
        _adminSession = _adminConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _adminConnection.start();
    }

    public Connection getAdminConnection()
    {
        return _adminConnection;
    }

    public Session getAdminSession()
    {
        return _adminSession;
    }

    public void writeACLFile(final String...rules) throws IOException
    {
        writeACLFileUtil(this, rules);
    }

    public static String writeACLFileUtil(QpidBrokerTestCase testcase, String...rules) throws IOException
    {
        File aclFile = File.createTempFile(testcase.getClass().getSimpleName(), testcase.getName());
        aclFile.deleteOnExit();

        testcase.getDefaultBrokerConfiguration().addAclFileConfiguration(aclFile.getAbsolutePath());

        PrintWriter out = new PrintWriter(new FileWriter(aclFile));
        out.println(String.format("# %s", testcase.getName()));
        for (String line : rules)
        {
            out.println(line);
        }
        out.close();
        return aclFile.getCanonicalPath();
    }

    public Connection getConnection(String vhost, String username, String password) throws Exception
    {
        return getConnectionBuilder().setFailover(false)
                                     .setVirtualHost(vhost)
                                     .setSyncPublish(true)
                                     .setPassword(password)
                                     .setUsername(username)
                                     .build();
    }

    public void writeACLFileWithAdminSuperUser(String... rules) throws IOException
    {
        List<String> newRules = new ArrayList<>(Arrays.asList(rules));
        newRules.add(0, "ACL ALLOW-LOG admin ALL ALL");
        writeACLFile(newRules.toArray(new String[newRules.size()]));
    }

    protected void createQueue(final String queueName) throws JMSException
    {
        createEntityUsingAmqpManagement(queueName, getAdminSession(), "org.apache.qpid.Queue");
    }

    protected void bindExchangeToQueue(final String exchangeName, final String queueName) throws JMSException
    {
        final Map<String, Object> bindingArguments = new HashMap<>();
        bindingArguments.put("destination", queueName);
        bindingArguments.put("bindingKey", queueName);

        performOperationUsingAmqpManagement(exchangeName,
                                            "bind",
                                            _adminConnection.createSession(false, Session.AUTO_ACKNOWLEDGE),
                                            "org.apache.qpid.Exchange",
                                            bindingArguments);
    }

    protected void assertJMSExceptionMessageContains(final JMSException e, final String expectedMessage)
    {
        Set<Throwable> examined = new HashSet<>();
        Throwable current = e;
        do
        {
            if (current.getMessage().contains(expectedMessage))
            {
                return;
            }
            examined.add(current);
            current = current.getCause();
        }
        while (current != null && !examined.contains(current));
        e.printStackTrace();
        fail("Unexpected message. Root exception : "
             + e.getMessage()
             + " expected root or underlyings to contain : "
             + expectedMessage);
    }
}
