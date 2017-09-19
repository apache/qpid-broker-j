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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;


public class Amqp0xMessagingACLTest extends AbstractACLTestCase
{
    public void setUpCreateNamedQueueFailure() throws Exception
    {
        writeACLFileWithAdminSuperUser("ACL ALLOW-LOG client ACCESS VIRTUALHOST",
                                       "ACL ALLOW-LOG client CREATE QUEUE name=\"ValidQueue\"");
    }

    /*
     * Legacy client creates the queue as part of consumer creation
     */
    public void testCreateNamedQueueFailure() throws Exception
    {
        Connection conn = getConnection("test", "client", "guest");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = sess.createQueue("IllegalQueue");

        try
        {
            sess.createConsumer(dest);
            fail("Test failed as Queue creation succeeded.");
        }
        catch (JMSException e)
        {
            assertJMSExceptionMessageContains(e, "Permission CREATE is denied for : Queue");
        }
    }


}
