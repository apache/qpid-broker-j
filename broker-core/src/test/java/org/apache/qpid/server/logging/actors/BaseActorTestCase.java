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
package org.apache.qpid.server.logging.actors;

import org.junit.After;
import org.junit.Before;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.UnitTestMessageLogger;
import org.apache.qpid.test.utils.UnitTestBase;


public abstract class BaseActorTestCase extends UnitTestBase
{
    private boolean _statusUpdatesEnabled = true;
    private UnitTestMessageLogger _rawLogger;
    private EventLogger _eventLogger;

    @Before
    public void setUp() throws Exception
    {
        _rawLogger = new UnitTestMessageLogger(_statusUpdatesEnabled);
        _eventLogger = new EventLogger(_rawLogger);
    }

    @After
    public void tearDown() throws Exception
    {
        if(_rawLogger != null)
        {
            _rawLogger.clearLogMessages();
        }
    }

    public String sendTestLogMessage()
    {
        String message = "Test logging: " + getTestName();
        sendTestLogMessage(message);

        return message;
    }

    public void sendTestLogMessage(final String message)
    {
        getEventLogger().message(new LogSubject()
                          {
                              @Override
                              public String toLogString()
                              {
                                  return message;
                              }

                          }, new LogMessage()
                          {
                              @Override
                              public String toString()
                              {
                                  return message;
                              }

                              @Override
                              public String getLogHierarchy()
                              {
                                  return "test.hierarchy";
                              }
                          }
                         );
    }

    public boolean isStatusUpdatesEnabled()
    {
        return _statusUpdatesEnabled;
    }

    public void setStatusUpdatesEnabled(boolean statusUpdatesEnabled)
    {
        _statusUpdatesEnabled = statusUpdatesEnabled;
    }

    public UnitTestMessageLogger getRawLogger()
    {
        return _rawLogger;
    }

    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }

}
