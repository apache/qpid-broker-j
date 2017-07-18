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
package org.apache.qpid.server.logging.subjects;

import java.text.MessageFormat;

import org.apache.qpid.server.transport.AMQPConnection;

import static org.apache.qpid.server.logging.subjects.LogSubjectFormat.CONNECTION_FORMAT;
import static org.apache.qpid.server.logging.subjects.LogSubjectFormat.SOCKET_FORMAT;
import static org.apache.qpid.server.logging.subjects.LogSubjectFormat.USER_FORMAT;

/** The Connection LogSubject */
public class ConnectionLogSubject extends AbstractLogSubject
{

    // The Session this Actor is representing
    private AMQPConnection<?> _connection;

    public ConnectionLogSubject(AMQPConnection<?> connection)
    {
        _connection = connection;
    }

    // Used to stop re-creating the _logString when we reach our final format
    private boolean _upToDate = false;

    /**
     * Update the LogString as the Connection process proceeds.
     *
     * When the Session has an authorized ID add that to the string.
     *
     * When the Session then gains a Vhost add that to the string, at this point
     * we can set upToDate = true as the _logString will not need to be updated
     * from this point onwards.
     */
    private void updateLogString()
    {
        if (!_upToDate)
        {
            if (_connection.getAuthorizedPrincipal() != null)
            {
                if (_connection.getAddressSpaceName() != null)
                {
                    /**
                     * LOG FORMAT used by the AMQPConnectorActor follows
                     * ConnectionLogSubject.CONNECTION_FORMAT :
                     * con:{0}({1}@{2}/{3})
                     *
                     * Uses a MessageFormat call to insert the required values
                     * according to these indices:
                     *
                     * 0 - Connection ID 1 - User ID 2 - IP 3 - Virtualhost
                     */
                    setLogString("[" + MessageFormat.format(CONNECTION_FORMAT,
                                                            _connection.getConnectionId(),
                                                            _connection.getAuthorizedPrincipal().getName(),
                                                            _connection.getRemoteAddressString(),
                                                            _connection.getAddressSpaceName())
                                 + "] ");

                    _upToDate = true;
                }
                else
                {
                    setLogString("[" + MessageFormat.format(USER_FORMAT,
                                                            _connection.getConnectionId(),
                                                            _connection.getAuthorizedPrincipal().getName(),
                                                            _connection.getRemoteAddressString())
                                 + "] ");

                }
            }
            else
            {
                    setLogString("[" + MessageFormat.format(SOCKET_FORMAT,
                                                            _connection.getConnectionId(),
                                                            _connection.getRemoteAddressString())
                                 + "] ");
            }
        }
    }

    @Override
    public String toLogString()
    {
        updateLogString();
        return super.toLogString();
    }
}
