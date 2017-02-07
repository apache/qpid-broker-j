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
package org.apache.qpid.server.protocol;

import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.framing.AMQMethodBody;
import org.apache.qpid.server.framing.ContentBody;
import org.apache.qpid.server.framing.ContentHeaderBody;
import org.apache.qpid.server.framing.HeartbeatBody;
import org.apache.qpid.server.framing.MethodRegistry;
import org.apache.qpid.server.transport.ByteBufferSender;


/**
 * AMQVersionAwareProtocolSession is implemented by all AMQP session classes, that need to provide an awareness to
 * callers of the version of the AMQP protocol that they are able to work with.
 * <p>
 * TODO Why is this a seperate interface to {@link ProtocolVersionAware}, could they be combined into a single
 *       interface and one of them eliminated? Move getRegistry method to ProtocolVersionAware, make the sessions
 *       implement AMQProtocolWriter directly and drop this interface.
 */
public interface AMQVersionAwareProtocolSession extends org.apache.qpid.server.protocol.AMQProtocolWriter, ProtocolVersionAware
{
    /**
     * Gets the method registry for a specific version of the AMQP.
     *
     * @return The method registry for a specific version of the AMQP.
     */

    MethodRegistry getMethodRegistry();


    public void methodFrameReceived(int channelId, AMQMethodBody body) throws QpidException;
    public void contentHeaderReceived(int channelId, ContentHeaderBody body) throws QpidException;
    public void contentBodyReceived(int channelId, ContentBody body) throws QpidException;
    public void heartbeatBodyReceived(int channelId, HeartbeatBody body) throws QpidException;


    public void setSender(ByteBufferSender sender);

}
