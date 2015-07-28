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
package org.apache.qpid.transport.network;

import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.Connection;
import org.apache.qpid.transport.ConnectionDelegate;
import org.apache.qpid.transport.ConnectionListener;
import org.apache.qpid.transport.Constant;
import org.apache.qpid.transport.ExceptionHandlingByteBufferReceiver;
import org.apache.qpid.transport.network.security.sasl.SASLReceiver;
import org.apache.qpid.transport.network.security.sasl.SASLSender;

/**
 * ConnectionBinding
 *
 */

public abstract class ConnectionBinding
{

    public static ConnectionBinding get(final Connection connection)
    {
        return new ConnectionBinding()
        {
            public Connection connection()
            {
                return connection;
            }
        };
    }

    public static ConnectionBinding get(final ConnectionDelegate delegate)
    {
        return new ConnectionBinding()
        {
            public Connection connection()
            {
                Connection conn = new Connection();
                conn.setConnectionDelegate(delegate);
                return conn;
            }
        };
    }

    public abstract Connection connection();

    public Connection endpoint(ByteBufferSender sender)
    {
        Connection conn = connection();

        if (conn.getConnectionSettings() != null && 
            conn.getConnectionSettings().isUseSASLEncryption())
        {
            sender = new SASLSender(sender);
            conn.addConnectionListener((ConnectionListener)sender);
        }
        
        // XXX: hardcoded max-frame
        Disassembler dis = new Disassembler(sender, Constant.MIN_MAX_FRAME_SIZE);
        conn.addFrameSizeObserver(dis);
        conn.setSender(dis);
        return conn;
    }

    public ExceptionHandlingByteBufferReceiver receiver(Connection conn)
    {
        final InputHandler inputHandler = new InputHandler(new Assembler(conn), false);
        conn.addFrameSizeObserver(inputHandler);
        if (conn.getConnectionSettings() != null &&
            conn.getConnectionSettings().isUseSASLEncryption())
        {
            SASLReceiver receiver = new SASLReceiver(inputHandler);
            conn.addConnectionListener(receiver);
            return receiver;
        }
        else
        {
            return inputHandler;
        }
    }

}
