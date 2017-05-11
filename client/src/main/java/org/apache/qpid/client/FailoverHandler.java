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
package org.apache.qpid.client;

import org.apache.qpid.client.failover.FailoverState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.AMQDisconnectedException;
import org.apache.qpid.client.state.AMQState;
import org.apache.qpid.client.state.AMQStateManager;

/**
 * FailoverHandler is a continuation that performs the failover procedure on a protocol session. As described in the
 * class level comment for {@link AMQProtocolHandler}, a protocol connection can span many physical transport
 * connections, failing over to a new connection if the transport connection fails. The procedure to establish a new
 * connection is expressed as a continuation, in order that it may be run in a seperate thread to the i/o thread that
 * detected the failure and is used to handle the communication to establish a new connection.
 *
 */
public class FailoverHandler implements Runnable
{
    /** Used for debugging. */
    private static final Logger _logger = LoggerFactory.getLogger(FailoverHandler.class);

    /** Holds the protocol handler for the failed connection, upon which the new connection is to be set up. */
    private final AMQProtocolHandler _amqProtocolHandler;

    public FailoverHandler(AMQProtocolHandler amqProtocolHandler)
    {
        _amqProtocolHandler = amqProtocolHandler;
    }

    /**
     * Performs the failover procedure.
     */
    @Override
    public void run()
    {
        AMQConnection connection = _amqProtocolHandler.getConnection();


        //Clear the exception now that we have the failover mutex there can be no one else waiting for a frame so
        // we can clear the exception.
        _amqProtocolHandler.failoverInProgress();

        // We switch in a new state manager temporarily so that the interaction to get to the "connection open"
        // state works, without us having to terminate any existing "state waiters". We could theoretically
        // have a state waiter waiting until the connection is closed for some reason. Or in future we may have
        // a slightly more complex state model therefore I felt it was worthwhile doing this.
        AMQStateManager existingStateManager = _amqProtocolHandler.getStateManager();


        // Use a fresh new StateManager for the reconnection attempts
        _amqProtocolHandler.setStateManager(new AMQStateManager());


        if (!connection.firePreFailover(false))
        {
            _logger.info("Failover process veto-ed by client");

            //Restore Existing State Manager
            _amqProtocolHandler.setStateManager(existingStateManager);

            AMQDisconnectedException cause = new AMQDisconnectedException("Failover was vetoed by client", null);

            connection.closed(cause);
            return;
        }

        _logger.info("Starting failover process");

        boolean failoverSucceeded = connection.attemptReconnection();

        if (!failoverSucceeded)
        {
            //Restore Existing State Manager
            _amqProtocolHandler.setStateManager(existingStateManager);
            connection.closed(new AMQDisconnectedException("Server closed connection and no failover " +
                    "was successful", null));
        }
        else
        {
            // Set the new Protocol Session in the StateManager.
            existingStateManager.setProtocolSession(_amqProtocolHandler.getProtocolSession());

            // Now that the ProtocolHandler has been reconnected clean up
            // the state of the old state manager. As if we simply reinstate
            // it any old exception that had occured prior to failover may
            // prohibit reconnection.
            // e.g. During testing when the broker is shutdown gracefully.
            // The broker
            // Clear any exceptions we gathered
            if (existingStateManager.getCurrentState() != AMQState.CONNECTION_OPEN)
            {
                // Clear the state of the previous state manager as it may
                // have received an exception
                existingStateManager.clearLastException();
                existingStateManager.changeState(AMQState.CONNECTION_OPEN);
            }


            //Restore Existing State Manager
            _amqProtocolHandler.setStateManager(existingStateManager);
            try
            {
                if (connection.firePreResubscribe())
                {
                    _logger.info("Resubscribing on new connection");
                    connection.resubscribeSessions();
                }
                else
                {
                    _logger.info("Client vetoed automatic resubscription");
                }

                connection.fireFailoverComplete();
                _amqProtocolHandler.setFailoverState(FailoverState.NOT_STARTED);
                _logger.info("Connection failover completed successfully");
            }
            catch (Exception e)
            {
                _logger.info("Failover process failed - exception being propagated by protocol handler");
                _amqProtocolHandler.setFailoverState(FailoverState.FAILED);
                _amqProtocolHandler.exception(e);
            }
        }
    }
}
