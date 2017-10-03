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

var ACKNOWLEDGE_MODE_SESSION_TRANSACTED = 0;
var ACKNOWLEDGE_MODE_AUTO_ACKNOWLEDGE = 1;
var ACKNOWLEDGE_MODE_CLIENT_ACKNOWLEDGE = 2;
var ACKNOWLEDGE_MODE_DUPS_OK_ACKNOWLEDGE = 3;
var DELIVERY_MODE_TRANSIENT = 1;
var DELIVERY_MODE_PERSISTENT = 2;

var messageSize = 1024;
var maximumDuration = 10000;
var numberOfParticipantPairs = 10;

function createProducerConnection(i, connectionFactory, destination, acknowledgeMode, deliveryMode)
{
    return {
        "_name": "producingConnection_" + i,
        "_factory": connectionFactory,
        "_sessions": [{
            "_sessionName": "producingSession_" + i,
            "_acknowledgeMode": acknowledgeMode,
            "_producers": [{
                "_name": "Producer_" + i,
                "_destinationName": destination,
                "_messageSize": messageSize,
                "_deliveryMode": deliveryMode,
                "_maximumDuration": maximumDuration
            }]
        }]
    };
}

function createConsumerConnection(i, connectionFactory, destination, acknowledgeMode)
{
    return {
        "_name": "consumingConnection_" + i,
        "_factory": connectionFactory,
        "_sessions": [{
            "_sessionName": "consumingSession_" + i,
            "_acknowledgeMode": acknowledgeMode,
            "_consumers": [{
                "_name": "Consumer_" + i,
                "_destinationName": destination,
                "_maximumDuration": maximumDuration
            }]
        }]
    };
}

function createTest(name, numberOfParticipantPairs, acknowledgeMode, deliveryMode, transport)
{
    var test = {
        "_name": name,
        "_queues": [],
        "_clients": []
    }

    var connectionFactory;
    if (transport.toLowerCase() == "plain")
    {
        connectionFactory = "connectionfactory";
    }
    else if (transport.toLowerCase() == "ssl")
    {
        connectionFactory = "sslconnectionfactory";
    }

    for (var i = 0; i < numberOfParticipantPairs; i++)
    {
        var queueName = "testQueue_" + i;
        var destination = queueName;
        test._queues.push({
            "_name": destination,
            "_durable": true
        });

        test._clients.push({
            "_name": "producingClient_" + i,
            "_connections": [createProducerConnection(i, connectionFactory, destination, acknowledgeMode, deliveryMode)]
        });
        test._clients.push({
            "_name": "consumingClient_" + i,
            "_connections": [createConsumerConnection(i, connectionFactory, destination, acknowledgeMode)]
        });
    }

    return test;
}

function createCompetingConsumerTest(name, numberOfParticipantPairs, transport)
{
    var destination = "testQueue_competing";
    var test = {
        "_name": name,
        "_queues": [{
            "_name": destination,
            "_durable": true
        }],
        "_clients": []
    };

    var connectionFactory;
    if (transport.toLowerCase() === "plain")
    {
        connectionFactory = "connectionfactory_noprefetch";
    }
    else if (transport.toLowerCase() === "ssl")
    {
        connectionFactory = "sslconnectionfactory_noprefetch";
    }

    test._clients.push({
        "_name": "producingClient",
        "_connections": [createProducerConnection(0, connectionFactory, destination, ACKNOWLEDGE_MODE_AUTO_ACKNOWLEDGE, DELIVERY_MODE_TRANSIENT)]
    });

    for (var i = 0; i < numberOfParticipantPairs; i++)
    {
        test._clients.push({
            "_name": "consumingClient_" + i,
            "_connections": [createConsumerConnection(i, connectionFactory, destination, ACKNOWLEDGE_MODE_SESSION_TRANSACTED)]
        });
    }

    return test;
}

var jsonObject = {
    _tests: [
        createTest("persistent_transaction_plain",
            numberOfParticipantPairs,
            ACKNOWLEDGE_MODE_SESSION_TRANSACTED,
            DELIVERY_MODE_PERSISTENT,
            "PLAIN"),
        createTest("transient_autoack_plain",
            numberOfParticipantPairs,
            ACKNOWLEDGE_MODE_AUTO_ACKNOWLEDGE,
            DELIVERY_MODE_TRANSIENT,
            "PLAIN"),
        createTest("persistent_transaction_ssl",
            numberOfParticipantPairs,
            ACKNOWLEDGE_MODE_SESSION_TRANSACTED,
            DELIVERY_MODE_PERSISTENT,
            "SSL"),
        createTest("transient_autoack_ssl",
            numberOfParticipantPairs,
            ACKNOWLEDGE_MODE_AUTO_ACKNOWLEDGE,
            DELIVERY_MODE_TRANSIENT,
            "SSL"),
        createTest("transient_transaction_plain",
            numberOfParticipantPairs,
            ACKNOWLEDGE_MODE_SESSION_TRANSACTED,
            DELIVERY_MODE_TRANSIENT,
            "PLAIN"),
        createTest("transient_transaction_ssl",
            numberOfParticipantPairs,
            ACKNOWLEDGE_MODE_SESSION_TRANSACTED,
            DELIVERY_MODE_TRANSIENT,
            "SSL"),
        createCompetingConsumerTest("competing_consumers_plain",
            30,
            "PLAIN")
    ]
};

