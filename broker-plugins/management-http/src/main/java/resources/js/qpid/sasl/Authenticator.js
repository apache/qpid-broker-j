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
define(["dojo/_base/declare",
        "dojo/_base/lang",
        "dojo/Deferred",
        "dojo/json",
        "dojo/promise/all"], function (declare, lang, Deferred, json, all)
{
    var absentSaslClient = {};
    return declare(null,
        {
            _management: null,
            _saslClient: null,
            _deferred: null,
            constructor: function (management)
            {
                this._management = management;
                this._deferred = new Deferred();
            },
            _successCallback: function (data)
            {
                this._deferred.resolve(data);
            },
            _failureCallback: function (data)
            {
                this._deferred.reject(data);
            },
            _chooseSaslClient: function (saslClients)
            {
                if (saslClients.length > 0)
                {
                    saslClients.sort(function (c1, c2)
                    {
                        if (c1 == null)
                        {
                            return 1;
                        }
                        if (c2 == null)
                        {
                            return -1;
                        }

                        return c2.getPriority() - c1.getPriority();
                    });
                    return saslClients[0];
                }
                return null;
            },
            _loadSaslClient: function (mechanism)
            {
                var deferred = new Deferred();
                var handle = require.on("error", lang.hitch(this, function (error)
                {
                    handle.remove();
                    deferred.resolve(null);
                    absentSaslClient[mechanism] = true;
                }));

                var url = "qpid/sasl/" + encodeURIComponent(mechanism.toLowerCase()) + "/SaslClient";
                require([url], function (SaslClient)
                {
                    try
                    {
                        var saslClient = new SaslClient();
                        deferred.resolve(saslClient);
                    }
                    catch (e)
                    {
                        deferred.resolve(null);
                        console.error("Unexpected error on loading of mechanism " + mechanism + ": " + e);
                    }
                    finally
                    {
                        handle.remove();
                    }
                });

                return deferred.promise;
            },
            _authenticate: function (serverMechanisms, credentials )
            {
                var mechanisms = lang.clone(serverMechanisms);

                var clients = [];

                var handle = lang.hitch(this, function ()
                {
                    if (mechanisms.length > 0)
                    {
                        var mechanism = mechanisms.shift();
                        var promise;
                        if (absentSaslClient[mechanism])
                        {
                            var deferred = new Deferred();
                            promise = deferred.promise;
                            deferred.resolve(null);
                        }
                        else
                        {
                            promise = this._loadSaslClient(mechanism);
                        }
                        promise.then(lang.hitch(this, function (client)
                        {
                            clients.push(client);
                            handle();
                        }));
                    }
                    else
                    {
                        var saslClient = this._chooseSaslClient(clients);

                        if (saslClient == null)
                        {
                            this._failureCallback(new Error("Authentication cannot be performed as none of"
                                                            + " the server's SASL mechanisms '"
                                                            + json.stringify(serverMechanisms)
                                                            + "' are supported"));
                        }
                        else
                        {
                            this._startAuthentication(saslClient, credentials);
                        }
                    }
                });
                handle();
            },
            _startAuthentication: function (saslClient, credentials)
            {
                this._saslClient = saslClient;
                this._saslClient.init(credentials)
                    .then(lang.hitch(this, function ()
                    {
                        var initialResponse;
                        try
                        {
                            initialResponse = saslClient.getInitialResponse();
                        }
                        catch (e)
                        {
                            this._failureCallback(e);
                            return;
                        }
                        var initialRequest = {
                            mechanism: saslClient.getMechanismName(),
                            response: initialResponse
                        };
                        this._management.sendSaslResponse(initialRequest)
                            .then(lang.hitch(this, this._handleChallengeOrOutcome),
                                lang.hitch(this, this._failureCallback));

                    }), lang.hitch(this, this._failureCallback));
            },
            _handleChallengeOrOutcome: function (serverResponse)
            {
                try
                {
                    if (serverResponse.challenge)
                    {
                        var response = this._saslClient.getResponse(serverResponse.challenge);
                        this._management.sendSaslResponse({
                            id: serverResponse.id,
                            response: response
                        })
                            .then(lang.hitch(this, this._handleChallengeOrOutcome),
                                lang.hitch(this, this._failureCallback));
                    }
                    else
                    {
                        if (serverResponse.additionalData)
                        {
                            this._saslClient.getResponse(serverResponse.additionalData);
                        }

                        if (this._saslClient.isComplete())
                        {
                            this._successCallback();
                        }
                        else
                        {
                            this._failureCallback(new Error("SASL negotiation has not been completed - cannot proceed"));
                        }
                    }
                }
                catch (e)
                {
                    this._failureCallback(e);
                }
            },
            authenticate: function (mechanisms, credentials)
            {
                this._authenticate(mechanisms, credentials);
                return this._deferred.promise;
            }
        }
    );
});
