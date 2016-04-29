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
define(["dojo/_base/lang", "dojo/Deferred", "dojo/json"], function (lang, Deferred, json)
{

    var loadSaslClients = function loadSaslClients(availableMechanisms, management, onLastLoaded, errorHandler)
    {
        var mechanisms = lang.clone(availableMechanisms);
        var saslClients = [];

        var handleMechanisms = function handleMechanisms()
        {
            if (mechanisms.length == 0)
            {
                onLastLoaded(saslClients)
            }
            else
            {
                loadSaslClient();
            }
        }

        var loadSaslClient = function loadSaslClient()
        {
            var mechanism = mechanisms.shift();
            if (mechanism)
            {
                var url = "qpid/sasl/" + encodeURIComponent(mechanism.toLowerCase()) + "/SaslClient";
                management.get({
                                   url: "js/" + url + ".js",
                                   handleAs: "text",
                                   headers: {"Content-Type": "text/plain"}
                               })
                          .then(function (data)
                                {
                                    require([url], function (SaslClient)
                                    {
                                        try
                                        {
                                            var saslClient = new SaslClient();
                                            saslClients.push(saslClient);
                                        }
                                        catch (e)
                                        {
                                            console.error("Unexpected error on loading of mechanism " + mechanism + ": "
                                                          + json.stringify(e));
                                        }
                                        finally
                                        {
                                            handleMechanisms();
                                        }
                                    });
                                }, function (data)
                                {
                                    if (data.response.status != 404)
                                    {
                                        console.error("Unexpected error on loading mechanism " + mechanism + ": "
                                                      + json.stringify(data));
                                    }
                                    handleMechanisms();
                                });
            }
            else
            {
                handleMechanisms();
            }
        }

        handleMechanisms();
    }

    return {
        authenticate: function (mechanisms, management)
        {
            var deferred = new Deferred();
            var successCallback = function (data)
            {
                deferred.resolve(data);
            };
            var failureCallback = function (data)
            {
                deferred.reject(data);
            };
            loadSaslClients(mechanisms, management, function (saslClients)
            {
                if (saslClients.length > 0)
                {
                    saslClients.sort(function (c1, c2)
                                     {
                                         return c2.getPriority() - c1.getPriority();
                                     });
                    saslClients[0].authenticate(management).then(successCallback, failureCallback);
                }
                else
                {
                    failureCallback({
                                        message: "No SASL client available for " + data.mechanisms
                                    });
                }
            }, failureCallback);
            return deferred.promise;
        }
    };
});
