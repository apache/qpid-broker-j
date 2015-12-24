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
define(["dojo/_base/lang"], function (lang)
{
    var saslServiceUrl = "service/sasl";
    var errorHandler = function errorHandler(error)
    {
        if (error.response)
        {
            if(error.response.status == 401)
            {
                alert("Authentication Failed");
            }
            else if(error.response.status == 403)
            {
                alert("Authorization Failed");
            }
            else
            {
                alert(error.message);
            }
        }
        else
        {
            alert(error.message ? error.message : error);
        }
    }

    var authenticate = function (management, saslClient, data, authenticationSuccessCallback, authenticationFailureCallback)
    {
        var response = null;
        try
        {
            response = saslClient.getResponse(data);
        }
        catch(e)
        {
            authenticationFailureCallback(e);
            return;
        }

        if (saslClient.isComplete())
        {
            authenticationSuccessCallback();
        }
        else
        {
            management.submit({
                                  url: saslServiceUrl,
                                  data: response,
                                  headers: {},
                                  method: "POST"
                              }).then(function (challenge)
                                      {
                                        authenticate(management,
                                                     saslClient,
                                                     challenge,
                                                     authenticationSuccessCallback,
                                                     authenticationFailureCallback);
                                      },
                                      authenticationFailureCallback);
        }
    }

    var loadSaslClients = function loadSaslClients(management, availableMechanisms, saslClients, onLastLoaded)
    {
        var mechanisms = lang.clone(availableMechanisms);
        var handleMechanisms = function handleMechanisms()
        {
            if (mechanisms.length == 0)
            {
                onLastLoaded(saslClients)
            }
            else
            {
                loadSaslClients(management, mechanisms, saslClients, onLastLoaded);
            }
        }

        var mechanism = mechanisms.shift();
        if (mechanism)
        {
          var url = "qpid/sasl/" + encodeURIComponent(mechanism.toLowerCase()) + "/SaslClient";
          management.get({url:"js/" + url + ".js",
                          handleAs: "text",
                          headers: { "Content-Type": "text/plain"}})
                    .then(function(data)
                          {
                              require([url],
                                      function(SaslClient)
                                      {
                                          try
                                          {
                                              var saslClient = new SaslClient();
                                              saslClient.initialized().then(function()
                                                                            {
                                                                                saslClients.push(saslClient);
                                                                                handleMechanisms();
                                                                            },
                                                                            function(e)
                                                                            {
                                                                                errorHandler("Unexpected error on " +
                                                                                             "loading of mechanism " +
                                                                                             mechanism + ": ", e);
                                                                                handleMechanisms();
                                                                            }
                                                                           );

                                          }
                                          catch(e)
                                          {
                                              errorHandler("Unexpected error on loading of mechanism " + mechanism +
                                                           ": ", e);
                                              handleMechanisms();
                                          }
                                      });
                          },
                          function(data)
                          {
                              if (data.response.status != 404 )
                              {
                                  errorHandler("Unexpected error on loading mechanism " + mechanism + ": ", data);
                              }
                              handleMechanisms();
                          }
                    );
        }
        else
        {
            handleMechanisms();
        }
    }

    return {
              authenticate:   function(management, authenticationSuccessCallback)
                              {
                                  management.get({url: saslServiceUrl})
                                            .then(function(data)
                                                  {
                                                     var saslClients = [];
                                                     loadSaslClients(management,
                                                                     data.mechanisms,
                                                                     saslClients,
                                                                     function (saslClients)
                                                                     {
                                                                        saslClients.sort(function(c1, c2)
                                                                                         {
                                                                                           return c2.getPriority() -
                                                                                                  c1.getPriority();
                                                                                         });
                                                                        if (saslClients.length > 0)
                                                                        {
                                                                          var saslClient = saslClients[0];
                                                                          dojo.when(saslClient.getCredentials())
                                                                              .then(function(data)
                                                                                    {
                                                                                        authenticate(management,
                                                                                                     saslClient,
                                                                                                     data,
                                                                                                     authenticationSuccessCallback,
                                                                                                     errorHandler);
                                                                                    },
                                                                                    errorHandler);
                                                                        }
                                                                        else
                                                                        {
                                                                          errorHandler("No SASL client available for " +
                                                                                       data.mechanisms);
                                                                        }
                                                                     });
                                                  },
                                                  errorHandler);
                              },
              getUser:        function(management, authenticationSuccessCallback)
                              {
                                  management.get({url: saslServiceUrl})
                                            .then(authenticationSuccessCallback,
                                                  errorHandler);
                              }
           };
});
