/*
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
 */

define(["dojo/_base/declare",
        "dojo/_base/lang",
        "dojox/encoding/base64",
        "dojo/json",
        "dojo/request/script",
        "dojo/promise/all",
        "dojox/uuid/generateRandomUuid",
        "dojo/Deferred",
        "qpid/sasl/CredentialBasedSaslClient"],
    function (declare, lang, base64, json, script, all, uuid, Deferred, SaslClient)
    {

        var toBase64 = function toBase64(input)
        {
            var result = [];
            for (var i = 0; i < input.length; i++)
            {
                result[i] = input.charCodeAt(i);
            }
            return base64.encode(result)
        };

        var fromBase64 = function fromBase64(input)
        {
            var decoded = base64.decode(input);
            var result = "";
            for (var i = 0; i < decoded.length; i++)
            {
                result += String.fromCharCode(decoded[i]);
            }
            return result;
        };

        var xor = function xor(lhs, rhs)
        {
            var words = [];
            for (var i = 0; i < lhs.words.length; i++)
            {
                words.push(lhs.words[i] ^ rhs.words[i]);
            }
            return CryptoJS.lib.WordArray.create(words);
        };

        var hasNonAscii = function hasNonAscii(name)
        {
            for (var i = 0; i < name.length; i++)
            {
                if (name.charCodeAt(i) > 127)
                {
                    return true;
                }
            }
            return false;
        };

        var generateSaltedPassword = function generateSaltedPassword(digest, salt, password, iterationCount)
        {
            var hmac = CryptoJS.algo.HMAC.create(CryptoJS.algo[digest], password);
            hmac.update(salt);
            hmac.update(CryptoJS.enc.Hex.parse("00000001"));
            var result = hmac.finalize();
            var previous = null;
            for (var i = 1; i < iterationCount; i++)
            {
                hmac = CryptoJS.algo.HMAC.create(CryptoJS.algo[digest], password);
                hmac.update(previous != null ? previous : result);
                previous = hmac.finalize();
                result = xor(result, previous);
            }
            return result;
        };

        var scriptLoadError = function scriptLoadError(error)
        {
            var message = "Cannot load script due to " + json.stringify(error);
            console.error(message);
            throw {message: message};
        };

        // hidden context scope variables
        var digest = null;
        var hmac = null;
        var gs2_header = "n,,";

        return declare("qpid.sasl.ShaSaslClient", [SaslClient], {
            _state: "initial",
            "-chains-": {
                constructor: "manual" // disable auto-constructor invocation
            },

            constructor: function (mechanism)
            {
                this._mechanism = mechanism;
            },
            init : function()
            {
                var superPromise = this.inherited(arguments);
                var deferred = new Deferred();
                var hmacDeferred = new Deferred();

                var shaName = this._mechanism.substring(6)
                    .replace('-', '')
                    .toLowerCase();
                digest = shaName.toUpperCase();
                hmac = "Hmac" + digest;

                // loading crypto-js functionality based on mechanism
                script.get("js/crypto-js/hmac-" + shaName + ".js")
                    .then(function ()
                    {
                        script.get("js/crypto-js/enc-base64-min.js")
                            .then(function ()
                            {
                                hmacDeferred.resolve(true);
                            }, function (error)
                            {
                                hmacDeferred.reject(error);
                                scriptLoadError(error);
                            });
                    }, function (error)
                    {
                        hmacDeferred.reject("error");
                        scriptLoadError(error);
                    });

                all([superPromise, hmacDeferred.promise])
                    .then(function ()
                    {
                        deferred.resolve();
                    }, function (error)
                    {
                        deferred.reject(error);
                    });
                return deferred.promise;

            },
            getMechanismName: function ()
            {
                return this._mechanism;
            },
            isComplete: function ()
            {
                return this._state == "completed";
            },
            getInitialResponse : function()
            {
                if (this._state != "initial")
                {
                    throw new Error("Unexpected state : " + this._state);
                }

                if (!hasNonAscii(this.username))
                {
                    var user = this.username;
                    user = user.replace(/=/g, "=3D");
                    user = user.replace(/,/g, "=2C");
                    this._clientNonce = uuid();
                    this._clientFirstMessageBare = "n=" + user + ",r=" + this._clientNonce;
                    this._state = "initiated";
                    return toBase64(gs2_header + this._clientFirstMessageBare);
                }
                else
                {
                    this._state = "error";
                    throw new Error("Username '" + this.username + "' is invalid");
                }
            },
            getResponse: function (challengeOrAdditionalData)
            {
                if (this._state == "initiated")
                {
                    var serverFirstMessage = fromBase64(challengeOrAdditionalData);
                    var parts = serverFirstMessage.split(",");
                    var nonce = parts[0].substring(2);
                    if (!nonce.substr(0, this._clientNonce.length) == this._clientNonce)
                    {
                        this._state = "error";
                        throw new Error("Authentication error - server nonce does not start with client nonce");
                    }
                    else
                    {
                        var salt = CryptoJS.enc.Base64.parse(parts[1].substring(2));
                        var iterationCount = parts[2].substring(2);
                        var saltedPassword = generateSaltedPassword(digest, salt, this.password, iterationCount);
                        var clientFinalMessageWithoutProof = "c=" + toBase64(gs2_header) + ",r=" + nonce;
                        var authMessage = this._clientFirstMessageBare + "," + serverFirstMessage + ","
                                          + clientFinalMessageWithoutProof;
                        var clientKey = CryptoJS[hmac]("Client Key", saltedPassword);
                        var storedKey = CryptoJS[digest](clientKey);
                        var clientSignature = CryptoJS[hmac](authMessage, storedKey);
                        var clientProof = xor(clientKey, clientSignature);
                        var serverKey = CryptoJS[hmac]("Server Key", saltedPassword);
                        this._serverSignature = CryptoJS[hmac](authMessage, serverKey);
                        var response = toBase64(clientFinalMessageWithoutProof + ",p="
                                                + clientProof.toString(CryptoJS.enc.Base64));
                        this._state = "generated";
                        return response;
                    }
                }
                else if (this._state == "generated")
                {
                    var serverFinalMessage = fromBase64(challengeOrAdditionalData);
                    if (this._serverSignature.toString(CryptoJS.enc.Base64) == serverFinalMessage.substring(2))
                    {
                        this._state = "completed";
                        return null;
                    }
                    else
                    {
                        this._state = "error";
                        throw new Error("Server signature does not match");
                    }
                }
                else
                {
                    throw new Error("Unexpected state '" + this._state + ". Cannot handle challenge!");
                }
            },
            toString: function ()
            {
                return "[SaslClient" + this.getMechanismName() + "]";
            }
        });

    });
