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
define(["dojo/_base/lang",
        "dojo/promise/all",
        "dojo/_base/array",
        "dojo/request/xhr",
        "dojo/io-query",
        "dojo/json",
        "dojo/promise/Promise",
        "dojo/Deferred",
        "qpid/sasl/Authenticator",
        "qpid/common/metadata",
        "qpid/common/timezone",
        "qpid/management/UserPreferences"],
    function (lang, all, array, xhr, ioQuery, json, Promise, Deferred, SaslAuthenticator, Metadata, Timezone, UserPreferences)
    {

        function shallowCopy(source, target, excludes)
        {
            if (source)
            {
                for (var fieldName in source)
                {
                    if (source.hasOwnProperty(fieldName))
                    {
                        if (excludes && array.indexOf(excludes, fieldName) != -1)
                        {
                            continue;
                        }

                        target[fieldName] = source[fieldName];
                    }
                }
            }
            return target;
        }

        function merge(data1, data2)
        {
            var result = {};
            shallowCopy(data1, result);
            shallowCopy(data2, result);
            return result;
        }

        // summary:
        // This is a facade for sending management requests to broker at given brokerURL specifying schema, host and port.
        // Optional errorHandler method can be set and invoked on error responses.
        function Management(brokerURL, errorHandler)
        {
            this.errorCallback = {};
            this.brokerURL = brokerURL;
            this.errorHandler = errorHandler || function (error)
                {
                    console.error(error);
                };
        }

        // summary:
        // Submits HTTP request to broker using dojo.request.xhr
        // request: Object?
        //        request object can have the same fields as dojo.request.xhr options and  additional 'url' field:
        //          data: String|Object|FormData?
        //                Data to transfer. This is ignored for GET and DELETE requests.
        //          headers: Object?
        //              Headers to use for the request.
        //          user: String?
        //                Username to use during the request.
        //          password: String?
        //                Password to use during the request.
        //          withCredentials: Boolean?
        //                For cross-site requests, whether to send credentials or not.
        //          query: Object?
        //              the query parameters to add to the request url
        //          handleAs: String
        //                Indicates how the response will be handled.
        //          url: String?
        //              relative URL to broker REST API
        //
        // returns: promise of type dojo.promise.Promise
        //      Promise returned by dojo.request.xhr with modified then method allowing to use default error handler if none is specified.
        Management.prototype.submit = function (request)
        {
            var requestOptions = {
                sync: false,
                handleAs: "json",
                withCredentials: true,
                headers: {"Content-Type": "application/json"}
            };

            if (request)
            {
                shallowCopy(request, requestOptions, ["url", "returnOriginalPromise"]);
                if (requestOptions.data && requestOptions.headers && requestOptions.headers["Content-Type"]
                    && requestOptions.headers["Content-Type"] == "application/json" && typeof requestOptions.data
                                                                                       != "string")
                {
                    requestOptions.data = json.stringify(request.data);
                }
            }

            if (!requestOptions.method)
            {
                requestOptions.method = "GET";
            }

            var url = this.getFullUrl(request.url);
            var promise = xhr(url, requestOptions);
            promise.otherwise(lang.hitch(this, function (error) {
                if (error)
                {
                    var status = error.status || (error.response ? error.response.status : null);
                    if (status)
                    {
                        var callbacks = this.errorCallback[status];
                        array.forEach(callbacks, function (callback) {
                            callback(error);
                        });
                    }
                }
            }));

            if ("returnOriginalPromise" in request && request.returnOriginalPromise === true)
            {
                return promise;
            }

            var errorHandler = this.errorHandler;

            // decorate promise in order to use a default error handler when 'then' method is invoked without providing error handler
            return lang.mixin(new Promise(), {
                then: function (callback, errback, progback)
                {
                    return promise.then(callback, errback || errorHandler, progback);
                },
                cancel: function (reason, strict)
                {
                    return promise.cancel(reason, strict);
                },
                isResolved: function ()
                {
                    return promise.isResolved();
                },
                isRejected: function ()
                {
                    return promise.isRejected();
                },
                isFulfilled: function ()
                {
                    return promise.isFulfilled();
                },
                isCanceled: function ()
                {
                    return promise.isCanceled();
                },
                always: function (callbackOrErrback)
                {
                    return promise.always(callbackOrErrback);
                },
                otherwise: function (errback)
                {
                    return promise.otherwise(errback);
                },
                trace: function ()
                {
                    return promise.trace();
                },
                traceRejected: function ()
                {
                    return promise.traceRejected();
                },
                toString: function ()
                {
                    return promise.toString();
                },
                response: promise.response
            });
        };

        Management.prototype.get = function (request)
        {
            var requestOptions = merge(request, {method: "GET"});
            return this.submit(requestOptions);
        };

        Management.prototype.post = function (request, data)
        {
            var requestOptions = merge(request, {
                method: "POST",
                data: data
            });
            return this.submit(requestOptions);
        };

        Management.prototype.put = function (request, data)
        {
            var requestOptions = merge(request, {
                method: "PUT",
                data: data
            });
            return this.submit(requestOptions);
        };

        Management.prototype.del = function (request)
        {
            var requestOptions = merge(request, {method: "DELETE"});
            return this.submit(requestOptions);
        };

        // summary:
        //  Loads object data specified as modelObj argument
        //   modelObj: Object?
        //             is a JSON object specifying the hierarchy
        //             It can have the following fields:
        //               name: String?
        //                     name of the object
        //               type: String?
        //                     category of the object
        //               parent: Object?
        //                     parent of the object in the same format, having fields name, type, parent
        //
        //   parameters: Object?
        //               is optional JSON to pass additional request parameters which will be added into query of REST url
        //
        // returns: promise of type dojo.promise.Promise
        //      Promise returned by dojo.request.xhr with modified then method allowing to use default error handler if none is specified.
        Management.prototype.load = function (modelObj, parameters, requestOptions)
        {
            var url = this.objectToURL(modelObj);
            var request = {url: url};

            if (requestOptions)
            {
                lang.mixin(request, requestOptions);
            }

            if (parameters)
            {
                request.query = parameters;
            }
            return this.get(request);
        };

        Management.prototype.invoke = function (modelObj, parameters, requestOptions)
        {
            var url = this.objectToURL(modelObj);
            var request = {url: url};

            if (requestOptions)
            {
                lang.mixin(request, requestOptions);
            }

            if (parameters)
            {
                request.query = parameters;
            }
            request.returnOriginalPromise = true;

            return this.get(request);
        };

        // summary:
        //  Creates object as specified in data parameter with given category and with given parent parentModelObject
        //   category: String?
        //             Object category
        //
        //   parentModelObject: Object?
        //              Parent object hierarchy
        //              It can have the following fields:
        //               name: String?
        //                     name of the object
        //               type: String?
        //                     category of the object
        //               parent: Object?
        //                     parent of the object in the same format, having fields name, type, parent
        //   data; Object?
        //              Object structure
        //
        // returns: promise of type dojo.promise.Promise
        //      Promise returned by dojo.request.xhr with modified then method allowing to use default error handler if none is specified.
        Management.prototype.create = function (category, parentModelObject, data)
        {
            var newObjectModel = {
                type: category.toLowerCase(),
                parent: parentModelObject
            };
            var url = this.objectToURL(newObjectModel);
            var request = {url: url};
            return this.post(request, data);
        };

        // summary:
        //  Updates object (which can be located using modelObj parameter) attributes to the values set in data parameter
        //
        //   modelObj: Object?
        //              Object specifying hierarchy
        //              It can have the following fields:
        //               name: String?
        //                     name of the object
        //               type: String?
        //                     category of the object
        //               parent: Object?
        //                     parent of the object in the same format, having fields name, type, parent
        //   data; Object?
        //              New attributes
        //
        // returns: promise of type dojo.promise.Promise
        //      Promise returned by dojo.request.xhr with modified then method allowing to use default error handler if none is specified.
        Management.prototype.update = function (modelObj, data)
        {
            var url = this.objectToURL(modelObj);
            var request = {url: url};
            return this.post(request, data);
        };

        // summary:
        //  Removes object specified as modelObj argument
        //   modelObj: Object?
        //             hierarchy object
        //             It can have the following fields:
        //               name: String?
        //                     name of the object
        //               type: String?
        //                     category of the object
        //               parent: Object?
        //                     parent of the object in the same format, having fields name, type, parent
        //   parameters: Object?
        //               is optional JSON object to pass additional request parameters which will be added into query of REST url
        //
        // returns: promise of type dojo.promise.Promise
        //      Promise returned by dojo.request.xhr with modified then method allowing to use default error handler if none is specified.
        Management.prototype.remove = function (modelObj, parameters)
        {
            var url = this.objectToURL(modelObj);
            var request = {url: url};
            if (parameters)
            {
                request.query = parameters;
            }
            return this.del(request);
        };

        // summary:
        //  Downloads current JSON for object specified as modelObj argument
        //
        //   modelObj: Object?
        //             hierarchy object
        //             It can have the following fields:
        //               name: String?
        //                     name of the object
        //               type: String?
        //                     category of the object
        //               parent: Object?
        //                     parent of the object in the same format, having fields name, type, parent
        //   parameters: Object?
        //               is optional JSON object to pass additional request parameters  which will be added into query of REST url
        //   operation: String?
        //               is optional String identifying the managed operation to be called on the object
        //
        Management.prototype.download = function (modelObj, parameters, operation)
        {
            var url = this.buildObjectURL(modelObj, parameters, operation);
            setTimeout(function ()
            {
                window.location = url;
            }, 100);
        };

        // summary:
        //  Downloads current JSON for object specified as modelObj argument into iframe
        Management.prototype.downloadIntoFrame = function (modelObj, parameters, operation)
        {
            var url = this.buildObjectURL(modelObj, parameters, operation);
            this.downloadUrlIntoFrame(url, "downloader_" + modelObj.name)
        };

        // summary:
        //  Downloads given URL into a iframe with given id
        Management.prototype.downloadUrlIntoFrame = function (url, frameId)
        {
            var iframe = document.createElement('iframe');
            iframe.id = frameId;
            iframe.style.display = "none";
            document.body.appendChild(iframe);
            iframe.src = url;
            // It seems there is no way to remove this iframe in a manner that is cross browser compatible.
        };

        // summary:
        //  Builds relative REST url (excluding schema, host and port) for the object representing CO hierarchy
        //   modelObj: Object?
        //             hierarchy object
        //             It can have the following fields:
        //               name: String?
        //                     name of the object
        //               type: String?
        //                     category of the object
        //               parent: Object?
        //                     parent of the object in the same format, having fields name, type, parent
        //   parameters: Object?
        //             is optional JSON object to pass additional request parameters  which will be added into query of REST url
        //
        // returns: relative REST url for the hierarchy object
        //
        Management.prototype.objectToURL = function (modelObj)
        {
            var url = null;
            if (modelObj.type === "broker")
            {
                url = "broker";
            }
            else
            {
                url = encodeURIComponent(modelObj.type);
                var parentPath = this.objectToPath(modelObj);
                if (parentPath)
                {
                    url = url + "/" + parentPath;
                }

                if (modelObj.name)
                {
                    if (url.substring(url.length - 1) !== "/")
                    {
                        url = url + "/";
                    }
                    // Double encode the object name in case it contains slashes
                    url = url + encodeURIComponent(encodeURIComponent(modelObj.name))
                }
            }
            return "api/latest/" + url;
        };

        // summary:
        //  Builds a servlet path of REST url for the object representing CO hierarchy
        //   modelObj: Object?
        //             hierarchy object
        //             It can have the following fields:
        //               name: String?
        //                     name of the object
        //               type: String?
        //                     category of the object
        //               parent: Object?
        //                     parent of the object in the same format, having fields name, type, parent
        //   parameters: Object?
        //             is optional JSON object to pass additional request parameters  which will be added into query of REST url
        //
        // returns: relative REST servlet path for the hierarchy object
        //
        Management.prototype.objectToPath = function (modelObj)
        {
            var path = "";
            var parent = modelObj.parent;
            while (parent && parent.type != "broker")
            {
                if (path)
                {
                    path = encodeURIComponent(encodeURIComponent(parent.name)) + "/" + path;
                }
                else
                {
                    path = encodeURIComponent(encodeURIComponent(parent.name));
                }
                parent = parent.parent;
            }
            return path;
        };

        // summary:
        //  Builds full REST url for the object representing CO hierarchy
        //   modelObj: Object?
        //             hierarchy object
        //             It can have the following fields:
        //               name: String?
        //                     name of the object
        //               type: String?
        //                     category of the object
        //               parent: Object?
        //                     parent of the object in the same format, having fields name, type, parent
        //   parameters: Object?
        //             is optional JSON object to pass additional request parameters  which will be added into query of REST url
        //   operation: String?
        //               is optional String identifying the managed operation to be called on the object
        //
        // returns: full REST url for the hierarchy object
        //
        Management.prototype.buildObjectURL = function (modelObj, parameters, operation)
        {
            var url = this.objectToURL(modelObj);
            if(operation)
            {
                url = url + "/" + operation;
            }
            if (parameters)
            {
                url = url + "?" + ioQuery.objectToQuery(parameters);
            }
            return this.getFullUrl(url);
        };

        // summary:
        //  Returns full REST url for the relative REST url
        //
        // returns: full urk for the given relative URL
        //
        Management.prototype.getFullUrl = function (url)
        {
            var baseUrl = this.brokerURL || "";
            if (baseUrl != "")
            {
                baseUrl = baseUrl + "/";
            }
            return baseUrl + url;
        };

        // summary:
        //  Loads meta data, time zones, user preferences and invokes callback functions after loading user preferences
        //
        Management.prototype.init = function (callback)
        {
            var userAndGroupsDetails = this.getAuthenticatedUserAndGroups();
            var metadata = this.loadMetadata();
            var timezones = this.loadTimezones();
            var userPreferences = this.loadUserPreferences();
            all([userAndGroupsDetails, metadata, timezones, userPreferences]).then(callback, callback);
        };

        Management.prototype.getAuthenticatedUserAndGroups = function ()
        {
            var baseUrl = this.objectToURL({type : "broker"});

            var userPromise = this.get({url: baseUrl + "/getUser"});
            var groupsPromise = this.get({url: baseUrl + "/getGroups"});

            var complete = function(result)
            {
                if (result)
                {
                    this.user = result.user;
                    this.groups = result.groups;
                }
            };

            return all({
                user: userPromise,
                groups: groupsPromise
            }).then(lang.hitch(this, complete), lang.hitch(this, this.errorHandler));

        };

        // summary:
        //  Loads meta data and store it under 'metadata' field as object of type qpid.common.metadata object.
        //
        Management.prototype.loadMetadata = function ()
        {
            return this.get({url: "service/metadata"})
                .then(lang.hitch(this, function (data)
                {
                    this.metadata = new Metadata(data);
                }));
        };

        // summary:
        //  Loads timezones and store them under 'timezone' field as  object of type qpid.common.timezone object
        //
        Management.prototype.loadTimezones = function ()
        {
            return this.get({url: "service/timezones"})
                .then(lang.hitch(this, function (timezones)
                {
                    this.timezone = new Timezone(timezones);
                }));
        };

        // summary:
        //  Loads user preferences and store them under 'userPreferences' field as object of type qpid.management.UserPreferences
        //  Returns promise
        //
        Management.prototype.loadUserPreferences = function ()
        {
            this.userPreferences = new UserPreferences(this);
            return this.userPreferences.load();
        };

        var saslServiceUrl = "service/sasl";

        Management.prototype.getSaslStatus = function ()
        {
            return this.get({url: saslServiceUrl});
        };

        Management.prototype.sendSaslResponse = function (response)
        {
            return this.submit({
                url: saslServiceUrl,
                data: response,
                headers: {},
                method: "POST"
            });
        };

        Management.prototype.authenticate = function (mechanisms, credentials)
        {
            return new SaslAuthenticator(this).authenticate(mechanisms, credentials);
        };

        // summary:
        //   Sends query request
        //   query: query Object?
        //             is a JSON object specifying the hierarchy
        //             It can have the following fields:
        //               where: String?
        //                     sql like expression containing 'where' conditions
        //               select: String?
        //                     coma separated list of fields to select
        //               category: String?
        //                     category of the object
        //               parent: Object?
        //                     hierarchy object defining query scope,
        //                     if not set queries will be executed against broker.
        //                     Virtual host query scope can be defined by setting parent
        //                     into corresponding object representing a virtual host hierarchy
        //
        // returns: promise of type dojo.promise.Promise
        //      Promise returned by dojo.request.xhr with modified then method allowing to use default error handler if none is specified.
        Management.prototype.query = function (query)
        {
            var request = {
                url: this.getQueryUrl(query),
                query: {}
            };
            shallowCopy(query, request.query, ["parent", "category"]);
            request.returnOriginalPromise = true;
            return this.get(request);
        };

        Management.prototype.getQueryUrl = function (query, parameters)
        {
            var url = "api/latest/" + (query.parent && query.parent.type === "virtualhost" ? "queryvhost/"
                      + this.objectToPath({parent: query.parent}) : "querybroker") + (query.category ? "/"
                      + query.category : "");
            if (parameters)
            {
                url = url + "?" + ioQuery.objectToQuery(parameters);
            }
           return this.getFullUrl(url);
        };

        Management.prototype.savePreference = function(parentObject, preference)
        {
           var url =  this.buildPreferenceUrl(parentObject, preference.type);
           return this.post({url: url}, [preference]);
        };

        Management.prototype.savePreferences = function(parentObject, preferences)
        {
            var url =  this.buildPreferenceUrl(parentObject);
            return this.post({url: url}, preferences);
        };

        Management.prototype.getPreference = function(parentObject, type, name)
        {
           var url =  this.buildPreferenceUrl(parentObject, type, name);
           return this.get({url: url});
        };

        Management.prototype.getPreferenceById = function(parentObject, preferenceId)
        {
            var url =  this.buildPreferenceUrl(parentObject, null, null, true, preferenceId);
            return this.get({url: url});
        };

        Management.prototype.getUserPreferences = function(parentObject, type)
        {
            var url =  this.buildPreferenceUrl(parentObject, type);
            return this.get({url: url});
        };

        Management.prototype.getVisiblePreferences = function(parentObject, type)
        {
            var url =  this.buildPreferenceUrl(parentObject, type, null, true);
            return this.get({url: url});
        };

        Management.prototype.deletePreference = function(parentObject, type, name)
        {
           var url =  this.buildPreferenceUrl(parentObject, type, name);
           return this.del({url: url});
        };

        Management.prototype.buildPreferenceUrl = function (parentObject, type, name, visiblePreferences, id)
        {
            var url = this.objectToURL(parentObject);
            url = url + (visiblePreferences ? "/visiblepreferences" : "/userpreferences");
            if (type)
            {
                url = url + "/" + encodeURIComponent(encodeURIComponent(type));
                if (name)
                {
                    url = url + "/" + encodeURIComponent(encodeURIComponent(name));
                }
            }
            else if (id)
            {
                url = url + "?id=" + encodeURIComponent(encodeURIComponent(id));
            }
            return url;
        };

        Management.prototype.getGroups = function ()
        {
            return lang.clone(this.groups);
        };

        Management.prototype.getAuthenticatedUser = function ()
        {
            return this.user;
        };

        Management.prototype.addErrorCallback = function(status, f)
        {
            if (!(status in this.errorCallback))
            {
                this.errorCallback[status] = [];
            }
            this.errorCallback[status].push(f);
        };

        return Management;
    });
