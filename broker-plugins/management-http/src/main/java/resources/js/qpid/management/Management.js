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
        "dojo/_base/array",
        "dojo/request/xhr",
        "dojo/io-query",
        "dojo/json",
        "qpid/common/metadata",
        "qpid/common/timezone",
        "qpid/management/UserPreferences"],
  function (lang, array, xhr, ioQuery, json, Metadata, Timezone, UserPreferences)
  {

    function shallowCopy(source, target, excludes)
    {
        if (source)
        {
            for(var fieldName in source)
            {
                if(source.hasOwnProperty(fieldName))
                {
                    if (excludes && array.indexOf(excludes,fieldName) != -1)
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
    // This is a proxy for sending management requests to broker at given host and port specified in brokerURL argument.
    // Optional errorHandler method can be set and invoked on error responses when methods are invoked as non promise.

    function Management(brokerURL, errorHandler)
    {
        this.brokerURL = brokerURL;
        this.errorHandler = errorHandler || function(error){console.error(error);};
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
    //              the object send defining request query parameters
    //          handleAs: String
    //                Indicates how the response will be handled.
    //          url: String?
    //              relative URL to broker REST API
    // successAction: function
    //        Optional callback function to execute on successful response.
    //      It can be specified if function does not need to return promise
    // failureAction: function
    //        Optional callback function to execute on erroneous response.
    //      It can be specified if function does not need to return promise
    //
    // returns: promise
    //      Promise returned by dojo.request.xhr.
    Management.prototype.submit = function(request, successAction, failureAction)
    {
        var requestOptions = {
            sync: false,
            handleAs: "json",
            withCredentials: true,
            headers: { "Content-Type": "application/json"}
        };

        if (request)
        {
            shallowCopy(request, requestOptions, ["url"]);
            if (requestOptions.data && requestOptions.headers && requestOptions.headers["Content-Type"]
                && requestOptions.headers["Content-Type"] == "application/json"
                && typeof requestOptions.data != "string")
            {
                requestOptions.data = json.stringify(request.data );
            }
        }

        if (!requestOptions.method)
        {
            requestOptions.method = "GET";
        }

        var url = this.getFullUrl(request.url);
        var promise = xhr(url, requestOptions);
        if (successAction || failureAction)
        {
            var that = this;
            promise.then(
                function(data)
                {
                    if (successAction)
                    {
                        successAction(data);
                    }
                },
                function(error)
                {
                    if (failureAction)
                    {
                        failureAction(error);
                    }
                    else
                    {
                        that.errorHandler(error);
                    }
                }
            );
        }
        return promise;
    };

    Management.prototype.get = function(request, successAction, failureAction)
    {
        var requestOptions = merge(request, {method: "GET"});
        return this.submit(requestOptions, successAction, failureAction);
    };

    Management.prototype.post = function(request, data, successAction, failureAction)
    {
        var requestOptions = merge(request, {method: "POST", data: data});
        return this.submit(requestOptions, successAction, failureAction);
    };

    Management.prototype.del = function(request, successAction, failureAction)
    {
        var requestOptions = merge(request, {method: "DELETE"});
        return this.submit(requestOptions, successAction, failureAction);
    };

    // summary:
    //  Loads object data specified as modelObj argument
    //   modelObj: Object?
    //             is a JSON object specifying the hierarchy
    //            It has the following fields:
    //               name: String?
    //                     name of the object
    //               type: String?
    //                     category of the object
    //               parent: Object?
    //                     parent of the object in the same format, having fields name, type, parent
    //
    //   parameters: Object?
    //               is optional JSON to pass additional request parameters
    //   successAction: function
    //        Optional callback function to execute on successful response.
    //      It can be specified if function does not need to return promise
    //   failureAction: function
    //        Optional callback function to execute on erroneous response.
    //      It can be specified if function does not need to return promise
    //
    //   returns: promise
    //      Promise returned by dojo.request.xhr.
    Management.prototype.load = function(modelObj, parameters, successAction, failureAction)
    {
        var url = this.objectToURL(modelObj);
        var request = {url: url};
        if (parameters)
        {
            request.query = parameters;
        }
        return this.get(request, successAction, failureAction);
    };

    // summary:
    //  Creates object as specified in data parameter with given category and with given parent parentModelObject
    //   category: String?
    //             Object category
    //
    //   parentModelObject: Object?
    //              Parent object hierarchy
    //   data; Object?
    //              Object structure
    //   successAction: function
    //        Optional callback function to execute on successful response.
    //      It can be specified if function does not need to return promise
    //   failureAction: function
    //        Optional callback function to execute on erroneous response.
    //      It can be specified if function does not need to return promise
    //
    //   returns: promise
    //      Promise returned by dojo.request.xhr.
    Management.prototype.create = function(category, parentModelObject, data, successAction, failureAction)
    {
        var newObjectModel ={type: category.toLowerCase(), parent: parentModelObject};
        var url = this.objectToURL(newObjectModel);
        var request = {url: url};
        this.post(request, data, successAction, failureAction);
    };

    // summary:
    //  Updates object (which can be located using modelObj parameter) attributes to the values set in data parameter
    //
    //   modelObj: Object?
    //              Object specifying hierarchy
    //   data; Object?
    //              New attributes
    //   successAction: function
    //        Optional callback function to execute on successful response.
    //      It can be specified if function does not need to return promise
    //   failureAction: function
    //        Optional callback function to execute on erroneous response.
    //      It can be specified if function does not need to return promise
    //
    //   returns: promise
    //      Promise returned by dojo.request.xhr.
    Management.prototype.update = function(modelObj, data, successAction, failureAction)
    {
        var url = this.objectToURL(modelObj);
        var request = {url: url};
        return this.post(request, data, successAction, failureAction);
    };

    // summary:
    //  Removes object specified as modelObj argument
    //   modelObj: Object?
    //             hierarchy object
    //   parameters: Object?
    //               is optional JSON object to pass additional request parameters
    //   successAction: function
    //        Optional callback function to execute on successful response.
    //      It can be specified if function does not need to return promise
    //   failureAction: function
    //        Optional callback function to execute on erroneous response.
    //      It can be specified if function does not need to return promise
    //
    //   returns: promise
    //      Promise returned by dojo.request.xhr.
    Management.prototype.remove = function(modelObj, parameters, successAction, failureAction)
    {
        var url = this.objectToURL(modelObj);
        var request = {url: url};
        if (parameters)
        {
            request.query = parameters;
        }
        return this.del(request, successAction, failureAction);
    };

    // summary:
    //  Downloads current JSON for object specified as modelObj argument
    //   modelObj: Object?
    //             hierarchy object
    //   parameters: Object?
    //               is optional JSON object to pass additional request parameters
    Management.prototype.download = function(modelObj, parameters)
    {
        var url = this.buildObjectURL(modelObj, parameters);
        setTimeout(function() {window.location  = url;}, 100);
    }

    Management.prototype.downloadIntoFrame = function(modelObj, parameters)
    {
        var url = this.buildObjectURL(modelObj, parameters);
        var iframe = document.createElement('iframe');
        iframe.id = "downloader_" + modelObj.name;
        document.body.appendChild(iframe);
        iframe.src = url;
        // It seems there is no way to remove this iframe in a manner that is cross browser compatible.
    }

    Management.prototype.objectToURL = function(modelObj)
    {
        var url = null;
        if (modelObj.type == "broker")
        {
            url = "broker"
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
               if (url.substring(url.length - 1) != "/")
               {
                    url = url + "/";
               }
               url = url + encodeURIComponent(modelObj.name)
            }
        }
        return "api/latest/" + url;
    };

    Management.prototype.objectToPath = function(modelObj)
    {
        var path = "";
        var parent = modelObj.parent;
        while (parent && parent.type != "broker")
        {
            if (path)
            {
                path = encodeURIComponent(parent.name) + "/" + path;
            }
            else
            {
                path = encodeURIComponent(parent.name);
            }
            parent = parent.parent;
        }
        return path;
    };

    Management.prototype.buildObjectURL = function(modelObj, parameters)
    {
        var url = this.objectToURL(modelObj);
        if (parameters)
        {
            url = url + "?" + ioQuery.objectToQuery(parameters);
        }
        return this.getFullUrl(url);
    }

    Management.prototype.getFullUrl = function(url)
    {
        var baseUrl = this.brokerURL || "";
        if (baseUrl != "")
        {
            baseUrl = baseUrl + "/";
        }
        return baseUrl + url;
    }

    Management.prototype.init = function(callback)
    {
        var that = this;
        this.loadMetadata(function()
                          {
                            that.loadTimezones(function()
                                                {
                                                    that.loadUserPreferences(callback)
                                                });
                          });
    };

    Management.prototype.loadMetadata = function(callback)
    {
        var that = this;
        this.get({url: "service/metadata"},
                 function(data)
                 {
                    that.metadata = new Metadata(data);
                    if (callback)
                    {
                        callback();
                    }
                 },
                 this.errorHandler);
    };

    Management.prototype.loadTimezones = function(callback)
    {
        var that = this;
        that.get({url: "service/timezones"},
                 function(timezones)
                 {
                    that.timezone = new Timezone(timezones);
                    if (callback)
                    {
                        callback();
                    }
                 },
                 this.errorHandler);
    };

    Management.prototype.loadUserPreferences = function(callback)
    {
        this.userPreferences = new UserPreferences(this);
        this.userPreferences.load(callback);
    };

    return Management;
  });
