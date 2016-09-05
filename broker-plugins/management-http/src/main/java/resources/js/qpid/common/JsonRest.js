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

define(["dojo/_base/lang", "dojo/json", "dojo/_base/declare", "dojo/store/util/QueryResults"],
    function (lang, json, declare, QueryResults)
    {
        return declare("qpid.common.JsonRest", null, {
            headers: {},
            idProperty: "id",
            firstProperty: "first",
            lastProperty: "last",
            accepts: "application/javascript, application/json",
            queryOperation: null,
            modelObject: null,
            management: null,
            queryParams: null,

            constructor: function (options)
            {
                this.headers = {};
                this.management = options.management;
                this.modelObject = options.modelObject;
                this.queryOperation = options.queryOperation;
                this.queryParams = options.queryParams;
                declare.safeMixin(this, options);
            },

            getIdentity: function (object)
            {
                return object[this.idProperty];
            },

            query: function (query, options)
            {
                query = lang.mixin(query || {}, this.queryParams);
                options = options || {};
                var headers = lang.mixin({Accept: this.accepts}, this.headers, options.headers);

                query[this.firstProperty] = options.start >= 0 ? options.start : -1;
                query[this.lastProperty] = options.count >= 0 && query.first >= 0 ? options.count + query.first : -1;

                if (options.start >= 0 || options.count >= 0)
                {
                    headers["X-Range"] =
                        "items=" + (options.start || '0') + '-' + (("count" in options && options.count != Infinity)
                            ? (options.count + (options.start || 0) - 1)
                            : '');
                    headers.Range = headers["X-Range"];
                }

                var modelObj = {
                    name: this.queryOperation,
                    parent: this.modelObject,
                    type: this.modelObject.type
                };
                var results = management.load(modelObj, query, {headers: headers});

                results.total = results.response.then(function (response)
                {
                    var range = response.getHeader("Content-Range");
                    if (!range)
                    {
                        range = response.getHeader("X-Content-Range");
                    }
                    return range && (range = range.match(/\/(.*)/)) && +range[1];
                });
                return QueryResults(results);
            }
        });

    });
