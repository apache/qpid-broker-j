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

define([
	'dojo/_base/lang',
	'dojo/_base/declare',
    "dojo/Evented",
    "dojo/json",
    'dstore/Store',
    'dstore/QueryResults',
    "dojo/Deferred"
], function (lang, declare, Evented, json, Store, QueryResults,Deferred) {

    return declare("qpid.management.query.QueryStore", [Store, Evented], {

        management: null,
        selectClause: null,
        where: null,
        category: null,
        parent: null,
        useCachedResults: false,
        zeroBased: true,
        _lastHeaders: [],
        _lastResponsePromise: null,

        fetch: function (kwArgs) {
            return this._request(kwArgs);
        },

        fetchRange: function (kwArgs) {
            return this._request(kwArgs);
        },

        _request: function (kwArgs) {


            if (!this.selectClause) {
                this._emitChangeHeadersIfNecessary([]);
                var deferred = new Deferred();
                deferred.resolve([]);
                return new QueryResults(deferred.promise);
            }

            var queryRequest = {
                category: this.category,
                select: this.selectClause ? this.selectClause + ",id" : "id"
            };

            if (this.parent) {
                queryRequest.parent = this.parent;
            }

            if (this.where) {
                queryRequest.where = this.where;
            }

            if ("start" in kwArgs) {
                queryRequest.offset = kwArgs.start;
            }

            if ("end" in kwArgs) {
                queryRequest.limit = kwArgs.end - (queryRequest.offset ? queryRequest.offset : 0);
            }

            if (this.orderBy) {
                queryRequest.orderBy = this.orderBy;
            }

            if (this.useCachedResults) {
                return this._createQueryResults(this._lastResponsePromise);
            }

            var responsePromise = this.management.query(queryRequest);
            responsePromise.then(lang.hitch(this, function(data) {
                var headers = lang.clone(data.headers);
                headers.pop();
                this._emitChangeHeadersIfNecessary(headers);
            }), lang.hitch(this, function(error) {
                this._emitChangeHeadersIfNecessary([]);
            }));

            this._lastResponsePromise = responsePromise;
            return this._createQueryResults(this._lastResponsePromise);
        },

        _createQueryResults: function(responsePromise) {
            var that = this;
            var queryResultData = {
                data: responsePromise.then(function (data) {
                    var dataResults = data.results;
                    var results = [];
                    for (var i = 0, l = dataResults.length; i < l; ++i) {
                        var result = dataResults[i];
                        var item = {id: result[result.length - 1]};

                        // excluding id, as we already added id field
                        for(var j = 0, rl = result.length - 1; j < rl ; ++j ){
                            // sql uses 1-based index in ORDER BY
                            var field = this.zeroBased ? j : j + 1;
                            item[new String(field)] = result[j];
                        }
                        results.push(item);
                    }
                    return results;
                }, function(error) {
                    this.management.errorHandler(error);
                    return [];
                }),
                total: responsePromise.then(function (data) {
                    return data.total;
                }, function(error) {
                    return 0;
                })
            };
            return new QueryResults(queryResultData.data, {
                totalLength: queryResultData.total
            });
        },

        _emitChangeHeadersIfNecessary: function (headers) {
            if (!this._equalStringArrays(headers, this._lastHeaders)) {
                this._lastHeaders = headers;
                this.emit("changeHeaders", {headers: headers});
            }
        },

        // override from dstore.Store to not copy collection
        _createSubCollection: function() {
            return this;
        },

        _equalStringArrays: function(a, b) {
            if (a.length != b.length) {
                return false;
            }
            for (var i = 0; i < a.length; ++i) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        }
    });
});
