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

define(['dojo/_base/lang',
        'dojo/_base/declare',
        "dojo/Evented",
        "dojo/json",
        'dstore/Store',
        'dstore/QueryResults',
        "dojo/Deferred"], function (lang, declare, Evented, json, Store, QueryResults, Deferred)
{

    return declare("qpid.management.query.QueryStore", [Store, Evented], {

        transformer: null,
        management: null,
        selectClause: null,
        where: null,
        category: null,
        parentObject: null,
        useCachedResults: false,
        _lastResponsePromise: null,

        fetch: function (kwArgs)
        {
            return this._request(kwArgs);
        },

        fetchRange: function (kwArgs)
        {
            return this._request(kwArgs);
        },

        _request: function (kwArgs)
        {
            if (this.useCachedResults && this._lastResponsePromise)
            {
                return this._createQueryResults(this._lastResponsePromise);
            }

            if (!this.selectClause)
            {
                var responseDeferred = new Deferred();
                responseDeferred.resolve({
                    headers: [],
                    results: [],
                    total: 0
                });
                this._lastResponsePromise = responseDeferred.promise;
            }
            else
            {
                var queryRequest = {
                    category: this.category,
                    select: this.selectClause
                };

                if (this.parentObject)
                {
                    queryRequest.parent = this.parentObject;
                }

                if (this.where)
                {
                    queryRequest.where = this.where;
                }

                if ("start" in kwArgs)
                {
                    queryRequest.offset = kwArgs.start;
                }

                if ("end" in kwArgs)
                {
                    queryRequest.limit = kwArgs.end - (queryRequest.offset ? queryRequest.offset : 0);
                }

                if (this.orderBy)
                {
                    queryRequest.orderBy = this.orderBy;
                }

                this._lastResponsePromise = this.management.query(queryRequest);
            }
            this._lastResponsePromise.then(lang.hitch(this, function (data)
            {
                this.emit("queryCompleted", data);
            }), lang.hitch(this, function (error)
            {
                this.emit("queryCompleted",
                    {
                        headers: [],
                        results: [],
                        total: 0
                    });
            }));
            return this._createQueryResults(this._lastResponsePromise);
        },

        _createQueryResults: function (responsePromise)
        {
            var that = this;
            return new QueryResults(responsePromise.then(function (data)
            {
                if (that.transformer)
                {
                    return that.transformer(data);
                }
                else
                {
                    return data.results;
                }
            }, function (error)
            {
                if (error && (!error.hasOwnProperty("response") || error.response.hasOwnProperty("status")))
                {
                    this.management.errorHandler(error);
                }
                return [];
            }), {
                totalLength: responsePromise.then(function (data)
                {
                    return data.total;
                }, function (error)
                {
                    return 0;
                })
            });
        },

        // override from dstore.Store to not copy collection
        _createSubCollection: function ()
        {
            return this;
        }
    });
});
