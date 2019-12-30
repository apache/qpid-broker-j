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
    'dstore/Store',
    'dstore/QueryResults'
], function (lang, declare, Store, QueryResults) {

    return declare("qpid.management.queue.MessageStore", [Store], {

        modelObject: null,
        management: null,
        operationName: "getMessageInfo",
        operationArguments: {includeHeaders: false},
        totalLength: null,

        fetch: function () {
            return this._request();
        },

        fetchRange: function (kwArgs) {
            return this._request(kwArgs);
        },

        _request: function (kwArgs) {
            var modelObj = {
                name: this.operationName,
                parent: this.modelObject,
                type: this.modelObject.type
            };
            var query = lang.clone(this.operationArguments);
            if (kwArgs && kwArgs.hasOwnProperty("start"))
            {
                query.first = kwArgs.start;
            }
            if (kwArgs && kwArgs.hasOwnProperty("end"))
            {
                query.last = kwArgs.end - 1;
            }
            var headers = lang.mixin({Accept: "application/javascript, application/json"}, kwArgs.headers);
            var messages = this.management.invoke(modelObj, query, {headers: headers}, true);
            var depth = this._getQueueDepth();
            return new QueryResults(messages, {totalLength: depth});
        },

        _getQueueDepth: function () {
            var modelObj = {
                name: "getStatistics",
                parent: this.modelObject,
                type: this.modelObject.type
            };
            return this.management.load(modelObj, {statistics: ["queueDepthMessages"]})
                .then(function (data) {
                    return data["queueDepthMessages"];
                }, function (error) {
                    return 0;
                });
        }

    });
});
