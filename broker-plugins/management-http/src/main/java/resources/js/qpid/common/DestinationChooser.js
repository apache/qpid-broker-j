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
        "dojo/promise/all",
        "dojo/dom-class",
        "dojo/store/Memory",
        "dojo/Deferred",
        "dijit/form/ComboBox",
        'qpid/common/util',
        "dojo/domReady!"],
    function (declare, lang, all, domClass, Memory, Deferred, ComboBox, util)
    {
        return declare("qpid.common.DestinationChooser", [ComboBox],
            {
                loadData: function (management, modelObj)
                {
                    var deferred = new Deferred();
                    domClass.add(this.domNode, "loading");
                    this.set("placeHolder", "    Loading...");
                    this.set("disabled", true);
                    var queuesQuery = management.query({
                        "category": "Queue",
                        "parent": modelObj,
                        "select": "name as id, name",
                        "orderBy": "name"
                    });
                    var exchangesQuery = management.query({
                        "category": "Exchange",
                        "parent": modelObj,
                        "select": "name as id, name",
                        "orderBy": "name"
                    });
                    all({
                        "queues": queuesQuery,
                        "exchanges": exchangesQuery
                    })
                        .then(lang.hitch(this,
                            function (data)
                            {
                                var items = [];
                                try
                                {
                                    items.push({"id" : "", "name": "-- Queues --"});
                                    items = items.concat(util.queryResultToObjects(data.queues));
                                    items.push({"id" : "", "name": "-- Exchanges --"});
                                    items = items.concat(util.queryResultToObjects(data.exchanges));
                                    var store = new Memory({data: items});
                                    this.set("store", store);
                                    this.set("disabled", false);
                                    domClass.remove(this.domNode, "loading");
                                    this.set("placeHolder", "alternate binding");
                                }
                                finally
                                {
                                    deferred.resolve(data);
                                }
                            }),
                            function (error)
                            {
                                deferred.reject(error);
                            }
                        );
                    return deferred.promise;
                },

                valueAsJson: function ()
                {
                    var destination = this.get("item") ? this.get("item").id : this.get("value");
                    if (destination)
                    {
                        destination = destination.replace(/^\s+|\s+$/gm, '');
                    }
                    if (destination)
                    {
                        return {"destination": destination};
                    }
                    else
                    {
                        return null;
                    }
                }
            });
    });
