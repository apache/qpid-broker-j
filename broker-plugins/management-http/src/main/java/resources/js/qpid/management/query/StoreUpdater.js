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
        "dojo/json",
        "dojo/promise/all",
        "qpid/common/util"],
    function (declare, lang, json, all, util)
    {
        var createIdToIndexMap = function (results, idProperty)
        {
            var map = {};
            for (var i = 0; i < results.length; i++)
            {
                var id = results[i][idProperty];
                map[id] = i;
            }
            return map;
        };

        var updateIdToIndexMap = function (results, idProperty, idToIndexMap, startIndex)
        {
            for (var i = startIndex; i < results.length; i++)
            {
                var id = results[i][idProperty];
                idToIndexMap[id] = i;
            }
        };

        return declare(null,
            {
                /**
                 * fields set from constructor parameter object
                 */
                targetStore: null,

                /**
                 * internal fields
                 */
                _currentResults: null,
                _currentResultsIdToIndexMap: null,
                _updating: false,
                _fetchRangeCapturedArguments: null,
                _inProgressfetchRange: null,
                _inProgressfetch: null,

                fetch: function ()
                {
                    if (this._inProgressfetch === null || this._inProgressfetch === undefined)
                    {
                        this._inProgressfetch = this.inherited(arguments);
                    }
                    this._captureResults(queryResults, "fetch");
                    return queryResults;
                },
                fetchRange: function (args)
                {
                    if (this._inProgressfetchRange === null || this._inProgressfetchRange === undefined)
                    {
                        this._inProgressfetchRange = this.inherited(arguments);
                    }
                    var queryResults = this._inProgressfetchRange;

                    this._captureResults(queryResults, "fetchRange", args);
                    return queryResults;
                },
                update: function ()
                {
                    if (!this._updating)
                    {
                        this._updating = true;
                        return this.fetch();
                    }
                },
                updateRange: function ()
                {
                    var args = this._fetchRangeCapturedArguments;
                    if (!this._updating && args)
                    {
                        this._updating = true;
                        return this.fetchRange(args);
                    }
                },
                _captureResults: function (queryResults, methodName, args)
                {
                    var handler = lang.hitch(this, function (data)
                    {
                        try
                        {
                            this._processResults(data, methodName, args);
                        }
                        finally
                        {
                            this["_inProgress" + methodName ] = null;
                        }
                        if (!data.hasOwnProperty("results"))
                        {
                            this.emit("unexpected", data);
                        }
                    });
                    all({results: queryResults, totalLength: queryResults.totalLength})
                        .always(handler);
                },
                _processResults: function (data, methodName, args)
                {
                    var capturedArguments = args ? lang.clone(args) : {};
                    if (this._updating)
                    {
                        try
                        {
                            lang.mixin(capturedArguments, data);
                            this._detectChangesAndNotify(capturedArguments);
                        }
                        finally
                        {
                            this._updating = false;
                            this.emit("updateCompleted", capturedArguments);
                        }
                    }
                    else
                    {
                        var hasResults = data.hasOwnProperty("results");
                        this._currentResults = hasResults ? data.results.slice(0) : [];
                        this._currentResultsIdToIndexMap = hasResults ? createIdToIndexMap(this._currentResults, this.idProperty) : {};
                        this["_" + methodName + "CapturedArguments"] = capturedArguments;
                    }
                },
                _detectChangesAndNotify: function (data)
                {
                    var results = data.results;
                    var offset = data.start || 0;
                    if (results)
                    {
                        var newResults = results.slice(0);
                        var idProperty = this.idProperty;
                        var newResultsIdToIndexMap = createIdToIndexMap(newResults, idProperty);
                        var total = data.totalLength;
                        if (this._currentResults)
                        {
                            var currentResults = this._currentResults.slice(0);
                            for (var i = currentResults.length - 1; i >= 0; i--)
                            {
                                var currentResult = currentResults[i];
                                var currentId = currentResult[idProperty];
                                var newResultIndex = newResultsIdToIndexMap[currentId];
                                if (newResultIndex === undefined)
                                {
                                    this._pushChange("delete", currentResult, offset + i, offset + i, total);
                                    currentResults.splice(i, 1);
                                    updateIdToIndexMap(currentResults,
                                        idProperty,
                                        this._currentResultsIdToIndexMap,
                                        i);
                                    delete this._currentResultsIdToIndexMap[currentId];
                                }
                            }

                            for (var j = 0; j < newResults.length; j++)
                            {
                                var newResult = newResults[j];
                                var id = newResult[idProperty];
                                var previousIndex = this._currentResultsIdToIndexMap[id];

                                if (previousIndex === undefined)
                                {
                                    currentResults.splice(j, 0, newResult);
                                    updateIdToIndexMap(currentResults,
                                        idProperty,
                                        this._currentResultsIdToIndexMap,
                                        j);
                                    this._pushChange("add", newResult, offset + j, -1, total);
                                }
                                else
                                {
                                    var current = currentResults[previousIndex];
                                    if (previousIndex === j)
                                    {
                                        currentResults[j] = newResult;
                                        if (!util.equals(newResult, current))
                                        {
                                            this._pushChange("update", newResult, offset + j, previousIndex + offset, total);
                                        }
                                    }
                                    else
                                    {
                                        this._pushChange("update", newResult, offset + j, previousIndex + offset, total);
                                        currentResults.splice(previousIndex, 1);
                                        currentResults.splice(j, 0, current);
                                        updateIdToIndexMap(currentResults,
                                            idProperty,
                                            this._currentResultsIdToIndexMap,
                                            Math.min(previousIndex, j));
                                    }
                                }
                            }
                        }
                        else
                        {
                            for (var k = 0; k < newResults.length; k++)
                            {
                                this._pushChange("add", newResults[k], offset + k, -1,  total);
                            }
                        }

                        this._currentResults = newResults;
                        this._currentResultsIdToIndexMap = newResultsIdToIndexMap;
                    }
                },
                _pushChange: function (change, item, currentIndex, previousIndex, total)
                {
                    if (this.targetStore)
                    {
                        if (change === "update")
                        {
                            this.targetStore.put(item);
                        }
                        else if (change === "add")
                        {
                            this.targetStore.add(item);
                        }
                        else if (change === "delete")
                        {
                            this.targetStore.remove(item.id);
                        }
                        else
                        {
                            throw new Error("Change " + change + " is unknown by the store");
                        }
                    }
                    else
                    {
                        var event = {
                            "target": item,
                            "index": currentIndex,
                            "previousIndex": previousIndex
                        };
                        if (total >= 0)
                        {
                            event.totalLength = total;
                        }
                        this.emit(change, event);
                    }
                }
            });
    });