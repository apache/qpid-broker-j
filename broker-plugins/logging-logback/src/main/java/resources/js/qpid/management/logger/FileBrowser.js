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
define(["qpid/common/util",
        "dojo/query",
        "dojo/number",
        "dojo/_base/lang",
        "dojo/_base/connect",
        "dojox/html/entities",
        "dojo/text!logger/file/fileBrowser.html",
        "dojox/grid/EnhancedGrid",
        "qpid/common/UpdatableStore",
        "dijit/registry",
        "dojo/domReady!"],
    function (util, query, number, lang, connect, entities, template, EnhancedGrid, UpdatableStore, registry)
    {
        function FileBrowser(params)
        {
            var that = this;
            this.management = params.management;
            this.modelObj = params.modelObj;
            util.parse(params.containerNode, template, function ()
            {
                that.postParse(params);
            });
        }

        FileBrowser.prototype.postParse = function (params)
        {
            var that = this;
            var gridProperties = {
                height: 400,
                selectionMode: "extended",
                plugins: {
                    indirectSelection: true,
                    pagination: {
                        pageSizes: [10, 25, 50, 100],
                        description: true,
                        sizeSwitch: true,
                        pageStepper: true,
                        gotoButton: true,
                        maxPageStep: 4,
                        position: "bottom"
                    }
                }
            };

            this.downloadButton = registry.byNode(query(".downloadButton", params.containerNode)[0]);
            this.downloadButton.on("click", function (e)
            {
                that.downloadSelectedFiles()
            });

            this.downloadAllButton = registry.byNode(query(".downloadAllButton", params.containerNode)[0]);
            this.downloadAllButton.on("click", function (e)
            {
                that.downloadAllFiles()
            });

            this.logFiles = this.addIdToFileObjects(params.data);
            this.logFileGrid = new UpdatableStore(this.logFiles, query(".logFilesGrid", params.containerNode)[0], [{
                name: "Name",
                field: "name",
                width: "40%"
            }, {
                name: "Size",
                field: "size",
                width: "20%",
                formatter: function (val)
                {
                    return val > 1024 ? (val > 1048576 ? number.round(val / 1048576) + " MB" : number.round(val / 1024)
                                                                                               + " KB") : val + " B";
                }
            }, {
                name: "Last Modified",
                field: "lastModified",
                width: "40%",
                formatter: function (val)
                {
                    return that.management.userPreferences.formatDateTime(val, {
                        addOffset: true,
                        appendTimeZone: true
                    });
                }
            }], function (obj)
            {
                obj.grid.on("rowDblClick", function (evt)
                {
                    var idx = evt.rowIndex;
                    var theItem = this.getItem(idx);
                    that.download(theItem);
                });
            }, gridProperties, EnhancedGrid);
        }

        FileBrowser.prototype.download = function (item)
        {
            var parentModelObj = this.modelObj;
            var modelObj = {
                type: parentModelObj.type,
                name: "getFile",
                parent: parentModelObj
            }
            this.management.download(modelObj, {fileName: item.name});
        }

        FileBrowser.prototype.addIdToFileObjects = function (data)
        {
            var fileItems = [];
            var logFiles = data.logFiles;
            for (var idx in logFiles)
            {
                var item = lang.mixin(logFiles[idx], {id: logFiles[idx].name});
                fileItems.push(item);
            }
            return fileItems;
        }

        FileBrowser.prototype.downloadSelectedFiles = function ()
        {
            var data = this.logFileGrid.grid.selection.getSelected();
            this.downloadFiles(data);
        }

        FileBrowser.prototype.downloadAllFiles = function ()
        {
            var parentModelObj = this.modelObj;
            var modelObj = {
                type: parentModelObj.type,
                name: "getAllFiles",
                parent: parentModelObj
            }
            this.management.download(modelObj, {});
        }

        FileBrowser.prototype.downloadFiles = function (fileItems)
        {
            if (fileItems.length)
            {
                var parentModelObj = this.modelObj;
                var modelObj = {
                    type: parentModelObj.type,
                    name: "getFiles",
                    parent: parentModelObj
                }
                var items = [];
                for (var i = 0; i < fileItems.length; i++)
                {
                    items.push(fileItems[i].id);
                }
                this.management.download(modelObj, {fileName: items});
            }
        }

        FileBrowser.prototype.update = function (restData)
        {
            if (this.logFileGrid)
            {
                this.logFiles = this.addIdToFileObjects(restData);
                if (this.logFileGrid.update(this.logFiles))
                {
                    //this.logFileGrid.grid._refresh();
                }
            }
        }

        return FileBrowser;
    });
