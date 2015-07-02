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
define(["dojo/_base/xhr",
        "dojo/parser",
        "dojo/query",
        "dojo/date/locale",
        "dijit/registry",
        "qpid/common/grid/GridUpdater",
        "qpid/common/grid/UpdatableGrid",
        "dojo/text!logger/memory/showLogViewer.html",
        "dojo/domReady!"],
       function (xhr, parser, query, locale, registry, GridUpdater, UpdatableGrid, template) {

           var defaulGridRowLimit = 4096;
           var currentTimeZone;

           function dataTransformer(data, userPreferences)
           {
             for(var i=0; i < data.length; i++)
             {
               data[i].time = userPreferences.addTimeZoneOffsetToUTC(data[i].timestamp);
             }
             return data;
           }

           function LogViewer(loggerModelObj, management, containerNode) {
               var that = this;
               this.management = management;
               this.modelObj = {type: loggerModelObj.type, name: "getLogEntries", parent: loggerModelObj};
               this.lastLogId = 0;
               this.containerNode = containerNode;
               containerNode.innerHTML = template;
               parser.parse(containerNode).then(function(instances){that._buildGrid();});
           }


           LogViewer.prototype._buildGrid = function() {
               var that = this;
               var userPreferences = this.management.userPreferences;
               currentTimeZone = userPreferences.getTimeZoneDescription();
               var gridStructure = [
                    {
                      hidden: false,
                      name: "ID",
                      field: "id",
                      width: "50px",
                      datatype: "number",
                      filterable: true
                    },
                    {
                      name: "Date", field: "time", width: "100px", datatype: "date",
                        formatter: function(val) {
                        return userPreferences.formatDateTime(val, {selector:"date"});
                      }
                    },
                    { name: "Time ", field: "time", width: "100px", datatype: "time",
                     formatter: function(val) {
                       return userPreferences.formatDateTime(val, {selector:"time"});
                     }
                   },
                   {
                     name: "Time zone",
                     field: "time",
                     width: "80px",
                     datatype: "string",
                     hidden: true,
                     filterable: false,
                     formatter: function(val) {
                       return currentTimeZone;
                     }
                   },
                   { name: "Level", field: "level", width: "50px", datatype: "string", autoComplete: true, hidden: true},
                   { name: "Logger", field: "logger", width: "150px", datatype: "string", autoComplete: false, hidden: true},
                   { name: "Thread", field: "threadName", width: "100px", datatype: "string", hidden: true},
                   { name: "Log Message", field: "message", width: "auto", datatype: "string"}
               ];

               var gridNode = query(".logEntries", this.containerNode)[0];
               try
               {
                 var updater = new GridUpdater({
                     userPreferences: userPreferences,
                     updatable: false,
                     serviceUrl: function()
                     {
                       return that.management.buildObjectURL(that.modelObj, {lastLogId: that.lastLogId});
                     },
                     onUpdate: function(items)
                     {
                       if (items)
                       {
                         var maxId = -1;
                         for(var i in items)
                         {
                           var item = items[i];
                           if (item.id > maxId)
                           {
                             maxId = item.id
                           }
                         }
                         if (maxId != -1)
                         {
                           that.lastLogId = maxId
                         }
                       }
                     },
                     append: true,
                     appendLimit: defaulGridRowLimit,
                     dataTransformer: function(data){ return dataTransformer(data, userPreferences);}
                 });
                 this.grid = new UpdatableGrid(updater.buildUpdatableGridArguments({
                     structure: gridStructure,
                     selectable: true,
                     selectionMode: "none",
                     sortInfo: -1,
                     sortFields: [{attribute: 'id', descending: true}],
                     plugins:{
                       nestedSorting:true,
                       enhancedFilter:{defaulGridRowLimit: defaulGridRowLimit,displayLastUpdateTime:true},
                       indirectSelection: false,
                       pagination: {defaultPageSize: 10}
                     }
                  }), gridNode);
                 var onStyleRow = function(row)
                 {
                   var item = that.grid.getItem(row.index);
                   if(item){
                      var level = that.grid.store.getValue(item, "level", null);
                      var changed = false;
                      if(level == "ERROR"){
                          row.customClasses += " redBackground";
                          changed = true;
                      } else if(level == "WARN"){
                          row.customClasses += " yellowBackground";
                          changed = true;
                      } else if(level == "DEBUG"){
                          row.customClasses += " grayBackground";
                          changed = true;
                      }
                      if (changed)
                      {
                          that.grid.focus.styleRow(row);
                      }
                   }
                 };
                 this.grid.on("styleRow", onStyleRow);
                 this.grid.startup();
                 userPreferences.addListener(this);
               }
               catch(err)
               {
                 if (console && console.error)
                 {
                   console.error(err);
                 }
               }
           };

           LogViewer.prototype.onPreferencesChange = function(data)
           {
             var userPreferences = this.management.userPreferences;
             currentTimeZone = userPreferences.getTimeZoneDescription();
             if (this.grid.updater.memoryStore)
             {
                dataTransformer(this.grid.updater.memoryStore.data, userPreferences);
                this.grid._refresh();
             }
           };

           LogViewer.prototype.close = function(data)
           {
             this.management.userPreferences.removeListener(this);
           }

           return LogViewer;
       });
