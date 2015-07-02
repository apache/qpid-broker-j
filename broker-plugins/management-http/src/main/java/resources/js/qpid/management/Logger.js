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
define(["dojo/parser",
    "dojo/query",
    "dojo/_base/connect",
    "dijit/registry",
    "dojox/html/entities",
    "qpid/common/properties",
    "qpid/common/updater",
    "qpid/common/util",
    "qpid/common/formatter",
    "qpid/common/UpdatableStore",
    "dojox/grid/EnhancedGrid",
    "dojo/text!showLogger.html",
    "qpid/management/addLogger",
    "qpid/management/addLoggerFilter",
    "dojo/domReady!"],
  function (parser, query, connect, registry, entities, properties, updater, util, formatter, UpdatableStore, EnhancedGrid, template, addLogger, addLoggerFilter)
  {

    function Logger(name, parent, controller)
    {
      this.name = name;
      this.controller = controller;
      this.management = controller.management;
      var isBrokerParent = parent.type == "broker";
      this.category = isBrokerParent ? "BrokerLogger" : "VirtualHostLogger";
      this.filterCategory = this.category + "Filter";
      this.modelObj = {type: this.category.toLowerCase(), name: name, parent: parent};
      this.userFriendlyName = (isBrokerParent ? "Broker Logger" : "Virtual Host Logger");
    }

    Logger.prototype.getTitle = function ()
    {
      return this.userFriendlyName + ": " + this.name;
    };

    Logger.prototype.open = function (contentPane)
    {
      var that = this;
      this.contentPane = contentPane;
      contentPane.containerNode.innerHTML = template;
      parser.parse(contentPane.containerNode).then(function (instances)
      {
        that.onOpen(contentPane.containerNode)
      });

    };

    Logger.prototype.onOpen = function (containerNode)
    {
      var that = this;
      this.editLoggerButton = registry.byNode(query(".editLoggerButton", containerNode)[0]);
      this.deleteLoggerButton = registry.byNode(query(".deleteLoggerButton", containerNode)[0]);
      this.deleteLoggerButton.on("click",
        function (e)
        {
          if (confirm("Are you sure you want to delete logger '" + entities.encode(String(that.name)) + "'?"))
          {
            that.management.remove(that.modelObj).then(
              function (x)
              {
                that.destroy();
              }, util.xhrErrorHandler);
          }
        }
      );
      this.editLoggerButton.on("click",
        function (event)
        {
          that.management.load(that.modelObj, {actuals: true, depth: 0}).then(
            function(data)
            {
              addLogger.show(that.management, that.modelObj, that.category, data[0]);
            }
          );
        }
      );

      var gridProperties = {
        selectionMode: "extended",
        plugins: {
          indirectSelection: true
        }
      };

      this.filterGrid = new UpdatableStore([], query(".filterGrid", containerNode)[0],
        [
          {name: "Name", field: "name", width: "40%"},
          {name: "Type", field: "type", width: "20%"},
          {name: "Level", field: "level", width: "20%"},
          {name: "Durable", field: "durable", width: "20%", formatter: util.buildCheckboxMarkup}
        ], function (obj)
        {
          connect.connect(obj.grid, "onRowDblClick", obj.grid,
            function (evt)
            {
              var idx = evt.rowIndex;
              var theItem = this.getItem(idx);
              that.showFilter(theItem);
            });
        }, gridProperties, EnhancedGrid);

      this.addLoggerFilterButton = registry.byNode(query(".addFilterButton", containerNode)[0]);
      this.deleteLoggerFilterButton = registry.byNode(query(".deleteFilterButton", containerNode)[0]);

      this.deleteLoggerFilterButton.on("click",
        function (e)
        {
          util.deleteSelectedObjects(
            that.filterGrid.grid,
            "Are you sure you want to delete logger filter",
            that.management,
            {type: that.filterCategory.toLowerCase(), parent: that.modelObj},
            that.loggerUpdater);
        }
      );

      this.addLoggerFilterButton.on("click",
        function(e)
        {
          addLoggerFilter.show(that.management, that.modelObj, that.filterCategory);
        }
      );

      this.loggerUpdater = new Updater(this);
      this.loggerUpdater.update(function (x)
      {
        updater.add(that.loggerUpdater);
      });
    };

    Logger.prototype.showFilter = function (item)
    {
      var filterModelObj = {name: item.name, type: this.filterCategory.toLowerCase(), parent: this.modelObj};
      var that = this;
      this.management.load(filterModelObj, {actuals: true}).then(
                              function(data)
                              {
                                addLoggerFilter.show(that.management, filterModelObj, that.filterCategory, data[0]);
                              });
    };

    Logger.prototype.close = function ()
    {
      updater.remove(this.loggerUpdater);
    };

    Logger.prototype.destroy = function ()
    {
      this.close();
      this.contentPane.onClose();
      this.controller.tabContainer.removeChild(this.contentPane);
      this.contentPane.destroyRecursive();
    };

    function Updater(logger)
    {
      var domNode = logger.contentPane.containerNode;
      this.tabObject = logger;
      this.modelObj = logger.modelObj;
      var that = this;

      function findNode(name)
      {
        return query("." + name, domNode)[0];
      }

      function storeNodes(names)
      {
        for (var i = 0; i < names.length; i++)
        {
          that[names[i]] = findNode(names[i]);
        }
      }

      storeNodes(["name", "state", "type", "loggerAttributes", "loggerTypeSpecificDetails", "filterWarning", "durable"]);
    }

    Updater.prototype.update = function (callback)
    {
      var that = this;
      that.tabObject.management.load(this.modelObj).then(
        function (data)
        {
          that.loggerData = data[0] || {};
          that.updateUI(that.loggerData);

          if (callback)
          {
            callback();
          }
        },
        function (error)
        {
          util.tabErrorHandler(error, {
            updater: that,
            contentPane: that.tabObject.contentPane,
            tabContainer: that.tabObject.controller.tabContainer,
            name: that.modelObj.name,
            category: that.tabObject.userFriendlyName
          });
        });
    };

    Updater.prototype.updateUI = function (data)
    {
      this.name.innerHTML = entities.encode(String(data["name"]));
      this.state.innerHTML = entities.encode(String(data["state"]));
      this.type.innerHTML = entities.encode(String(data["type"]));
      this.durable.innerHTML = util.buildCheckboxMarkup(data["durable"]);

      if (!this.details)
      {
        var that = this;
        require(["qpid/management/logger/" + this.tabObject.modelObj.type + "/show"],
          function (Details)
          {
            that.details = new Details({
              containerNode: that.loggerAttributes,
              typeSpecificDetailsNode: that.loggerTypeSpecificDetails,
              metadata: that.tabObject.management.metadata,
              data: data,
              management: that.tabObject.management,
              modelObj: that.tabObject.modelObj
            });
          }
        );
      }
      else
      {
        this.details.update(data);
      }

      var filterFieldName = this.tabObject.filterCategory.toLowerCase() + "s"; // add plural "s"
      if (data[filterFieldName])
      {
        this.tabObject.filterGrid.grid.domNode.style.display = "block";
        this.filterWarning.style.display = "none";
      }
      else
      {
        this.tabObject.filterGrid.grid.domNode.style.display = "none";
        this.filterWarning.style.display = "block";
      }
      util.updateUpdatableStore(this.tabObject.filterGrid, data[filterFieldName]);
    };

    return Logger;
  });
