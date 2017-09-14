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
        "qpid/common/properties",
        "qpid/common/util",
        "dijit/registry",
        "dojo/_base/event",
        "dojox/html/entities",
        "dojo/text!showPlugin.html",
        "dojo/domReady!"],
    function (parser, query, connect, properties, util, registry, event, entities, template)
    {

        function Plugin(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.modelObj = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.name = this.modelObj.name;
        }

        Plugin.prototype.getTitle = function ()
        {
            return "Plugin: " + this.name;
        };

        Plugin.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.pluginUpdater = new PluginUpdater(that);
                });
        };

        Plugin.prototype.close = function ()
        {
            if (this.pluginUpdater.details)
            {
                this.pluginUpdater.details.close();
            }
        };

        function PluginUpdater(tabObject)
        {
            this.contentPane = tabObject.contentPane;
            this.controller = tabObject.controller;
            this.modelObj = tabObject.modelObj;
            this.management = this.controller.management;
            var node = this.contentPane.containerNode;
            this.name = query(".name", node)[0];
            this.type = query(".type", node)[0];

            var that = this;

            this.management.load(that.modelObj, {excludeInheritedContext: true})
                .then(function (data)
                {
                    that.pluginData = data;

                    that.updateHeader();

                    require(["qpid/management/plugin/" + that.pluginData.type.toLowerCase()
                        .replace('-', '')], function (SpecificPlugin)
                    {
                        that.details =
                            new SpecificPlugin(query(".pluginDetails", node)[0], that.modelObj, that.controller, that.contentPane);
                    });

                }, util.xhrErrorHandler);

        }

        PluginUpdater.prototype.updateHeader = function ()
        {
            this.name.innerHTML = entities.encode(String(this.pluginData["name"]));
            this.type.innerHTML = entities.encode(String(this.pluginData["type"]));
        };

        return Plugin;
    });
