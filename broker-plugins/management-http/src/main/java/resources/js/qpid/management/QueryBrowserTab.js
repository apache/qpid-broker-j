/*
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
        "dojo/text!showQueryBrowserTab.html",
        "qpid/management/query/QueryBrowserWidget",
        "qpid/common/updater",
        "dojo/domReady!"],
    function (parser, query, template, QueryBrowserWidget, updater)
    {
        function QueryBrowserTab(name, parent, controller)
        {
            this.controller = controller;
            this.management = controller.management;
        }

        QueryBrowserTab.prototype.getTitle = function (changed)
        {
            return "Query Browser";
        };

        QueryBrowserTab.prototype.open = function (contentPane)
        {
            var that = this;
            this.contentPane = contentPane;
            contentPane.containerNode.innerHTML = template;
            parser.parse(contentPane.containerNode)
                .then(function (instances)
                {
                    that.onOpen(contentPane.containerNode)
                }, function (e)
                {
                    console.error("Unexpected error on parsing query tab template", e);
                });
        };

        QueryBrowserTab.prototype.onOpen = function (containerNode)
        {
            var that = this;
            var queryBrowserWidgetNode = query(".queryBrowserWidgetNode", containerNode)[0];
            this.queryBrowserWidget = new QueryBrowserWidget({
                management: this.management,
                structure: this.controller.structure
            }, queryBrowserWidgetNode);
            this.queryBrowserWidget.on("openQuery",
                function (event)
                {
                    that.controller.show("query", event.preference, event.parentObject, event.preference.id);
                });
            this.queryBrowserWidget.startup();

            this.contentPane.on("show",
                function ()
                {
                    that.queryBrowserWidget.resize();
                });
            updater.add(this.queryBrowserWidget);
        };

        QueryBrowserTab.prototype.close = function ()
        {
            updater.remove(this.queryBrowserWidget);
            if (this.queryBrowserWidget)
            {
                this.queryBrowserWidget.destroyRecursive();
                this.queryBrowserWidget = null;
            }
        };

        QueryBrowserTab.prototype.destroy = function ()
        {
            this.close();
            this.contentPane.onClose();
            this.controller.tabContainer.removeChild(this.contentPane);
            this.contentPane.destroyRecursive();
        };

        return QueryBrowserTab;
    });
