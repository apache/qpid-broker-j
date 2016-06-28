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
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/properties",
        "qpid/common/util",
        "qpid/common/formatter",
        "dojo/text!showQueryTab.html",
        "qpid/management/query/QueryWidget",
        "dojo/dom-construct",
        "dojo/domReady!"],
    function (parser, query, registry, entities, properties, util, formatter, template, QueryWidget, domConstruct)
    {
        function getPath(parentObject)
        {
            if (parentObject)
            {
                var type = parentObject.type.charAt(0).toUpperCase() + parentObject.type.substring(1);
                var val = parentObject.name;
                for (var i = parentObject.parent; i && i.name; i = i.parent)
                {
                    val = i.name + "/" + val;
                }
                return type + ":" + val;
            }
            return "";
        }

        function QueryTab(data, parent, controller)
        {
            this.controller = controller;
            this.management = controller.management;
            this.parent = parent;
            this.preference = data;
        }

        QueryTab.prototype.getTitle = function (changed)
        {
            var category = "";
            if (this.preference && this.preference.value && this.preference.value.category)
            {
                category = this.preference.value.category;
                category = category.charAt(0).toUpperCase() + category.substring(1);
            }
            var name = this.preference.id ? this.preference.name : "New";
            var prefix = this.preference.id && !changed ? "" : "*";
            var path = this.parent && this.parent.name ? " (" + getPath(this.parent)+ ")" : "";
            return prefix + category + " query:" + name + path;
        };

        QueryTab.prototype.open = function (contentPane)
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

        QueryTab.prototype.onOpen = function (containerNode)
        {
            this.queryWidgetNode = query(".queryWidgetNode", containerNode)[0];
            this.queryWidget = new QueryWidget({
                management: this.management,
                parentObject: this.parent,
                preference: this.preference,
                controller: this.controller
            }, this.queryWidgetNode);
            var that = this;
            this.queryWidget.on("save", function(e)
            {
                that.preference = e.preference;
                var title = that.getTitle();
                that.contentPane.set("title", title);
            });
            this.queryWidget.on("change", function(e)
            {
                var changed = !util.equals(this.preference, e.preference);
                var title = that.getTitle(changed);
                that.contentPane.set("title", title);
            });
            this.queryWidget.on("delete", function(e)
            {
                that.destroy();
            });
            this.queryWidget.startup();
        };

        QueryTab.prototype.close = function ()
        {

        };

        QueryTab.prototype.destroy = function ()
        {
            this.close();
            this.contentPane.onClose();
            this.controller.tabContainer.removeChild(this.contentPane);
            this.queryWidget.destroyRecursive();
            this.contentPane.destroyRecursive();
        };

        return QueryTab;
    });
