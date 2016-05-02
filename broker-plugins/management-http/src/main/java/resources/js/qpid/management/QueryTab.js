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
        "qpid/management/query/QueryBuilder",
        "dojo/dom-construct",
        "dojo/domReady!"],
    function (parser, query, registry, entities, properties, util, formatter, template, QueryBuilder, domConstruct)
    {

        function QueryTab(name, parent, controller)
        {
            this.name = name;
            this.controller = controller;
            this.management = controller.management;
            this.parent = parent;
        }

        QueryTab.prototype.getTitle = function ()
        {
            return "Query";
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
            this.queryEditorNode = query(".queryEditorNode", containerNode)[0];
            this.queryBuilder = new QueryBuilder({
                management: this.management,
                parentModelObj: this.parent,
                controller: this.controller
            }, this.queryEditorNode);
        };

        QueryTab.prototype.close = function ()
        {

        };

        QueryTab.prototype.destroy = function ()
        {
            this.close();
            this.contentPane.onClose();
            this.controller.tabContainer.removeChild(this.contentPane);
            this.contentPane.destroyRecursive();
        };

        return QueryTab;
    });
