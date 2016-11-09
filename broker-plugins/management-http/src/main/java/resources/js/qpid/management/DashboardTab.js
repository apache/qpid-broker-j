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
        "dojo/_base/lang",
        "dojo/promise/all",
        "dojo/Deferred",
        "dojo/query",
        "dojo/json",
        "qpid/common/util",
        "dojo/text!showDashboardTab.html",
        "qpid/common/MessageDialog",
        "qpid/management/dashboard/DashboardWidget",
        "dojo/domReady!"],
    function (parser, lang, all, Deferred, query, json, util, template, MessageDialog)
    {

        function DashboardTab(kwArgs)
        {
            this.controller = kwArgs.controller;
            this.tabData = kwArgs.tabData;
            this.parent = kwArgs.tabData.modelObject;
            this.management = this.controller.management;
            this.changed = !this.tabData.data || !this.tabData.data.name;
        }

        DashboardTab.prototype.getTitle = function ()
        {
            if (this.tabData.preferenceId && !this.tabData.data)
            {
                return "Loading...";
            }
            var name = this.tabData.data.name ? this.tabData.data.name : "New";
            var prefix = this.tabData.data.name && !this.changed ? "" : "*";
            var path = this.controller.structure.getHierarchicalName(this.parent);
            return prefix + "Dashboard:" + name + path;
        };

        DashboardTab.prototype.open = function (contentPane)
        {
            this.contentPane = contentPane;
            contentPane.containerNode.innerHTML = template;
            var parserPromise = parser.parse(contentPane.containerNode);
            var preferencePromise = null;
            if (this.tabData.preferenceId && !this.tabData.data)
            {
                preferencePromise = this.management.getPreferenceById(this.parent, this.tabData.preferenceId);
            }
            else
            {
                var deferred = new Deferred();
                var obj = {};
                obj[this.tabData.data.type] = [this.tabData.data];
                deferred.resolve(obj);
                preferencePromise = deferred.promise;
            }
            all({parser: parserPromise, preference: preferencePromise})
                .then(lang.hitch(this, function (data)
                {
                    for (var type in data.preference)
                    {
                        var preferences = data.preference[type];
                        if (preferences[0])
                        {
                            this.tabData.data = preferences[0];
                            this.changed = !this.tabData.data.name;
                        }
                        if (preferences.length !== 1)
                        {
                            console.warn("Unexpected number of preferences returned for id "
                                         + this.tabData.preferenceId);
                        }
                    }

                    if (this.tabData.data)
                    {
                        this.onOpen(contentPane.containerNode)
                    }
                    else
                    {
                        this.management.userPreferences.removeTab(this.tabData);
                        this.destroy();
                    }

                }), lang.hitch(this, function (e)
                {
                    this.management.errorHandler(e);
                }));
        };

        DashboardTab.prototype.onOpen = function (containerNode)
        {
            this.contentPane.set("title", this.getTitle());
            this.dashboardWidgetNode = query(".dashboardWidgetNode", containerNode)[0];

            this.dashboardWidget = new qpid.management.dashboard.DashboardWidget({
                management: this.management,
                parentObject: this.parent,
                preference: this.tabData.data,
                controller: this.controller,
                structure: this.controller.structure
            }, this.dashboardWidgetNode);

            this.dashboardWidget.on("save", lang.hitch(this, function(e)
            {
                this.tabData.data = e.preference;
                this.changed = false;
                var title = this.getTitle();
                this.contentPane.set("title", title);
            }));
            this.dashboardWidget.on("change", lang.hitch(this, function(e)
            {
                this.changed = true;
                var title = this.getTitle();
                this.contentPane.set("title", title);
            }));
            this.dashboardWidget.on("delete", lang.hitch(this, function(e)
            {
                this.management.userPreferences.removeTab(this.tabData);
                this.destroy();
            }));
            this.dashboardWidget.on("clone", lang.hitch(this, function(e)
            {
                this.controller.showTab({
                    preferenceId: e.preference.id,
                    tabType: "dashboard",
                    data: e.preference,
                    modelObject: e.parentObject
                });
            }));
            this.dashboardWidget.startup();
            this.contentPane.on("show", lang.hitch(this, function ()
            {
                this.dashboardWidget.activate();
            }));
            this.contentPane.on("hide", lang.hitch(this, function ()
            {
                this.dashboardWidget.deactivate();
            }));
        };

        DashboardTab.prototype.close = function ()
        {
            if (!this.changed)
            {
                this.destroy();
                return true;
            }

            MessageDialog.confirm({
                title: "Discard unsaved changed?",
                message: "<div>Dashbord contains unsaved changes.<br/>Would you like to close it anyway?</div>",
                confirmationId: "dashboard.confirmation.close.changed"
            })
                .then(lang.hitch(this, function(){this.destroy(true);}));

            return false;
        };

        DashboardTab.prototype.destroy = function (destroyContentPane)
        {
            if (this.destroyed)
            {
                return;
            }

            this.destroyed = true;
            this.changed = false;
            this.contentPane.onClose();
            if (this.dashboardWidget != null)
            {
                this.dashboardWidget.destroyRecursive();
                this.dashboardWidget = null;
            }

            if (destroyContentPane)
            {
                this.contentPane.getParent().removeChild(this.contentPane);
                this.contentPane.destroyRecursive();
            }
        };

        DashboardTab.stopDisplayingConfirmation = false;

        return DashboardTab;
    });
