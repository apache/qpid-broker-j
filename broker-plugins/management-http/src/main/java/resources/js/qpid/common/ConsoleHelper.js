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
define(["dojo/query",
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/Structure",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/management/Management",
        "qpid/management/treeView",
        "qpid/management/controller",
        "dijit/Dialog",
        "dojo/dom-class",
        "dojo/dom",
        "dojo/domReady!"],
    function (query, registry, entities, Structure, updater, util, Management, TreeView, controller, Dialog, domClass, dom)
{

    var documentationUrl = null;
    var queryCreateDialog = null;
    var queryCreateDialogForm = null;
    var dashboardCreateDialog = null;
    var dashboardCreateDialogForm = null;

    var openWindow = function (url, title)
    {
        var newWindow = window.open(
            url,
            title,
            'height=600,width=600,scrollbars=1,location=1,resizable=1,status=0,toolbar=1,titlebar=1,menubar=0',
            true);
        if (newWindow)
        {
            newWindow.focus();
        }
    };

    return {
        showPreferencesDialog: function ()
        {
            this.management.userPreferences.showEditor();
        },
        showDocumentation: function ()
        {
            if (documentationUrl)
            {
                openWindow(documentationUrl, "Qpid Documentation");
            }
            else
            {
                this.management.load({type: "broker"}, {depth: 0, excludeInheritedContext: true})
                    .then(function (broker)
                    {
                        if (broker.documentationUrl)
                        {
                            documentationUrl = broker.documentationUrl;
                        }
                        else
                        {
                            documentationUrl = "http://qpid.apache.org/components/broker-j/";
                        }
                        openWindow(documentationUrl, "Qpid Documentation")
                    });
            }
        },
        showAPI: function ()
        {
            openWindow(getContextPath() + "apidocs", 'Qpid REST API');
        },
        showQueryCreateDialog: function (e)
        {
            var management = this.management;
            var controller = this.controller;
            if (queryCreateDialog === null)
            {
                require(["qpid/management/query/QueryCreateDialogForm"],
                    function (QueryCreateDialogForm)
                    {
                        queryCreateDialogForm =
                            new QueryCreateDialogForm({management: management, structure: controller.structure});
                        queryCreateDialogForm.on("create", function (e)
                        {
                            queryCreateDialog.hide();
                            var tabData = {
                                tabType: "query",
                                modelObject: e.parentObject,
                                data: e.preference,
                                preferenceId: e.preference.id
                            };
                            controller.showTab(tabData);
                        });
                        queryCreateDialogForm.on("cancel", function (e)
                        {
                            queryCreateDialog.hide();
                        });
                        queryCreateDialog = new Dialog({title: "Create query", content: queryCreateDialogForm});
                        queryCreateDialog.show();
                    });
            }
            else
            {
                queryCreateDialogForm.initScope();
                queryCreateDialog.show();
            }
        },
        showQueryBrowser: function (e)
        {
            this.controller.showTab({
                tabType: "queryBrowser"
            });
        },
        showDashboardCreateDialog: function (e)
        {
            var management = this.management;
            var controller = this.controller;
            if (dashboardCreateDialog === null)
            {
                require(["qpid/management/dashboard/DashboardCreateDialogForm"],
                    function (DashboardCreateDialogForm)
                    {
                        dashboardCreateDialogForm =
                            new DashboardCreateDialogForm({management: management, structure: controller.structure});
                        dashboardCreateDialogForm.on("create", function (e)
                        {
                            dashboardCreateDialog.hide();
                            var tabData = {
                                tabType: "dashboard",
                                modelObject: e.parentObject,
                                data: e.preference,
                                preferenceId: e.preference.id
                            };
                            controller.showTab(tabData);
                        });
                        dashboardCreateDialogForm.on("cancel", function (e)
                        {
                            dashboardCreateDialog.hide();
                        });
                        dashboardCreateDialog = new Dialog({title: "Create dashboard", content: dashboardCreateDialogForm});
                        dashboardCreateDialog.show();
                    });
            }
            else
            {
                dashboardCreateDialogForm.initScope();
                dashboardCreateDialog.show();
            }
        },
        showDashboardBrowser: function (e)
        {
            this.controller.showTab({
                tabType: "dashboardBrowser"
            });
        },
        init: function ()
        {
            this.controller = controller;
            this.management = new Management("", util.xhrErrorHandler);
            this.management.addErrorCallback(401, updater.cancel);
            this.structure = new Structure();

            var management = this.management;
            var structure = this.structure;

            var authenticationSuccessCallback = function ()
            {
                domClass.add(dom.byId("loginLayout"), "dijitHidden");

                var pageLagoutContainer = registry.byId("pageLayout");
                domClass.remove(pageLagoutContainer.domNode, "dijitHidden")
                pageLagoutContainer.resize();

                var controlButton = registry.byId("authenticatedUserControls");
                registry.byId("login").domNode.style.display = "inline";
                management.init(function ()
                {
                    updater.registerUpdateIntervalListener(management.userPreferences);
                    var treeView = new TreeView(management, query('div[qpid-type="treeView"]')[0]);
                    controller.init(management, structure, treeView);
                    dijit.Tooltip.defaultPosition =
                        ["after-centered",
                         "below-centered"];
                    if (controlButton)
                    {
                        var userName = management.getAuthenticatedUser();
                        controlButton.set("label", util.toFriendlyUserName(userName));
                        controlButton.set("title", userName);
                        controlButton.domNode.style.display = '';
                    }
                });
            };

            this.management.getSaslStatus().then(function (data)
            {
                domClass.add(dom.byId("loadingLayout"), "dijitHidden");
                if (data.user)
                {
                    authenticationSuccessCallback();
                }
                else
                {
                    domClass.remove(dom.byId("loginLayout"), "dijitHidden");
                    var loginForm = registry.byId("loginForm");
                    loginForm.on("submit", function (credentials)
                    {
                        management.authenticate(data.mechanisms, credentials)
                            .then(function ()
                                {
                                    loginForm.hide();
                                    authenticationSuccessCallback();
                                },
                                function (error)
                                {
                                    loginForm.onError(error);
                                });
                    });
                    loginForm.show();
                }
            }, util.xhrErrorHandler);
        },
        logout: function ()
        {
            window.location = "logout";
        }

    };

});
