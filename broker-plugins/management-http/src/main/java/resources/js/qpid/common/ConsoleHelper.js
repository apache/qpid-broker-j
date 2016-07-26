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
        "qpid/management/Management",
        "qpid/common/util",
        "dijit/Dialog",
        "dojo/domReady!"], function (query, registry, entities, Structure, updater, Management, util, Dialog)
{

    var helpURL = null;
    var queryCreateDialog = null;
    var queryCreateDialogForm = null;

    return {
        showPreferencesDialog: function ()
        {
            this.management.userPreferences.showEditor();
        },
        getHelpUrl: function (callback)
        {
            this.management.load({type: "broker"}, {depth: 0})
                .then(function (data)
                {
                    var broker = data[0];
                    if ("context" in broker && "qpid.helpURL" in broker["context"])
                    {
                        helpURL = broker["context"]["qpid.helpURL"];
                    }
                    else
                    {
                        helpURL = "http://qpid.apache.org/";
                    }
                    if (callback)
                    {
                        callback(helpURL);
                    }
                });
        },
        showHelp: function ()
        {
            var openWindow = function (url)
            {
                var newWindow = window.open(url,
                    'QpidHelp',
                    'height=600,width=600,scrollbars=1,location=1,resizable=1,status=0,toolbar=0,titlebar=1,menubar=0',
                    true);
                newWindow.focus();
            }

            if (helpURL)
            {
                openWindow(helpURL)
            }
            else
            {
                this.getHelpUrl(openWindow);
            }
        },
        showAPI: function ()
        {
            var openWindow = function (url)
            {
                var newWindow = window.open(url,
                    'Qpid REST API',
                    'height=800,width=800,scrollbars=1,location=1,resizable=1,status=0,toolbar=1,titlebar=1,menubar=1',
                    true);
                newWindow.focus();
            }

            openWindow("/apidocs");
        },
        showQueryCreateDialog: function (e)
        {
            var management = this.management;
            var controller = this.controller;
            if (queryCreateDialog == null)
            {
                require(["qpid/management/query/QueryCreateDialogForm", "dojo/ready"],
                    function (QueryCreateDialogForm, ready)
                    {
                        ready(function ()
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
        init: function (controller, TreeView)
        {
            this.controller = controller;
            this.management = new Management("", util.xhrErrorHandler);
            this.structure = new Structure();

            var that = this;
            var management = this.management;
            var structure = this.structure;

            var authenticationSuccessCallback = function (data)
            {
                if (data.user)
                {
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
                            controlButton.set("label", management.getAuthenticatedUser().name);
                            controlButton.domNode.style.display = '';
                        }
                    });
                }
                else
                {
                    alert("User identifier is not found! Re-authenticate!");
                    that.logout();
                }
            };

            management.authenticate()
                .then(authenticationSuccessCallback);
        },
        logout: function ()
        {
            window.location = "logout";
        }

    };

});
