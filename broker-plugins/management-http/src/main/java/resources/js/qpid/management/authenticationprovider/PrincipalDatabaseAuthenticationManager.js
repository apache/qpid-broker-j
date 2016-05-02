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
define(["dojo/dom",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-construct",
        "dojo/_base/connect",
        "dojo/_base/window",
        "dojo/_base/event",
        "dojo/_base/json",
        "dijit/registry",
        "qpid/common/util",
        "qpid/common/properties",
        "qpid/common/UpdatableStore",
        "dojox/grid/EnhancedGrid",
        "dojo/text!authenticationprovider/showPrincipalDatabaseAuthenticationManager.html",
        "dojo/text!authenticationprovider/addUser.html",
        "dojo/text!authenticationprovider/setPassword.html",
        "dojox/grid/enhanced/plugins/Pagination",
        "dojox/grid/enhanced/plugins/IndirectSelection",
        "dojox/validate/us",
        "dojox/validate/web",
        "dijit/Dialog",
        "dijit/form/TextBox",
        "dijit/form/ValidationTextBox",
        "dijit/form/TimeTextBox",
        "dijit/form/Button",
        "dijit/form/Form",
        "dijit/form/DateTextBox",
        "dojo/domReady!"],
    function (dom,
              parser,
              query,
              construct,
              connect,
              win,
              event,
              json,
              registry,
              util,
              properties,
              UpdatableStore,
              EnhancedGrid,
              template,
              addUserTemplate,
              setPasswordTemplate)
    {
        function DatabaseAuthManager(containerNode, authProviderObj, controller)
        {
            var node = construct.create("div", null, containerNode, "last");
            var that = this;
            this.name = authProviderObj.name;
            this.modelObj = authProviderObj;
            this.management = controller.management;
            node.innerHTML = template;
            parser.parse(node)
                .then(function (instances)
                {
                    that.init(node, authProviderObj, controller);
                });
        }

        DatabaseAuthManager.prototype.update = function ()
        {
            this.authDatabaseUpdater.update();
        };

        DatabaseAuthManager.prototype.close = function ()
        {
            updater.remove(this.authDatabaseUpdater);
        };

        DatabaseAuthManager.prototype.init = function (node, authProviderObj, controller)
        {
            this.controller = controller;
            var that = this;

            that.authProviderData = {};

            var userDiv = query(".users")[0];

            var gridProperties = {
                height: 400,
                keepSelection: true,
                plugins: {
                    pagination: {
                        pageSizes: ["10", "25", "50", "100"],
                        description: true,
                        sizeSwitch: true,
                        pageStepper: true,
                        gotoButton: true,
                        maxPageStep: 4,
                        position: "bottom"
                    },
                    indirectSelection: true

                }
            };

            that.usersGrid = new UpdatableStore(that.authProviderData.users, userDiv, [{
                name: "User Name",
                field: "name",
                width: "100%"
            }], function (obj)
            {
                connect.connect(obj.grid, "onRowDblClick", obj.grid, function (evt)
                {
                    var idx = evt.rowIndex, theItem = this.getItem(idx);
                    var name = obj.dataStore.getValue(theItem, "name");
                    var id = obj.dataStore.getValue(theItem, "id");
                    setPassword.show(that.management, that.modelObj, {
                        name: name,
                        id: id
                    });
                });
            }, gridProperties, EnhancedGrid);

            var addUserButton = query(".addUserButton", node)[0];
            connect.connect(registry.byNode(addUserButton), "onClick", function (evt)
            {
                addUser.show(that.management, that.modelObj)
            });

            var deleteUserButton = query(".deleteUserButton", node)[0];
            var deleteWidget = registry.byNode(deleteUserButton);
            connect.connect(deleteWidget, "onClick", function (evt)
            {
                event.stop(evt);
                that.deleteUsers();
            });
        }

        DatabaseAuthManager.prototype.deleteUsers = function ()
        {
            var grid = this.usersGrid.grid;
            var data = grid.selection.getSelected();
            if (data.length)
            {
                var that = this;
                if (confirm("Delete " + data.length + " users?"))
                {
                    var i;
                    var parameters = {id: []};
                    for (i = 0; i < data.length; i++)
                    {
                        parameters.id.push(data[i].id);
                    }

                    that.success = true
                    this.management.remove({
                            type: "user",
                            parent: this.modelObj
                        }, parameters)
                        .then(function (data)
                        {
                            grid.setQuery({id: "*"});
                            grid.selection.deselectAll();
                        }, util.xhrErrorHandler);
                }
            }
        };

        DatabaseAuthManager.prototype.update = function (data)
        {
            this.authProviderData = data;
            this.name = data.name
            this.usersGrid.update(this.authProviderData.users);
        };

        var addUser = {};

        var node = construct.create("div", null, win.body(), "last");

        var convertToUser = function convertToUser(formValues)
        {
            var newUser = {};
            newUser.name = formValues.name;
            for (var propName in formValues)
            {
                if (formValues.hasOwnProperty(propName))
                {
                    if (formValues[propName] !== "")
                    {
                        newUser[propName] = formValues[propName];
                    }
                }
            }

            return newUser;
        };

        node.innerHTML = addUserTemplate;
        addUser.dialogNode = dom.byId("addUser");
        parser.parse(node)
            .then(function (instances)
            {
                var theForm;
                theForm = registry.byId("formAddUser");
                theForm.on("submit", function (e)
                {

                    event.stop(e);
                    if (theForm.validate())
                    {

                        var newUser = convertToUser(theForm.getValues());

                        addUser.management.create("user", addUser.authProvider, newUser)
                            .then(function (x)
                            {
                                registry.byId("addUser")
                                    .hide();
                            });
                        return false;

                    }
                    else
                    {
                        alert('Form contains invalid data.  Please correct first');
                        return false;
                    }

                });
            });

        addUser.show = function (management, authProvider)
        {
            addUser.management = management;
            addUser.authProvider = authProvider;
            registry.byId("formAddUser")
                .reset();
            registry.byId("addUser")
                .show();
        };

        var setPassword = {};

        var setPasswordNode = construct.create("div", null, win.body(), "last");

        setPasswordNode.innerHTML = setPasswordTemplate;
        setPassword.dialogNode = dom.byId("setPassword");
        parser.parse(setPasswordNode)
            .then(function (instances)
            {
                var theForm = registry.byId("formSetPassword");
                theForm.on("submit", function (e)
                {

                    event.stop(e);
                    if (theForm.validate())
                    {

                        var newUser = convertToUser(theForm.getValues());
                        newUser.name = setPassword.name;
                        newUser.id = setPassword.id;

                        var model = {
                            type: "user",
                            name: setPassword.name,
                            parent: setPassword.authProvider
                        };
                        setPassword.management.update(model, newUser)
                            .then(function (x)
                            {
                                registry.byId("setPassword")
                                    .hide();
                            });
                        return false;

                    }
                    else
                    {
                        alert('Form contains invalid data.  Please correct first');
                        return false;
                    }

                });
            });

        setPassword.show = function (management, authProvider, user)
        {
            setPassword.management = management;
            setPassword.authProvider = authProvider;
            setPassword.name = user.name;
            setPassword.id = user.id;
            registry.byId("formSetPassword")
                .reset();

            var namebox = registry.byId("formSetPassword.name");
            namebox.set("value", user.name);
            namebox.set("disabled", true);

            registry.byId("setPassword")
                .show();

        };

        return DatabaseAuthManager;
    });
