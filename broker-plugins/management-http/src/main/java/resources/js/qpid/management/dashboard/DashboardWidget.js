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
define(["dojo/_base/declare",
        "dojo/_base/lang",
        "dojo/json",
        "dojo/Evented",
        "dojo/text!dashboard/DashboardWidget.html",
        "dojo/text!dashboard/AddWidgetDialogContent.html",
        "dojo/text!dashboard/QueryWidgetSettings.html",
        "qpid/management/preference/PreferenceBrowserWidget",
        "qpid/management/preference/PreferenceSaveDialogContent",
        "dojox/uuid/generateRandomUuid",
        "dojo/promise/all",
        "dojo/Deferred",
        "qpid/common/MessageDialog",
        "dojox/layout/GridContainer",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/Button",
        "dijit/Toolbar",
        "dijit/Tooltip",
        "dijit/Dialog",
        "dijit/registry"],
    function (declare,
              lang,
              json,
              Evented,
              template,
              addWidgetDialogContentTemplate,
              queryWidgetSettingsTemplate,
              PreferenceBrowserWidget,
              PreferenceSaveDialogContent,
              generateRandomUuid,
              all,
              Deferred,
              MessageDialog)
    {

        var _failedWidgetTypes = {};

        var AddWidgetDialogContent = declare("qpid.management.dashboard.AddWidgetDialogContent",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: addWidgetDialogContentTemplate.replace(/<!--[\s\S]*?-->/g, ""),

                // template fields
                cancelButton: null,
                queryBrowserNode: null,

                // constructor mixed-in fields
                structure: null,
                management: null,
                preferenceRoot: null,

                // inner fields
                _queryBrowser: null,

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this.cancelButton.on("click", lang.hitch(this, function ()
                    {
                        this.emit("cancel");
                    }));
                    this._queryBrowser = new PreferenceBrowserWidget({
                        structure: this.structure,
                        management: this.management,
                        preferenceRoot: this.preferenceRoot,
                        preferenceType: "query",
                        preferenceTypeFriendlyPlural: "queries",
                        preferenceTypeFriendlySingular: "Query"
                    }, this.queryBrowserNode);
                    this._queryBrowser.on("open", lang.hitch(this, this._onOpenQuery));
                },
                startup: function ()
                {
                    this.inherited(arguments);
                    this._queryBrowser.startup();
                },
                resizeQueryBrowser: function ()
                {
                    this._queryBrowser.resize();
                },
                update: function ()
                {
                    return this._queryBrowser.update();
                },
                _onOpenQuery: function (event)
                {
                    var chosenWidget = {
                        type: "query",
                        preference: event.preference,
                        parentObject: event.parentObject,
                        settings: {type: "query", preference: {id: event.preference.id}},
                        id: generateRandomUuid()
                    };

                    this.emit("add", {widget: chosenWidget});
                }
            });

        return declare("qpid.management.dashboard.DashboardWidget",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

                // template fields
                saveButton: null,
                cloneButton: null,
                deleteButton: null,
                addWidgetButton: null,
                widgetContainer: null,
                saveButtonTooltip: null,
                cloneButtonTooltip: null,
                deleteButtonTooltip: null,

                // constructor mixed in fields
                parentObject: null,
                preference: null,
                controller: null,
                management: null,

                // inner fields
                _addWidgetDialog: null,
                _addWidgetDialogContent: null,
                _widgets: null,

                postCreate: function ()
                {
                    this.inherited(arguments);

                    this._widgets = [];
                    var ownDashboard = !this.preference || !this.preference.owner
                                       || this.preference.owner === this.management.getAuthenticatedUser();
                    var _newDashboard = !this.preference || !this.preference.createdDate;
                    this.saveButton.set("disabled", !ownDashboard);
                    this.deleteButton.set("disabled", !ownDashboard || _newDashboard);

                    if (!ownDashboard)
                    {
                        this.saveButtonTooltip.set("label", "The query belongs to another user.<br/>"
                                                            + "Use clone if you wish to make your own copy.");
                        this.deleteButtonTooltip.set("label", "This query belongs to another user.");
                    }

                    this.saveButton.on("click", lang.hitch(this, this._onSaveButton));
                    this.cloneButton.on("click", lang.hitch(this, this._onCloneButton));
                    this.deleteButton.on("click", lang.hitch(this, this._onDeleteButton));
                    this.addWidgetButton.on("click", lang.hitch(this, this._onAddWidget));
                    this._addWidgetDialogContent =
                        new AddWidgetDialogContent({
                            structure: this.controller.structure,
                            management: this.management,
                            preferenceRoot: this.parentObject
                        });
                    this._addWidgetDialog =
                        new dijit.Dialog({title: "Add Widget", content: this._addWidgetDialogContent});
                    this._addWidgetDialogContent.on("cancel",
                        lang.hitch(this._addWidgetDialog, this._addWidgetDialog.hide));
                    this._addWidgetDialogContent.on("add", lang.hitch(this, this._onWidgetChosen));
                    this._addWidgetDialog.on("show", lang.hitch(this._addWidgetDialogContent, this._addWidgetDialogContent.resizeQueryBrowser));
                    this._saveDashboardDialogContent = new PreferenceSaveDialogContent({management: this.management});
                    this._saveDashboardDialog =
                        new dijit.Dialog({title: "Save Dashboard", content: this._saveDashboardDialogContent});
                    this._saveDashboardDialogContent.on("cancel",
                        lang.hitch(this._saveDashboardDialog, this._saveDashboardDialog.hide));
                    this._saveDashboardDialogContent.on("save", lang.hitch(this, this._onPreferenceSave));

                    this.widgetContainer.subscribe("/dojox/mdnd/drop", lang.hitch(this, "_widgetDropped"));

                    this.widgetContainer.enableDnd();

                    this.preference.type = "X-Dashboard";
                    this._verifyLayout();
                    this._loadPreferencesAndRestoreWidgets();
                },
                activate: function ()
                {
                    for (var i = 0; i < this._widgets.length; i++)
                    {
                        this._widgets[i].activate();
                    }
                },
                deactivate: function ()
                {
                    for (var i = 0; i < this._widgets.length; i++)
                    {
                        this._widgets[i].deactivate();
                    }
                },
                destroyRecursive: function (preserveDom)
                {
                    this.inherited(arguments);
                    if (this._saveDashboardDialog)
                    {
                        this._saveDashboardDialog.destroyRecursive(preserveDom);
                        this._saveDashboardDialog = null;
                    }
                    if (this._addWidgetDialog)
                    {
                        this._addWidgetDialog.destroyRecursive(preserveDom);
                        this._addWidgetDialog = null;
                    }
                },
                _onSaveButton: function ()
                {
                    this._saveDashboardDialogContent.set("preference", this.preference);
                    this._saveDashboardDialog.show();
                },
                _onCloneButton: function ()
                {
                    var preference = {
                        id: generateRandomUuid(),
                        type: this.preference.type,
                        value: this.preference.value
                    };
                    this.emit("clone", {preference: preference, parentObject: this.parentObject});
                },
                _onDeleteButton: function ()
                {
                    var confirmation = MessageDialog.confirm({
                        title: "Discard dashboard?",
                        message: "Are you sure you want to delete this dashboard?",
                        confirmationId: "dashboard.confirmation.delete"
                    });
                    confirmation.then(lang.hitch(this, function ()
                    {
                        if (this.preference.id)
                        {
                            var deletePromise = this.management.deletePreference(this.parentObject,
                                this.preference.type,
                                this.preference.name);
                            deletePromise.then(lang.hitch(this, function (preference)
                            {
                                this.emit("delete");
                            }));
                        }
                        else
                        {
                            this.emit("delete");
                        }
                    }));
                },
                _onPreferenceSave: function (event)
                {
                    var preference = event.preference;
                    this.management.savePreference(this.parentObject, preference)
                        .then(lang.hitch(this, function ()
                        {
                            this.preference = preference;
                            this._saveDashboardDialog.hide();
                            this.emit("save", {preference: this.preference});
                            this.deleteButton.set("disabled", false);
                        }));
                },
                _onAddWidget: function ()
                {
                    this._addWidgetDialogContent.update()
                        .then(lang.hitch(this._addWidgetDialog, this._addWidgetDialog.show));
                },
                _onWidgetChosen: function (event)
                {
                    this._addWidgetDialog.hide();
                    var promise = this._createWidget(event.widget);
                    promise.then(lang.hitch(this, function (widget)
                    {
                        this._placePortlet(widget.portlet);
                        this._widgetChanged(widget);
                    }));
                },
                _createWidget: function (kwargs)
                {
                    var deferred = new Deferred();
                    if (_failedWidgetTypes[kwargs.type])
                    {
                        // Dojo loader failed before, return a cancelled promise.
                        var message = "Previous attempt to load module for widget type '"
                                      + kwargs.type + "' failed. : " + json.stringify(_failedWidgetTypes[kwargs.type]);
                        deferred.cancel(message);
                        return deferred.promise;
                    }

                    var handle = require.on("error", lang.hitch(this, function (error)
                    {
                        var message = "Error loading module for widget type '" + kwargs.type + "' : " + json.stringify(
                                error);
                        deferred.cancel(message);
                        handle.remove();
                        // The on error handler is not re-thrown for subsequent attempts to reload the same failed module.
                        _failedWidgetTypes[kwargs.type] = error;
                    }));

                    require(["qpid/management/dashboard/widget/" + kwargs.type.toLowerCase()],
                        lang.hitch(this, function (Widget)
                        {
                            handle.remove();

                            var widget = new Widget({
                                controller: this.controller,
                                management: this.management,
                                widgetSettings: kwargs.settings,
                                preference: kwargs.preference,
                                parentObject: kwargs.parentObject
                            });
                            widget.id = kwargs.id;
                            this._widgets.push(widget);
                            var portletPromise = widget.createPortlet();
                            portletPromise.then(lang.hitch(this, function (portlet)
                                {
                                    // Required so we can update the layout in sympathy with drag and drop events.
                                    portlet.widgetId = widget.id;
                                    widget.on("close", lang.hitch(this, function ()
                                    {
                                        var index = this._widgets.indexOf(widget);
                                        if (index != -1)
                                        {
                                            this._widgets.splice(index, 1);
                                        }

                                        this.widgetContainer.removeChild(portlet);
                                        delete this.preference.value.widgets[widget.id];
                                        var position = this.preference.value.layout.column.indexOf(widget.id);
                                        this.preference.value.layout.column.splice(position, 1);
                                        widget.destroy();
                                        this._dashboardChanged();

                                    }));

                                    widget.on("change", lang.hitch(this, function ()
                                    {
                                        this._widgetChanged(widget);
                                    }));

                                    deferred.resolve(widget);
                                }),
                                lang.hitch(this, function (error)
                                {
                                    deferred.cancel(error);
                                    this.management.errorHandler(error);
                                }));
                        }));
                    return deferred.promise;
                },
                _dashboardChanged: function ()
                {
                    this.emit("change", {preference: this.preference});
                },
                _verifyLayout: function ()
                {
                    if (!this.preference.value)
                    {
                        this.preference.value = {widgets: {}, layout: {type: "singleColumn", column: []}};
                    }
                    else if (!this.preference.value.layout || this.preference.value.layout.type !== "singleColumn")
                    {
                        var layout = {type: "singleColumn", column: []};
                        this.preference.value.layout = layout;
                        if (this.preference.value.widgets)
                        {
                            for (var id in this.preference.value.widgets)
                            {
                                layout.column.push(id);
                            }
                        }
                    }
                },
                _loadPreferencesAndRestoreWidgets: function ()
                {
                    if (this.preference.value && this.preference.value.widgets)
                    {
                        var preferencesPromise = this.management.getVisiblePreferences(this.parentObject);
                        preferencesPromise.then(lang.hitch(this, this._unwrapPreferencesAndRestoreWidgets));
                    }
                },
                _unwrapPreferencesAndRestoreWidgets: function (typePreferenceMap)
                {
                    var preferences = {};
                    for (var type in typePreferenceMap)
                    {
                        if (typePreferenceMap.hasOwnProperty(type))
                        {
                            var typePreferences = typePreferenceMap[type];
                            for (var i = 0; i < typePreferences.length; i++)
                            {
                                var preference = typePreferences[i];
                                preferences[preference.id] = preference;
                            }
                        }
                    }
                    this._restoreWidgets(preferences);
                },
                _restoreWidgets: function (preferences)
                {
                    var widgetPromises = [];
                    for (var i = 0; i < this.preference.value.layout.column.length; i++)
                    {
                        var id = this.preference.value.layout.column[i];
                        var widgetSetting = this.preference.value.widgets[id];
                        if (widgetSetting && widgetSetting.preference && widgetSetting.preference.id)
                        {
                            var widgetArgs = null;
                            var preference = preferences[widgetSetting.preference.id];
                            if (preference)
                            {
                                var parentObject = this.structure.findById(preference.associatedObject);
                                if (parentObject)
                                {
                                    widgetArgs = {
                                        preference: preference,
                                        parentObject: parentObject,
                                        settings: widgetSetting,
                                        type: widgetSetting.type,
                                        id: id
                                    };
                                }
                            }

                            if (!widgetArgs)
                            {
                                widgetArgs = {
                                    settings: widgetSetting,
                                    type: "unavailable",
                                    id: id
                                };
                            }

                            widgetPromises.push(this._createWidget(widgetArgs));
                        }
                    }
                    all(widgetPromises)
                        .then(lang.hitch(this,
                            function (widgets)
                            {
                                for (var i = 0; i < widgets.length; i++)
                                {
                                    this._placePortlet(widgets[i].portlet);
                                }
                            }), this.management.errorHandler);
                },
                _placePortlet: function (portlet)
                {
                    this.widgetContainer.addChild(portlet);
                    portlet.startup();
                },
                _widgetChanged: function (widget)
                {
                    this.preference.value.widgets[widget.id] = widget.getSettings();
                    var index = this.preference.value.layout.column.indexOf(widget.id);
                    if (index === -1)
                    {
                        this.preference.value.layout.column.push(widget.id);
                    }
                    this._dashboardChanged();
                },
                _widgetDropped: function (node, targetArea, indexChild)
                {
                    var widgetNodes = this.widgetContainer._grid[0].node.children; // Assumes a 1 column grid
                    if (widgetNodes)
                    {
                        var column = [];
                        for (var i = 0; i < widgetNodes.length; i++)
                        {
                            var widgetNode = widgetNodes[i];
                            if (widgetNode.id)
                            {
                                var portlet = dijit.registry.byId(widgetNode.id);
                                if (portlet.widgetId)
                                {
                                    column.push(portlet.widgetId);
                                }
                            }
                        }
                        this.preference.value.layout.column = column;
                        this._dashboardChanged();
                    }
                }
            });
    });