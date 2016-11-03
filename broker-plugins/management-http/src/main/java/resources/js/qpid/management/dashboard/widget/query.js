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
define(["dojo/_base/declare",
        "dojo/_base/lang",
        "dojo/json",
        "dojo/Deferred",
        "dojo/Evented",
        "dojo/text!dashboard/QueryWidgetSettings.html",
        "qpid/management/query/QueryGrid",
        "qpid/common/util",
        "dojox/html/entities",
        "dojox/widget/Portlet",
        "dojox/widget/PortletDialogSettings",
        "qpid/common/MessageDialog",
        "dojo/query",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/Button",
        "dijit/Toolbar",
        "dijit/Dialog"],
    function (declare,
              lang,
              json,
              Deferred,
              Evented,
              queryWidgetSettingsTemplate,
              QueryGrid,
              util,
              entities,
              Portlet,
              PortletDialogSettings,
              MessageDialog)
    {

        var QueryWidgetSettings = declare("qpid.management.dashboard.QueryWidgetSettings",
            [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
            {
                //Strip out the apache comment header from the template html as comments unsupported.
                templateString: queryWidgetSettingsTemplate.replace(/<!--[\s\S]*?-->/g, ""),

                // template fields
                cancelButton: null,
                applyButton: null,
                refreshPeriod: null,
                limit: null,
                settingsForm: null,

                postCreate: function ()
                {
                    this.inherited(arguments);
                    this.cancelButton.on("click", lang.hitch(this, function ()
                    {
                        this.emit("cancel");
                    }));
                    this.settingsForm.on("submit", lang.hitch(this, function ()
                    {
                        if (this.settingsForm.validate())
                        {
                            var settings = {
                                limit: this.limit.get("value"),
                                refreshPeriod: this.refreshPeriod.get("value")
                            };
                            this.emit("save", {settings: settings});
                        }
                        else
                        {
                            alert('Form contains invalid data.  Please correct first');
                        }
                        return false;
                    }));
                    this.refreshPeriod.on("change", lang.hitch(this, this._onChange));
                    this.limit.on("change", lang.hitch(this, this._onChange));
                },
                _onChange: function ()
                {
                    this.applyButton.set("disabled", !this.settingsForm.validate());
                },
                _setLimitAttr: function (value)
                {
                    this.limit.set("value", value)
                },
                _setRefreshPeriodAttr: function (value)
                {
                    this.refreshPeriod.set("value", value)
                }
            });

        return declare(Evented, {
            _timer : null,
            portlet : null,

            constructor: function (kwargs)
            {
                this.preference = kwargs.preference;
                this.controller = kwargs.controller;
                this.management = kwargs.management;
                this.parentObject = kwargs.parentObject;
                var widgetSettings = kwargs.widgetSettings;
                this.limit = 10;
                if (widgetSettings
                    && widgetSettings.preference
                    && widgetSettings.preference.configuration
                    && widgetSettings.preference.configuration.limit)
                {
                    this.limit = widgetSettings.preference.configuration.limit;
                }
                this.refreshPeriod = widgetSettings && widgetSettings.refreshPeriod ? widgetSettings.refreshPeriod : 10;
                this.hidden = !!(widgetSettings && widgetSettings.hidden);
            },
            createPortlet: function ()
            {
                var preference = this.preference;
                var query = {
                    category: preference.value.category,
                    select: preference.value.select,
                    where: "1=2"
                };

                var deferred = new Deferred();

                var queryPreflightRequestPromise = this.management.query(query);
                queryPreflightRequestPromise.then(lang.hitch(this, function (preflightResults)
                {
                    var headers = lang.clone(preflightResults.headers);
                    var columns = [];
                    for (var i = 0; i < headers.length; i++)
                    {
                        columns.push({
                            field: i,
                            label: headers[i],
                            sortable: false
                        });
                    }

                    var queryGrid = new QueryGrid({
                        detectChanges: true,
                        rowsPerPage: this.limit,
                        management: this.management,
                        parentObject: this.parentObject,
                        category: preference.value.category,
                        selectClause: preference.value.select + ",id",
                        orderBy: preference.value.orderBy,
                        where: preference.value.where,
                        pageSizeOptions: [],
                        previousNextArrows: false,
                        pagingLinks: 0,
                        columns: columns,
                        idProperty: columns.length
                    });

                    queryGrid.on('rowBrowsed', lang.hitch(this, function (event)
                    {
                        this.controller.showById(event.id);
                    }));

                    this._queryGrid = queryGrid;
                    var portlet = new Portlet({
                        title: preference.name,
                        content: queryGrid,
                        open: !this.hidden,
                        onClose: lang.hitch(this, function ()
                        {
                            MessageDialog.confirm({
                                title: "Remove widget?",
                                message: ("Are you sure you want to remove the query widget '"
                                          + entities.encode(new String(preference.name)) + "'"
                                          + " from the dashboard?"),
                                confirmationId: "dashboard.confirmation.widget.delete"
                            })
                                .then(lang.hitch(this, function ()
                                {
                                    this.emit("close");
                                }));
                        }),
                        startup: function ()
                        {
                            this.inherited("startup", arguments);

                            // inherited startup adds the settings icon to the portlet's toolbar but does not expose
                            // a handle to it.

                            var settingsIconNodes = dojo.query(".dashboardWidgetSettingsIcon", this.domNode);
                            if (settingsIconNodes && settingsIconNodes.length == 1)
                            {
                                settingsIconNodes[0].title = "Configure the settings of this widget.";
                                util.stopEventPropagation(settingsIconNodes[0], "mousedown");
                            }
                        }
                    });

                    if (portlet.closeIcon)
                    {
                        portlet.closeIcon.title = "Remove this query from the dashboard.";
                        util.stopEventPropagation(portlet.closeIcon, "mousedown");
                    }

                    if (portlet.arrowNode)
                    {
                        portlet.arrowNode.title = "Maximise/minimise this widget.";
                        util.stopEventPropagation(portlet.arrowNode, "mousedown");
                    }

                    portlet._preferenceAccessIcon = portlet._createIcon("preferenceAccessIcon",
                        "preferenceAccessHoverIcon",
                        lang.hitch(this, function ()
                        {
                            var tabData = {
                                tabType: "query",
                                modelObject: this.parentObject,
                                data: preference,
                                preferenceId: preference.id
                            };
                            this.controller.showTab(tabData);
                        }));
                    portlet._preferenceAccessIcon.title = "Open this query in a separate tab.";
                    util.stopEventPropagation(portlet._preferenceAccessIcon, "mousedown");

                    var settings = new QueryWidgetSettings();
                    settings.set("limit", this.limit);
                    settings.set("refreshPeriod", this.refreshPeriod);

                    // TODO: remove hardcoded sizes
                    var settingsDialog = new PortletDialogSettings({
                        content: settings,
                        dimensions: [540, 130],
                        portletIconClass: "dashboardWidgetSettingsIcon",
                        portletIconHoverClass: "dashboardWidgetSettingsHoverIcon",
                        title: "Widget settings - " + preference.name
                    });
                    settings.on("save", lang.hitch(this, function (event)
                    {
                        var settings = event.settings;
                        var emitChange = this.limit !== settings.limit || this.refreshPeriod !== settings.refreshPeriod;
                        this.limit = settings.limit;
                        this.refreshPeriod = settings.refreshPeriod;
                        this.deactivate();
                        this.activate();
                        queryGrid.set("rowsPerPage", this.limit);
                        settingsDialog.toggle();
                        if (emitChange)
                        {
                            this.emit("change");
                        }
                    }));

                    settings.on("cancel", lang.hitch(settingsDialog, settingsDialog.toggle));

                    portlet.addChild(settingsDialog);
                    portlet.on("hide", lang.hitch(this, function(){
                        this.hidden = true;
                        this.emit("change");
                    }));
                    portlet.on("show", lang.hitch(this, function(){
                        this.hidden = false;
                        this.emit("change");
                    }));

                    this.activate();

                    this.portlet = portlet;
                    deferred.resolve(portlet);

                }), function (error)
                {
                    deferred.cancel(error);
                });

                return deferred.promise;
            },
            destroy : function ()
            {
                if (this.portlet)
                {
                    this.portlet.destroyRecursive();
                }
                this.deactivate();
            },
            getSettings: function ()
            {
                return {
                    type: "query",
                    refreshPeriod: this.refreshPeriod,
                    hidden: this.hidden,
                    preference: {
                        id: this.preference.id,
                        configuration: {
                            limit: this.limit
                        }
                    }
                };
            },
            activate: function ()
            {
                var updateQuery = function ()
                {
                    this._queryGrid.updateData();
                };
                if (this._timer)
                {
                    this.deactivate();
                }
                this._queryGrid.updateData();
                this._timer = setInterval(lang.hitch(this, updateQuery), this.refreshPeriod  * 1000);
            },
            deactivate: function ()
            {
                if (this._timer)
                {
                    clearInterval(this._timer);
                    this._timer = null;
                }
            }
        });

    });