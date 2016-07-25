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
        "dojo/Evented",
        "dojo/store/Memory",
        "dojo/text!common/TimeZoneSelector.html",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dijit/form/FilteringSelect",
        "dijit/form/CheckBox",
        "dojox/validate/us",
        "dojox/validate/web",
        "dojo/domReady!"],
    function (declare, lang, Evented, Memory, template)
    {

        var preferencesRegions = ["Africa",
                                  "America",
                                  "Antarctica",
                                  "Arctic",
                                  "Asia",
                                  "Atlantic",
                                  "Australia",
                                  "Europe",
                                  "Indian",
                                  "Pacific"];

        function initSupportedRegions()
        {
            var supportedRegions = [];
            for (var j = 0; j < preferencesRegions.length; j++)
            {
                supportedRegions.push({
                    id: preferencesRegions[j],
                    name: preferencesRegions[j]
                });
            }
            return supportedRegions;
        }

        return declare("qpid.common.TimeZoneSelector", [dijit._WidgetBase,
                                                        dijit._TemplatedMixin,
                                                        dijit._WidgetsInTemplateMixin,
                                                        Evented], {
            templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

            /**
             * template fields
             */
            value: null,
            regionSelector: null,
            citySelector: null,
            utcSelector: null,

            /**
             * internal fields
             */
            _supportedRegions: null,

            postCreate: function ()
            {
                this.inherited(arguments);

                this.citySelector.set("searchAttr", "city");
                this.citySelector.set("query", {region: /.*/});
                this.citySelector.set("labelAttr", "city");
                this._supportedRegions = initSupportedRegions();
                this.regionSelector.set("store", new Memory({data: this._supportedRegions}));

                this.utcSelector.on("change", lang.hitch(this, function (value)
                {
                    var checked = this.utcSelector.get("checked");
                    this.citySelector.set("disabled", checked);
                    this.regionSelector.set("disabled", checked);
                    if (checked)
                    {
                        this.value = "UTC";
                        this.regionSelector.reset();
                        this.citySelector.reset();
                        this._handleOnChange(this.value);
                    }
                    else
                    {
                        this._defaultRegionAndCity();
                    }

                }));
                this.regionSelector.on("change", lang.hitch(this, function (value)
                {
                    if (value)
                    {
                        this.citySelector.set("disabled", false);
                        this.citySelector.query.region = value;
                        var store = this.citySelector.get("store");
                        var cities = store.query({region: value});
                        this.citySelector.set("value", cities[0].id, true);
                    }
                }));

                this.citySelector.on("change", lang.hitch(this, function (value)
                {
                    if (value)
                    {
                        this.value = value;
                        this._handleOnChange(value);
                    }
                }));
            },

            _defaultRegionAndCity: function ()
            {
                var firstRegion = this._supportedRegions[0];
                this.regionSelector.set("value", firstRegion.id, false);
                var store = this.citySelector.get("store");
                var cities = store.query({region: firstRegion.id});
                this.citySelector.set("value", cities[0].id, true);
            },

            _setTimezonesAttr: function (supportedTimeZones)
            {
                this.citySelector.set("store", new Memory({data: supportedTimeZones}));
            },

            _setValueAttr: function (value)
            {
                this._lastValueReported = value;
                if (value)
                {
                    if (value == "UTC")
                    {
                        this.utcSelector.set("checked", true);
                    }
                    else
                    {
                        this.utcSelector.set("checked", false);
                        var elements = value.split("/");
                        if (elements.length > 1)
                        {
                            this.regionSelector.timeZone = value;
                            this.regionSelector.set("value", elements[0], false);
                            this.citySelector.set("value", value, true);
                        }
                        else
                        {
                            this._defaultRegionAndCity();
                        }
                    }
                }
                else
                {
                    this._defaultRegionAndCity();
                }
            },

            _handleOnChange: function (newValue)
            {
                if (this._lastValueReported != newValue)
                {
                    this._lastValueReported = newValue;
                    this.emit("change", newValue);
                }
            }

        });
    });