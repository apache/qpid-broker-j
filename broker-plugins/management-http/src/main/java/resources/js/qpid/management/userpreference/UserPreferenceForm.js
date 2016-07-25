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
        "dojo/text!userpreference/UserPreferenceForm.html",
        "dojo/Evented",
        "qpid/common/TimeZoneSelector",
        "dijit/form/Form",
        "dijit/form/Button",
        "dijit/form/NumberSpinner",
        "dijit/_WidgetBase",
        "dijit/_TemplatedMixin",
        "dijit/_WidgetsInTemplateMixin",
        "dojox/validate/us",
        "dojox/validate/web",
        "dojo/domReady!"], function (declare, lang, template, Evented, TimeZoneSelector)
{

    return declare("qpid.management.userpreference.UserPreferenceForm",
        [dijit._WidgetBase, dijit._TemplatedMixin, dijit._WidgetsInTemplateMixin, Evented],
        {
            /**
             * dijit._TemplatedMixin enforced fields
             */
            //Strip out the apache comment header from the template html as comments unsupported.
            templateString: template.replace(/<!--[\s\S]*?-->/g, ""),

            /**
             * template attach points
             */
            preferenceForm: null,
            timeZoneSelector: null,
            updatePeriod: null,
            cancelButton: null,
            saveButton: null,

            /**
             * inner fields
             */
            _changedPreferences: null,
            _timeZone: null,
            _updatePeriod: null,

            postCreate: function ()
            {
                this.inherited(arguments);
                this._postCreate();
            },
            _postCreate: function ()
            {
                this._changedPreferences = {};
                this.cancelButton.on("click", lang.hitch(this, this._onCancel));
                this.preferenceForm.on("submit", lang.hitch(this, this._onFormSubmit));
                this.timeZoneSelector.on("change", lang.hitch(this, this._onTimeZoneChange));
                this.updatePeriod.on("change", lang.hitch(this, this._onUpdatePeriodChange));
                this.saveButton.set("disabled", true);
            },
            _onCancel: function ()
            {
                this.emit("cancel");
            },
            _setTimezonesAttr: function (timezones)
            {
                this.timeZoneSelector.set("timezones", timezones);
            },
            _setTimezoneAttr: function (timezone)
            {
                this.timeZoneSelector.set("value", timezone);
                this._timeZone = timezone;
            },
            _setUpdatePeriodAttr: function (updatePeriod, priority)
            {
                this.updatePeriod.set("value", updatePeriod, priority);
                this._updatePeriod = updatePeriod;
            },
            _onTimeZoneChange: function (value)
            {
                this._changedPreferences.timeZone = value;
                this._onChange();
            },
            _onUpdatePeriodChange: function (value)
            {
                this._changedPreferences.updatePeriod = value;
                this._onChange();
            },
            _onChange: function ()
            {
                this.saveButton.set("disabled",
                    this._updatePeriod == this.updatePeriod.value && this._timeZone == this.timeZoneSelector.value);
            },
            _onFormSubmit: function ()
            {
                if (this.preferenceForm.validate())
                {
                    try
                    {
                        var preferences = lang.clone(this._changedPreferences);
                        this.emit("save", {preferences: preferences});
                        this._changedPreferences = {};
                    }
                    catch(e)
                    {
                        alert('Save failed:' + e);
                    }
                }
                else
                {
                    alert('Form contains invalid data.  Please correct first');
                }
                return false;
            }
        });
});
