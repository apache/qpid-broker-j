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
        "dojo/dom-construct",
        "dijit/_WidgetBase",
        "dojo/domReady!"],
    function (declare, lang, domConstruct)
    {

        return declare("qpid.common.HexDumpWidget", [dijit._WidgetBase], {
            /**
             * constructor fields
             */
            data: null,
            numberOfColumns: 8,

            postCreate: function ()
            {
                this.inherited(arguments);
                var rows = Math.floor(this.data.length / this.numberOfColumns) + (this.data.length % this.numberOfColumns == 0 ? 0 : 1);

                var hexDumpBox = domConstruct.create("div", {class: "hexDumpBox"}, this.domNode);
                var hexBox = domConstruct.create("span", {class: "hexBox"}, hexDumpBox);
                var asciiBox = domConstruct.create("span", {class: "asciiBox"}, hexDumpBox);

                this._createHeadings(hexBox, asciiBox);
                this._createRows(rows, hexBox, asciiBox);
            },

            _createHeadings: function (hexBox, asciiBox)
            {
                var hexHeadRowDom = domConstruct.create("div", {class: "hexDumpHeadRow"}, hexBox);
                var asciiHeadRowDom = domConstruct.create("div", {class: "hexDumpHeadRow"}, asciiBox);
                domConstruct.create("span", {class: "hexCountCell"}, hexHeadRowDom);

                for (var column = 0; column < this.numberOfColumns; column++)
                {
                    var hexHeadCellDom = domConstruct.create("span", {class: "hexDumpCell"}, hexHeadRowDom);
                    hexHeadCellDom.innerHTML = this._toHex(column, 2);

                    var asciiHeadCellDom = domConstruct.create("span", {class: "hexDumpCell"}, asciiHeadRowDom);
                    asciiHeadCellDom.innerHTML = this._toHex(column, 1);
                }
            },

            _createRows: function (rows, hexBox, asciiBox)
            {
                for (var row = 0; row < rows; row++)
                {
                    var hexRowDom = domConstruct.create("div", {class: "hexDumpRow"}, hexBox);
                    var asciiRowDom = domConstruct.create("div", {class: "hexDumpRow"}, asciiBox);

                    var hexCountCellDom = domConstruct.create("span", {class: "hexCountCell"}, hexRowDom);
                    hexCountCellDom.innerHTML = this._toHex(row * this.numberOfColumns, 4);

                    for (var column = 0; column < this.numberOfColumns; column++)
                    {
                        var dataIndex = (row * this.numberOfColumns) + column;
                        if (dataIndex >= this.data.length)
                        {
                            break;
                        }
                        var item = this.data[dataIndex];
                        var hexCellDom = domConstruct.create("span", {class: "hexDumpCell"}, hexRowDom);
                        hexCellDom.innerHTML = this._toHex(item, 2);

                        var asciiCellDom = domConstruct.create("span", {class: "hexDumpCell"}, asciiRowDom);
                        asciiCellDom.innerHTML = this._toAsciiPrintable(item);

                        asciiCellDom.cousin = hexCellDom;
                        hexCellDom.cousin = asciiCellDom;

                        var mouseOverListener = function (event)
                        {
                            // highlight the mouseover target
                            event.target.classList.add("hexDumpCellHighlight");
                            event.target.cousin.classList.add("hexDumpCellHighlight");
                        };
                        var mouseLeaveListener = function (event)
                        {
                            event.target.classList.remove("hexDumpCellHighlight");
                            event.target.cousin.classList.remove("hexDumpCellHighlight");
                        };

                        hexCellDom.addEventListener("mouseover", mouseOverListener);
                        asciiCellDom.addEventListener("mouseover", mouseOverListener);
                        hexCellDom.addEventListener("mouseleave", mouseLeaveListener);
                        asciiCellDom.addEventListener("mouseleave", mouseLeaveListener);
                    }
                }
            },

            _toAsciiPrintable: function (c)
            {
                if (c <= 32 || c >= 127)
                {
                    return ".";
                }
                else
                {
                    return String.fromCharCode(c);
                }
            },

            _toHex: function (d, pad)
            {
                var hex = Number(d & 0xFF).toString(16);

                while (hex.length < pad)
                {
                    hex = "0" + hex;
                }

                return hex;
            }
        });
    });
