/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 
define(function(require){
	'use strict';	

	var XABaseCollection	= require('collections/XABaseCollection');
	var XAGlobals			= require('utils/XAGlobals');
	var RangerPolicyResource		= require('models/RangerPolicyResource');

	var RangerPolicyResourceListBase = XABaseCollection.extend(
	/** @lends RangerPolicyResourceListBase.prototype */
	{
//		url: XAGlobals.baseURL + 'plugins/policies',

		model : RangerPolicyResource,

		/**
		 * RangerPolicyListBase initialize method
		 * @augments XABaseCollection
		 * @constructs
		 */
		initialize : function() {
			this.modelName = 'RangerPolicyResource';
			this.modelAttrName = 'RangerPolicyResources';
			this.bindErrorEvents();
		},
			/**
		 * override the parseRecords of PageableCollection for our use
		 */
		parseRecords : function(resp, options){
			return resp;
		}
	},{
		// static class members
		/**
		* Table Cols to be passed to Backgrid
		* UI has to use this as base and extend this.
		*
		*/

		tableCols : {}

	});

    return RangerPolicyResourceListBase;
});


