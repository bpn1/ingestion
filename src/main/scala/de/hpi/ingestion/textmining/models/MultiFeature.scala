/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.models

/**
  * Case class representing value for a feature with its respective values for the second order features.
  *
  * @param value           original value for the feature
  * @param rank            rank of the original value (1 for the highest value)
  * @param delta_top       absolute difference to the highest value (Double.PositiveInfinity for the highest value)
  * @param delta_successor absolute difference to the successive (next smaller) value
  *                        (Double.PositiveInfinity for the smallest value)
  */
case class MultiFeature(
	value: Double,
	var rank: Int = 1,
	var delta_top: Double = Double.PositiveInfinity,		// NaN and negative values cannot be used to train the
	var delta_successor: Double = Double.PositiveInfinity	// Naive Bayes model
)
