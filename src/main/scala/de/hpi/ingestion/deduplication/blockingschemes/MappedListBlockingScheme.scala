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

package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses a list of attributes as key.
  * @param f Function to create customized output values
  */
class MappedListBlockingScheme(f: String => String = identity) extends BlockingScheme {
	tag = "ListBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap(subject.get(_).map(f))
		if(key.nonEmpty) key else List(undefinedValue)
	}
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object MappedListBlockingScheme {

	/**
	  * Returns a Mapped List Blocking Scheme with the given tag, function and attributes.
	  * @param tag tag to use
	  * @param f function to use
	  * @param attrList attribute list to use
	  * @return Mapped List Blocking Schemes with the given properties
	  */
	def apply(tag: String, f: String => String, attrList: String*): MappedListBlockingScheme = {
		val scheme = new MappedListBlockingScheme(f)
		scheme.tag = tag
		scheme.inputAttributes = attrList.toList
		scheme
	}
}
