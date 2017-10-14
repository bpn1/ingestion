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

package de.hpi.ingestion.framework

import java.io.EOFException

import org.scalatest.{FlatSpec, Matchers}

class CommandLineConfTest extends FlatSpec with Matchers {
	"Options" should "be set by word flags" in {
		val args = Array(
			"--comment", "this_is_a_comment",
			"--commit-json", "\"{abcde}\"",
			"--config", "test.xml",
			"--diff-versions", "version1", "version2",
			"--import-config", "normalization.xml",
			"--restore-version", "version1",
			"--to-reduced",
			"--tokenizer", "flag1", "flag2", "flag3")
		val conf = new CommandLineConf(args)
		conf.comment() shouldEqual "this_is_a_comment"
		conf.commitJson() shouldEqual "\"{abcde}\""
		conf.config() shouldEqual "test.xml"
		conf.diffVersions() shouldEqual List("version1", "version2")
		conf.importConfig() shouldEqual "normalization.xml"
		conf.restoreVersion() shouldEqual "version1"
		conf.toReduced() shouldBe true
		conf.tokenizer() shouldEqual List("flag1", "flag2", "flag3")
	}

	they should "be set by character flags" in {
		val args = Array(
			"-b", "this_is_a_comment",
			"-j", "\"{abcde}\"",
			"-c", "test.xml",
			"-d", "version1", "version2",
			"-i", "normalization.xml",
			"-v", "version1",
			"-r",
			"-t", "flag1", "flag2", "flag3")
		val conf = new CommandLineConf(args)
		conf.comment() shouldEqual "this_is_a_comment"
		conf.commitJson() shouldEqual "\"{abcde}\""
		conf.config() shouldEqual "test.xml"
		conf.diffVersions() shouldEqual List("version1", "version2")
		conf.importConfig() shouldEqual "normalization.xml"
		conf.restoreVersion() shouldEqual "version1"
		conf.toReduced() shouldBe true
		conf.tokenizer() shouldEqual List("flag1", "flag2", "flag3")
	}

	"Tokenizer flags" should "be verified" in {
		val args = Array("--tokenizer", "flag1", "flag2", "flag3", "flag4")
		new CommandLineConf(args.take(2))
		new CommandLineConf(args.take(3))
		an [IllegalArgumentException] should be thrownBy new CommandLineConf(args)
	}

	"VersionDiff versions" should "be verified" in {
		val args = Array("--diff-versions", "version1", "version2", "version3")
		an [IllegalArgumentException] should be thrownBy new CommandLineConf(args.take(2))
		new CommandLineConf(args.take(3))
		an [IllegalArgumentException] should be thrownBy new CommandLineConf(args)
	}

	"Errors" should "cause Illegal Argument Exceptions" in {
		val conf = new CommandLineConf(Seq())
		an [IllegalArgumentException] should be thrownBy conf.onError(new EOFException())
		an [IllegalArgumentException] should be thrownBy conf.onError(new NullPointerException())
		an [IllegalArgumentException] should be thrownBy conf.onError(new NumberFormatException())
	}
}
