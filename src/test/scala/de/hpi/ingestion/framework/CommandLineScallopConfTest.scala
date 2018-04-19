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

import java.io.{ByteArrayOutputStream, EOFException}

import org.rogach.scallop.exceptions.Help
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json.{JsObject, Json}

class CommandLineScallopConfTest extends FlatSpec with Matchers {
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
        val conf = new CommandLineScallopConf(args)
        conf.comment() shouldEqual "this_is_a_comment"
        conf.commitJson() shouldEqual "\"{abcde}\""
        conf.config() shouldEqual "test.xml"
        conf.diffVersions() shouldEqual List("version1", "version2")
        conf.importConfig() shouldEqual "normalization.xml"
        conf.restoreVersion() shouldEqual "version1"
        conf.toReduced() shouldBe true
        conf.tokenizer() shouldEqual List("flag1", "flag2", "flag3")
        val conf2 = new CommandLineScallopConf(Seq())
        conf2.toReduced() shouldBe false
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
            "-t", "flag1", "flag2", "flag3",
            "-Fkey1=value1", "key2=value2")
        val conf = new CommandLineScallopConf(args)
        conf.comment() shouldEqual "this_is_a_comment"
        conf.commitJson() shouldEqual "\"{abcde}\""
        conf.config() shouldEqual "test.xml"
        conf.diffVersions() shouldEqual List("version1", "version2")
        conf.importConfig() shouldEqual "normalization.xml"
        conf.restoreVersion() shouldEqual "version1"
        conf.toReduced() shouldBe true
        conf.tokenizer() shouldEqual List("flag1", "flag2", "flag3")
        conf.sentenceEmbeddingFiles shouldEqual Map("key1" -> "value1", "key2" -> "value2")
    }

    "Tokenizer flags" should "be verified" in {
        val args = Array("--tokenizer", "flag1", "flag2", "flag3", "flag4")
        new CommandLineScallopConf(args.take(2))
        new CommandLineScallopConf(args.take(3))
        an [IllegalArgumentException] should be thrownBy new CommandLineScallopConf(args)
    }

    "VersionDiff versions" should "be verified" in {
        val args = Array("--diff-versions", "version1", "version2", "version3")
        an [IllegalArgumentException] should be thrownBy new CommandLineScallopConf(args.take(2))
        new CommandLineScallopConf(args.take(3))
        an [IllegalArgumentException] should be thrownBy new CommandLineScallopConf(args)
    }

    "Errors" should "cause Illegal Argument Exceptions" in {
        val conf = new CommandLineScallopConf(Seq())
        an [IllegalArgumentException] should be thrownBy conf.onError(new EOFException())
        an [IllegalArgumentException] should be thrownBy conf.onError(new NullPointerException())
        an [IllegalArgumentException] should be thrownBy conf.onError(new NumberFormatException())
        Console.withOut(new ByteArrayOutputStream()) {
            an [IllegalArgumentException] should be thrownBy conf.onError(Help(""))
        }
    }

    "Config" should "be transformed into a Command Line Config" in {
        val args = Array(
            "--comment", "this_is_a_comment",
            "--config", "test.xml",
            "--import-config", "normalization.xml",
            "--to-reduced")
        val conf = new CommandLineScallopConf(args).toCommandLineConf
        conf.comment shouldEqual "this_is_a_comment"
        conf.commitJsonOpt shouldBe None
        conf.config shouldEqual "test.xml"
        conf.diffVersionsOpt shouldBe None
        conf.importConfig shouldEqual "normalization.xml"
        conf.restoreVersionOpt shouldBe None
        conf.toReduced shouldBe true
        conf.tokenizerOpt shouldBe None

        val args2 = Array(
            "--commit-json", "{\"abcde\"}",
            "--diff-versions", "version1", "version2",
            "--restore-version", "version1",
            "--tokenizer", "flag1", "flag2", "flag3")
        val conf2 = CommandLineConf(args2)
        conf2.commentOpt shouldBe None
        conf2.commitJson shouldEqual "{\"abcde\"}"
        conf2.configOpt shouldBe None
        conf2.diffVersions shouldEqual List("version1", "version2")
        conf2.importConfigOpt shouldBe None
        conf2.restoreVersion shouldEqual "version1"
        conf2.toReduced shouldBe false
        conf2.tokenizer shouldEqual List("flag1", "flag2", "flag3")

        val args3 = Array("--commit-json", "eyJhYmNkZSJ9")
        val conf3 = CommandLineConf(args3)
        conf3.commitJson shouldEqual "{\"abcde\"}"
    }

    "Help" should "be printed" in {
        val output = new ByteArrayOutputStream()
        Console.withOut(output) {
            an [IllegalArgumentException] should be thrownBy new CommandLineScallopConf(Seq("--help"))
        }
        val printedHelp = output.toString()
        printedHelp should startWith ("Usage: spark.sh ... myJar.jar [OPTION]...\nOptions:\n")
        printedHelp should endWith ("\nFor more information visit the documentation in the GitHub Wiki:\n" +
            "https://github.com/bpn1/ingestion/wiki/Pass-Command-Line-Arguments\n")
    }

    "Commit JSON" should "be parsed" in {
        val args = Array("-j", TestData.commitJson)
        val conf = new CommandLineScallopConf(args)
        val commitJson = Json.parse(conf.commitJson.getOrElse("")).as[JsObject]
        val commitKeys = commitJson.fields.toMap.keySet
        commitKeys shouldEqual Set("created", "updated", "deleted")
    }

    it should "be converted" in {
        val conf = new CommandLineScallopConf(Seq.empty[String])
        val convertedJson = conf.commitConverter(TestData.commitJson)
        convertedJson shouldEqual TestData.commitJson
        val convertedBase64 = conf.commitConverter(TestData.base64Commit)
        convertedBase64 shouldEqual TestData.commitJson
    }
}
