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

package de.hpi.ingestion.deduplication.models

import java.util.UUID

import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

// scalastyle:off line.size.limit
object TestData {
    def featureEntries(): List[FeatureEntry] = {
        List(
            FeatureEntry(null, null, null, Map("a" -> List(1.0, 2.0)), true),
            FeatureEntry(null, null, null, Map("a" -> List(1.0), "b" -> List(2.0)), false),
            FeatureEntry(null, null, null, Map("a" -> List(0.2, 0.1)), false),
            FeatureEntry(null, null, null, Map("a" -> List(1.0, 2.0), "c" -> Nil), true),
            FeatureEntry(null, null, null, Map("asd" -> List(0.0, 0.1)), false))
    }

    def labeledPoints(): List[LabeledPoint] = {
        List(
            LabeledPoint(1.0, new DenseVector(Array(1.0, 2.0))),
            LabeledPoint(0.0, new DenseVector(Array(1.0, 2.0))),
            LabeledPoint(0.0, new DenseVector(Array(0.2, 0.1))),
            LabeledPoint(1.0, new DenseVector(Array(1.0, 2.0))),
            LabeledPoint(0.0, new DenseVector(Array(0.0, 0.1))))
    }

    def idList: List[UUID] = {
        List(
            UUID.fromString("0195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("1195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("4195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("5195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("6195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("7195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("8195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("9195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("a195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("b195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("c195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("d195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("e195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
            UUID.fromString("f195bc70-f6ba-11e6-aa16-63ef39f49c5d")
        )
    }

    def crossBlocks(): List[Block] = {
        List(
            Block(
                key = "1",
                subjects = List(Subject.master(idList.head), Subject.master(idList(1))),
                staging = List(Subject.master(idList(2)))
            ),
            Block(
                key = "2",
                subjects = List(Subject.master(idList(3)), Subject.master(idList(4))),
                staging = List(Subject.master(idList(5)), Subject.master(idList(6)))
            ),
            Block(
                key = "3",
                subjects = List(Subject.master(idList(7)), Subject.master(idList(8))),
                staging = Nil
            ),
            Block(
                key = "4",
                subjects = Nil,
                staging = List(Subject.master(idList(9)))
            )
        )
    }

    def crossedSubjects(): Set[(Subject, Subject)] = {
        Set(
            (Subject.master(idList.head), Subject.master(idList(2))),
            (Subject.master(idList(1)), Subject.master(idList(2))),
            (Subject.master(idList(3)), Subject.master(idList(5))),
            (Subject.master(idList(3)), Subject.master(idList(6))),
            (Subject.master(idList(4)), Subject.master(idList(5))),
            (Subject.master(idList(4)), Subject.master(idList(6)))
        )
    }

    def filteredCrossedSubjects(): Set[(Subject, Subject)] = {
        Set(
            (Subject.master(idList.head), Subject.master(idList(2))),
            (Subject.master(idList(1)), Subject.master(idList(2))),
            (Subject.master(idList(3)), Subject.master(idList(5))),
            (Subject.master(idList(3)), Subject.master(idList(6)))
        )
    }

    def largeBlocks(): List[(Block, Int)] = {
        List(
            (Block(
                key = "1",
                subjects = idList.take(10).map(Subject.master),
                staging = List(idList(10)).map(Subject.master)
            ), 2),
            (Block(
                key = "2",
                subjects = idList.take(8).map(Subject.master),
                staging = idList.slice(8, 16).map(Subject.master)
            ), 4),
            (Block(
                key = "3",
                subjects = idList.slice(0, 2).map(Subject.master),
                staging = idList.slice(2, 4).map(Subject.master)
            ), 4),
            (Block(
                key = "4",
                subjects = idList.map(Subject.master),
                staging = idList.map(Subject.master)
            ), 0)
        )
    }

    def splitBlocks(): List[List[Block]] = {
        List(
            List(
                Block(null, "1", idList.slice(0, 2).map(Subject.master), List(Subject.master(idList(10)))),
                Block(null, "1", idList.slice(2, 3).map(Subject.master), List(Subject.master(idList(10)))),
                Block(null, "1", idList.slice(3, 5).map(Subject.master), List(Subject.master(idList(10)))),
                Block(null, "1", idList.slice(5, 7).map(Subject.master), List(Subject.master(idList(10)))),
                Block(null, "1", idList.slice(7, 8).map(Subject.master), List(Subject.master(idList(10)))),
                Block(null, "1", idList.slice(8, 10).map(Subject.master), List(Subject.master(idList(10))))
            ),
            List(
                Block(null, "2", idList.slice(0, 2).map(Subject.master), idList.slice(8, 10).map(Subject.master)),
                Block(null, "2", idList.slice(0, 2).map(Subject.master), idList.slice(10, 12).map(Subject.master)),
                Block(null, "2", idList.slice(2, 4).map(Subject.master), idList.slice(8, 10).map(Subject.master)),
                Block(null, "2", idList.slice(2, 4).map(Subject.master), idList.slice(10, 12).map(Subject.master)),
                Block(null, "2", idList.slice(0, 2).map(Subject.master), idList.slice(12, 14).map(Subject.master)),
                Block(null, "2", idList.slice(0, 2).map(Subject.master), idList.slice(14, 16).map(Subject.master)),
                Block(null, "2", idList.slice(2, 4).map(Subject.master), idList.slice(12, 14).map(Subject.master)),
                Block(null, "2", idList.slice(2, 4).map(Subject.master), idList.slice(14, 16).map(Subject.master)),
                Block(null, "2", idList.slice(4, 6).map(Subject.master), idList.slice(8, 10).map(Subject.master)),
                Block(null, "2", idList.slice(4, 6).map(Subject.master), idList.slice(10, 12).map(Subject.master)),
                Block(null, "2", idList.slice(6, 8).map(Subject.master), idList.slice(8, 10).map(Subject.master)),
                Block(null, "2", idList.slice(6, 8).map(Subject.master), idList.slice(10, 12).map(Subject.master)),
                Block(null, "2", idList.slice(4, 6).map(Subject.master), idList.slice(12, 14).map(Subject.master)),
                Block(null, "2", idList.slice(4, 6).map(Subject.master), idList.slice(14, 16).map(Subject.master)),
                Block(null, "2", idList.slice(6, 8).map(Subject.master), idList.slice(12, 14).map(Subject.master)),
                Block(null, "2", idList.slice(6, 8).map(Subject.master), idList.slice(14, 16).map(Subject.master))
            ),
            List(Block(null, "3", idList.slice(0, 2).map(Subject.master), idList.slice(2, 4).map(Subject.master))),
            List(Block(null, "4", idList.map(Subject.master), idList.map(Subject.master)))
        )
    }
}
// scalastyle:on line.size.limit
