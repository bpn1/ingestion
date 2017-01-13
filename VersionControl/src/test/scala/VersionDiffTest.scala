import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import java.util.UUID

class VersionDiffTest extends FlatSpec with SharedSparkContext {

    val oldVersion = UUID.fromString("fc2c5a40-c566-11e6-aee2-5f2c06e3b302")
    val newVersion = UUID.fromString("7ce032b0-c567-11e6-8252-5f2c06e3b302")
/*
    "retrieved versions" should "contain exactly two elements" in {
        val versionRDD = subjectVersionRDD()
            .map(VersionDiff.retrieveVersions(_, oldVersion, newVersion))

        val invalidVersions = versionRDD
            .filter { entry =>
                var invalidity = false
                for(element <- entry) {
                    element match {
                        case el : List[Version] =>
                            if(element != null) {
                                if(element.size != 2)
                                    invalidity = true
                            }
                        case el : Map[String, List[Version]] =>
                            for((key, value) <- el) {
                                if(value != null) {
                                    if(value.size != 2)
                                        invalidity = true
                                }
                            }
                        case el : Map[UUID, Map[String, List[Version]]] =>
                            for((key, value) <- el) {
                                for((key2, value2) <- value) {
                                    if(value2 != null) {
                                        if(value2.size != 2)
                                            invalidity = true
                                    }
                                }
                            }
                        case _ => {}
                    }
                }
                invalidity
            }
        assert(invalidVersions.count == 0 && versionRDD.count > 0)
    }

    "retrieved versions" should "contain exactly two elements" in {
        val jsonRDD = subjectVersionRDD()
            .map(VersionDiff.retrieveVersions(_, oldVersion, newVersion))
            .map(VersionDiff.diffToJson)

        val invalidVersions = jsonRDD
            .map(Json.parse(_))

        assert(invalidVersions.count == 0 && versionRDD.count > 0)
    }
*/


    // extracted from WikiData dump
    def subjectVersionRDD(): RDD[String] = {
        sc.parallelize(List(

        ))
    }
}
