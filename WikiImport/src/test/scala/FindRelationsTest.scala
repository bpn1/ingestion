import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class FindRelationsTest extends FlatSpec with SharedSparkContext with Matchers {
	"Subject relations" should "be found" in {
		val nameMap = TestData.resolvedNameMap()
		val subjects = TestData.testSubjects()
			.map(FindRelations.findRelations(_, nameMap, FindRelations.makeTemplateVersion()))
			.map(subject => (subject.id, subject.name, subject.properties, subject.relations))
		val expectedSubjects = TestData.resolvedTestSubjects()
			.map(subject => (subject.id, subject.name, subject.properties, subject.relations))
		subjects shouldBe expectedSubjects
	}

	"Template version" should "have data only in necessary fields" in {
		val version = FindRelations.makeTemplateVersion()
		version.version should not be null
		version.program should not be null
		version.program should not be empty
		version.value shouldBe empty
		version.validity should not be null
		version.datasources should not be empty
		version.timestamp should not be null
	}

	"Name resolve map" should "contain all resolvable names" in {
		val subjects = sc.parallelize(TestData.testSubjects())
		val resolvedNames = FindRelations.resolvableNamesMap(subjects)
		val expectedMap = TestData.resolvedNameMap()
		resolvedNames shouldBe expectedMap
	}

}
