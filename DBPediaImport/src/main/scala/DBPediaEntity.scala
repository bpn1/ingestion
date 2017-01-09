/**
	* Created by Lando on 20.12.16.
	*/
case class DBPediaEntity (
	var wikipageid: String = "null",
	var dbpedianame: String = "null",
	var label: Option[Iterable[String]] = None,
	var description: Option[Iterable[String]] = None,
	var instancetype: Option[Iterable[String]] = None,
	var data: Map[String, Iterable[String]] = Map[String, List[String]]()
)