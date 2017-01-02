/**
	* Created by Lando on 20.12.16.
	*/
case class DBPediaEntity (
	var wikipageId: String = "null",
	var dbPediaName: String = "null",
	var label: Option[Iterable[String]] = None,
	var description: Option[Iterable[String]] = None,
	var data: Map[String, Iterable[String]] = Map[String, List[String]]()
)