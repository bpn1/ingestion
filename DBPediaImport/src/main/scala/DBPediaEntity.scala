case class DBPediaEntity (
	var wikipageId: String = "",
	var dbPediaName: String = "",
	var label: Option[Iterable[String]] = None,
	var description: Option[Iterable[String]] = None,
	var instancetype: Option[Iterable[String]] = None,
	var data: Map[String, Iterable[String]] = Map[String, List[String]]()
)
