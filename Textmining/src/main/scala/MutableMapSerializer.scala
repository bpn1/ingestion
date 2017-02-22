import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import scala.collection.mutable

class MutableMapSerializer() extends Serializer[mutable.Map[_, _]] {
	override def read(
		kryo: Kryo,
		input: Input,
		typ: Class[mutable.Map[_, _]]
	): mutable.Map[_, _] =
	{
		val len = input.readInt(true)
		val coll = mutable.Map[Any, Any]()
		if (len != 0) {
			var i = 0
			while (i < len) {
				coll(kryo.readClassAndObject(input)) = kryo.readClassAndObject(input)
				i += 1
			}
		}
		coll
	}

	override def write(kryo: Kryo, output: Output, collection: mutable.Map[_, _]) = {
		val len = collection.size
		output.writeInt(len, true)
		if (len != 0) {
			val it = collection.iterator
			while (it.hasNext) {
				val t = it.next
				kryo.writeClassAndObject(output, t._1)
				kryo.writeClassAndObject(output, t._2)
			}
		}
	}
}
