import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import scala.collection.mutable.ListBuffer

class OptionSerializer() extends Serializer[Option[_]] {
	override def read(kryo: Kryo, input: Input, typ: Class[Option[_]]): Option[_] = {
		val mode = input.readInt(true)
		mode match {
			case 1 =>
				val in = kryo.readClassAndObject(input)
				Option(in)
			case 2 =>
				val len = input.readInt(true)
				val in = ListBuffer[Any]()
				for(i <- 0 until len) {
					in += kryo.readClassAndObject(input)
				}
			Option(in.toList)
			case 3 => None
			case _ => None
		}

	}

	override def write(kryo: Kryo, output: Output, value: Option[_]) = {
		value match {
			case Some(a: String) =>
				output.writeInt(1, true)
				kryo.writeClassAndObject(output, a)
			case Some(a: List[_]) =>
				output.writeInt(2, true)
				val len = a.length
				output.writeInt(len, true)
				if(len != 0) {
					val it = a.iterator
					while(it.hasNext) {
						val t = it.next
						kryo.writeClassAndObject(output, t)
					}
				}
			case None =>
				output.writeInt(3, true)
			case _ =>
		}
	}
}
