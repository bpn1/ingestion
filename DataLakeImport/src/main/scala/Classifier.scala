package DataLake

abstract class Classifier {
	def execute(subject1: Subject, subject2: Subject): Double
}
