// get_word_freq.scala

import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import util.matching.Regex.MatchIterator

val alphabet = 'a' to 'z' toArray
def words(text : String) = ("[%s]+" format alphabet.mkString).r.findAllIn(text.toLowerCase)
val textFile = sc.textFile("nltk_test_500.txt")
//  word frequency-   RDD of (String, Int) pairs.
val wds = textFile.flatMap(line => words(line)).map(w => ( w, 1) ).reduceByKey((a, b) => a + b)
wds.map(t => t._2.toString + "," + t._1).collect().foreach(println)

/* this will get all word-token frequency
val wf = textFile.flatMap(line => line.split(" ")).map(w => (w, 1)).reduceByKey((a, b) => a + b)
// print count first, followed by 'word'
wf.map(t => t._2.toString + "," + t._1).collect().foreach(println)
*/
