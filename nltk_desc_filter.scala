// nltk_desc_filter.scala
import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

import util.matching.Regex.MatchIterator

val alphabet = 'a' to 'z' toArray
def words(text : String) = ("[%s./]+" format alphabet.mkString).r.findAllIn(text.toLowerCase)
val bcw = sc.broadcast(sc.textFile("combined.txt").flatMap(line => words(line)).collect())

val textFile = sc.textFile("nltk_test.txt")
// wf word frequency-   RDD of (String, Int) pairs.
// remove beginning punct. then ending punct.
val wf = textFile.flatMap(line => words(line)).map(w => w.replaceAll("""^[\p{Punct}]""", "").replaceAll("""[\p{Punct}]$""", ""))
val wc = wf.map(w => ( w, 1)).reduceByKey((a, b) => a + b)
wc.filter(t => !(bcw.value.contains(t._1)) && t._1.length() > 1).map(t => t._2.toString + "," + t._1).coalesce(1).saveAsTextFile("nltk_test_flt_scala")

