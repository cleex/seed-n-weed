#!/usr/bin/env scala

import util.matching.Regex.MatchIterator

// command line:  test_spelling.scala
var chkList = List[String]()
args.foreach(a => chkList = a :: chkList )
// remove last elem from chkList
// chkList = chkList.dropRight(1)
// chkList foreach println

val alphabet = 'a' to 'z' toArray
def train(features : MatchIterator) = (Map[String, Int]() /: features)((m, f) => m + ((f, m.getOrElse(f, 0) + 1)))
def words(text : String) = ("[%s]+" format alphabet.mkString).r.findAllIn(text.toLowerCase)
val dict = train(words( io.Source.fromFile("big.txt").mkString))
 
def edits(s : Seq[(String, String)]) = (for((a,b) <- s; if b.length > 0) yield a + b.substring(1)) ++
  (for((a,b) <- s; if b.length > 1) yield a + b(1) + b(0) + b.substring(2)) ++
  (for((a,b) <- s; c <- alphabet if b.length > 0) yield a + c + b.substring(1)) ++
  (for((a,b) <- s; c <- alphabet) yield a + c + b)
 
def edits1(word : String) = edits(for(i <- 0 to word.length) yield (word take i, word drop i))
def edits2(word : String) = for(e1 <- edits1(word); e2 <-edits1(e1)) yield e2
def known(words : Seq[String]) = for(w <- words; found <- dict.get(w)) yield w
def or[T](candidates : Seq[T], other : => Seq[T]) = if(candidates.isEmpty) other else candidates
 
def candidates(word: String) = or(known(List(word)), or(known(edits1(word)), known(edits2(word))))
 
def correct(word : String) = ((-1, word) /: candidates(word))(
  (max, word) => if(dict(word) > max._1) (dict(word), word) else max)._2
 
//List("osters", "musters", "mixters", "chsee", "school") map correct foreach println 
chkList map correct foreach println 
