package io.elegans.exercises

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.broadcast.Broadcast

/* import core nlp */
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._

/* these are necessary since core nlp is a java library */
import java.util.Properties
import scala.collection.JavaConversions._

class TextProcessingUtils {

  /** Instantiate a StanfordCoreNLP pipeline
    *
    * @return an instance of StanfordCoreNLP
    */
  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
    pipeline
  }

  /** check if a string is only made by letters
    *
    * @param str the input string
    * @return true if the string is only letters, otherwise false
    */
  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  /** tokenize a string, optionally remove stopwords using StanfordCoreNLP
    *
    * @param text the input text
    * @param stopWords the set with stopwords
    * @param pipeline the instance of StanfordCoreNLP
    * @return a List of lowercase token
    */
  def plainTextToLemmas(text: String, stopWords: Broadcast[Set[String]],
                        pipeline: StanfordCoreNLP): List[String] = {
    val doc: Annotation = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences;
         token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.getString(classOf[LemmaAnnotation])
      val lc_lemma = lemma.toLowerCase
      if (!stopWords.value.contains(lc_lemma) && isOnlyLetters(lc_lemma)) {
        lemmas += lc_lemma.toLowerCase
      }
    }
    lemmas.toList
  }

}
