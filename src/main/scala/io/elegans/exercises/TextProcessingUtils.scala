package io.elegans.exercises

import scala.collection.mutable.ArrayBuffer

/* import core nlp */
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._

/* these are necessary since core nlp is a java library */
import java.util.Properties
import scala.collection.JavaConversions._

class TextProcessingUtils {

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
    pipeline
  }

  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  def plainTextToLemmas(text: String, stopWords: Set[String],
                        pipeline: StanfordCoreNLP): List[String] = {
    val doc: Annotation = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences;
         token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.getString(classOf[LemmaAnnotation])
      val lc_lemma = lemma.toLowerCase
      if (!stopWords.contains(lc_lemma) && isOnlyLetters(lc_lemma)) {
        lemmas += lc_lemma.toLowerCase
      }
    }
    lemmas.toList
  }

}
