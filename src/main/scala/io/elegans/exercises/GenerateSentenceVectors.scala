package io.elegans.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scopt.OptionParser

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector

object GenerateSentenceVectors {

  lazy val textProcessingUtils = new TextProcessingUtils
  lazy val termVectors = new TermVectors

  private case class Params(
    input: String = "TFIDF/DOCUMENTS_BIN",
    dictionary: String = "TFIDF/DICTIONARY_BIN",
	  output: String = "VECTORIZED_SENTENCES",
    binary: Boolean = false)

  private def doGenerate(params: Params) {
    val conf = new SparkConf().setAppName("GenerateSentenceVectors")

    val sc = new SparkContext(conf)

    /* (docid, Map((term_string, (term_raw_freq, term_id, term_occurrence_in_docs, term_tfidf)))) */
    val annotatedDocs : RDD[(String, Map[String, Tuple4[Long, Long, Long, Double]])] = sc.objectFile(params.input)

    /* (term, (term_id, #docs_where_term_occurs)) */
    val dictionaryRDD : RDD[(String, (Long, Long))] = sc.objectFile(params.dictionary)
    val dictionaryMap : Map[String, (Long, Long)] = dictionaryRDD.collectAsMap.toMap
    val dictionary = sc.broadcast(dictionaryMap)

    val docVectors : RDD[(String, SparseVector)] = annotatedDocs.map(doc => {
      val vector : SparseVector = termVectors.generateVector(doc._2, dictionary)
      (doc._1, vector)
    })

    if (params.binary) {
      val output = params.output + "/" + "DOCVECTORS_BIN"
      docVectors.saveAsObjectFile(output)
    } else {
      val output = params.output + "/" + "DOCVECTORS"
      docVectors.saveAsTextFile(output)
    }
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("TokenizeSentences") {
      head("Tokenize a list of sentences with spark")
      help("help").text("prints this usage text")
      opt[String]("input")
        .text(s"the input file or directory with the corpus of sentences" +
          s"  default: ${defaultParams.input}")
        .action((x, c) => c.copy(input = x))
      opt[String]("dictionary")
        .text(s"the dictionary of terms with id and occurrence" +
          s"  default: ${defaultParams.dictionary}")
        .action((x, c) => c.copy(dictionary = x))
      opt[String]("output")
        .text(s"the destination directory for the output" +
          s"  default: ${defaultParams.output}")
        .action((x, c) => c.copy(output = x))
      opt[Unit]("binary")
        .text(s"serialize objects in binary formats" +
          s"  default: ${defaultParams.output}")
        .action((x, c) => c.copy(binary = true))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doGenerate(params)
      case _ =>
        sys.exit(1)
    }
  }
}
