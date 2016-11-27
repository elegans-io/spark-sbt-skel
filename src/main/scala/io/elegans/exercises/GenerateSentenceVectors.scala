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

    //TODO: load binary object with tokenized documents with TFIDF annotations (AnnotateWithTFIDF)

    /* (term, (term_id, #docs_where_term_occurs)) */
    //TODO: load object file with the RDD containing the terms dictionary (AnnotateWithTFIDF)
    //TODO: convert the RDD dictionary into a Map[String, (Long, Long)]
    //TODO: broadcast the dictionary

    //TODO: generate an RDD[(String, SparseVector)] with the list of (<doc_id>, vector)

    if (params.binary) {
      val output_path = params.output + "/" + "DOCVECTORS_BIN"
      //TODO: save the RDD with vectorized sentence as a binary object in output_path
    } else {
      val output_path = params.output + "/" + "DOCVECTORS"
      //TODO: save the RDD with vectorized sentence as a textFile in output_path
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
