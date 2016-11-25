package io.elegans.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scopt.OptionParser

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector

object SearchSentenceVectors {

  private case class Params(
    input: String = "TFIDF/DOCUMENTS_BIN",
    dictionary: String = "TFIDF/DICTIONARY_BIN",
	  output: String = "SEARCH_ON_VECTORIZED_SENTENCES",
    threshold: Double = 0.3,
    query: String = "test query sentence",
    stopwordsFile: Option[String] = None,
    binary: Boolean = false)

  private def doSearchVectors(params: Params) {

    //TODO: initialization of SparkConf and SparkContext

    //TODO: loading stopwords

    //TODO: loading of the binary RDD with (<doc_id>, <sparse_vector>), see GenerateSentenceVectors

    //TODO: loading terms dictionary, conver to map and variable broadcasting

    //TODO: create a variable with the count the documents, will be used to create the vector of the query

    //TODO: tokenize the query

    //TODO: create a variable with word count for the query

    //TODO: create a variable for TFIDF of the query (see TextProcessingUtils.getTfIDFAnnotatedVector)

    //TODO: create a vector for the query (see TermVector.generateVector)

    //TODO: map over the vectors of document, and calculate the cosine vector similarity between the vector and the query
    //TODO: filter the results by removing terms with score below the threshold, then sort by rank in descending order

    //TODO: write the results in binary and textual format
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
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
          s"  default: ${defaultParams.stopwordsFile}")
        .action((x, c) => c.copy(stopwordsFile = Option(x)))
      opt[String]("query")
        .text(s"the query" +
          s"  default: ${defaultParams.query}")
        .action((x, c) => c.copy(query = x))
      opt[Double]("threshold")
        .text(s"a cutoff for the score" +
          s"  default: ${defaultParams.threshold}")
        .action((x, c) => c.copy(threshold = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doSearchVectors(params)
      case _ =>
        sys.exit(1)
    }
  }
}
