package io.elegans.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scopt.OptionParser

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector

object SearchSentenceVectors {

  lazy val tokenizeSentence = new TextProcessingUtils
  lazy val tfIdf = new TFIDF
  lazy val termVectors = new TermVectors

  private case class Params(
    input: String = "TFIDF/DOCUMENTS_BIN",
    dictionary: String = "TFIDF/DICTIONARY_BIN",
	  output: String = "SEARCH_ON_VECTORIZED_SENTENCES",
    threshold: Double = 0.3,
    query: String = "test query sentence",
    stopwordsFile: Option[String] = None,
    binary: Boolean = false)

  private def doSearchVectors(params: Params) {
    val conf = new SparkConf().setAppName("SearchOnSentenceVectors")

    val sc = new SparkContext(conf)

    val stopWords = params.stopwordsFile match {
      case Some(stopwordsFile) => sc.broadcast(scala.io.Source.fromFile(stopwordsFile)
        .getLines().map(_.trim).toSet)
      case None => sc.broadcast(Set.empty[String])
    }

    /* (docid, sentence_vector) */
    val annotatedDocs : RDD[(String, SparseVector)] = sc.objectFile(params.input)

    /* (term, (term_id, #docs_where_term_occurs)) */
    val dictionaryRDD : RDD[(String, (Long, Long))] = sc.objectFile(params.dictionary)
    val dictionaryMap : Map[String, (Long, Long)] = dictionaryRDD.collectAsMap.toMap
    val dictionary = sc.broadcast(dictionaryMap)

    val num_of_documents : Long = annotatedDocs.count()

    val query = params.query
    val threshold = params.threshold
    val query_tokens = tokenizeSentence.tokenizeSentence(query, stopWords)
    val query_word_count = query_tokens.groupBy(identity).mapValues(_.length : Long)

    val query_tfidf_annotated_terms = tfIdf.getTfIDFAnnotatedVector(query_word_count, dictionary, num_of_documents)

    val query_vector : SparseVector = termVectors.generateVector(doc_tfifd_terms = query_tfidf_annotated_terms,
      dictionary = dictionary, stopWords = stopWords)

    val results = annotatedDocs.map(doc => {
      val cosine : Double = termVectors.cosineSimilarity(doc._2, query_vector)
      (doc._1, cosine)
    }).filter(_._2 > threshold).sortBy(_._2, ascending = false)

    results.saveAsTextFile(params.output)
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
