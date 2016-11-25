package io.elegans.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import scopt.OptionParser
import org.apache.spark.rdd.RDD

object SearchDocumentsWithTFIDF {

  lazy val textProcessingUtils = new TextProcessingUtils

  private case class Params(
    input: String = "SENTENCES_CORPUS",
	  output: String = "TOKENIZED_SENTENCES",
    threshold: Double = 0.3,
    query: String = "test query sentence",
    stopwordsFile: Option[String] = None)

  private def doSearch(params: Params) {
    val conf = new SparkConf().setAppName("SearchSentences")

    val sc = new SparkContext(conf)

    val stopWords = params.stopwordsFile match {
      case Some(stopwordsFile) => sc.broadcast(scala.io.Source.fromFile(stopwordsFile)
        .getLines().map(_.trim).toSet)
      case None => sc.broadcast(Set.empty[String])
    }

    /* (docid, Map((term_string, (term_raw_freq, term_id, term_occurrence_in_docs, term_tfidf)))) */
    val annotatedDocs : RDD[(String, Map[String, Tuple4[Long, Long, Long, Double]])] = sc.objectFile(params.input)

    val query = params.query
    val threshold = params.threshold
    val query_tokens : Broadcast[List[String]] = sc.broadcast(List.empty) //TODO: replace with the tokenized query

    val search_result = annotatedDocs.map( sentence => {
      val doc_id = "xxx" //TODO: replace with the document id
      val weighted_terms : Map[String, Tuple4[Long, Long, Long, Double]] = Map.empty //TODO: replace with map (term -> annotations)
      val query_terms : List[String] = List.empty //TODO: replace with the list of query tokens
      val score : Double = 0.0 //TODO: sum the TFIDF term score for each term of the query which match with the sentence
      (doc_id, score)
    }).filter(_._2 >= threshold).sortBy(_._2, ascending = false)

    search_result.saveAsTextFile(params.output)

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
      opt[String]("output")
        .text(s"the destination directory for the output" +
          s"  default: ${defaultParams.output}")
        .action((x, c) => c.copy(output = x))
      opt[String]("query")
        .text(s"the query" +
          s"  default: ${defaultParams.query}")
        .action((x, c) => c.copy(query = x))
      opt[Double]("threshold")
        .text(s"a cutoff for the score" +
          s"  default: ${defaultParams.threshold}")
        .action((x, c) => c.copy(threshold = x))
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
          s"  default: ${defaultParams.stopwordsFile}")
        .action((x, c) => c.copy(stopwordsFile = Option(x)))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doSearch(params)
      case _ =>
        sys.exit(1)
    }
  }
}
