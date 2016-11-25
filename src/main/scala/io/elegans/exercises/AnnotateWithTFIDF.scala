package io.elegans.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scopt.OptionParser

import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object AnnotateWithTFIDF {

  lazy val textProcessingUtils = new TextProcessingUtils
  lazy val tfIdf = new TFIDF

  private case class Params(
    input: String = "sentences.txt",
	  output: String = "TOKENIZED_SENTENCES",
    binary: Boolean = false,
    stopwordsFile: Option[String] = None)

  private def doTFIDFAnnotation(params: Params) {
    val conf = new SparkConf().setAppName("AnnotateWithTFIDF")

    val sc = new SparkContext(conf)

    val inputfile = sc.textFile(params.input).map(_.trim)

    val stopWords = params.stopwordsFile match {
      case Some(stopwordsFile) => sc.broadcast(scala.io.Source.fromFile(stopwordsFile)
        .getLines().map(_.trim).toSet)
      case None => sc.broadcast(Set.empty[String])
    }

    /* process the lines of the input file, assigning to each line a progressive ID*/
    val tokenizedSentences : RDD[Tuple2[String, List[String]]] = inputfile.zipWithIndex.map( line => {
      val original_string = line._1 /* the original string as is on file */
      val id = line._2.toString /* the unique sentence id converted to string */
      val token_list = textProcessingUtils.tokenizeSentence(original_string, stopWords)
      (id, token_list)
    })

    /* list of documents with word raw frequency annotation (doc_text, frequencies_per_token) */
    val wordsRawFreqInDocuments = tokenizedSentences.map(item => {
      val word_count = item._2.groupBy(identity).mapValues(_.length : Long).toSeq
      val doc = (item._1, word_count)
      doc
    })

    /* list of terms with id and occurrence in documents (term, (term_id, occurrence)) */
    val wordOccurrenceInDocs : RDD[Tuple2[String, Tuple2[Long, Long]]] = wordsRawFreqInDocuments.map(_._2)
      .flatMap(s => s.map(x => (x._1, 1 : Long))) // apply a function (like map) then flatten
      .reduceByKey((a, b) => {a + b}).sortByKey(ascending = true).zipWithIndex.map(x => {(x._1._1, (x._2, x._1._2))})

    /* broadcast the dictionary */
    val dictionary_map : Map[String, Tuple2[Long, Long]] = wordOccurrenceInDocs.collectAsMap().toMap
    val dictionary = sc.broadcast(dictionary_map)
    val dictionary_size = dictionary_map.size

    val num_of_documents = wordsRawFreqInDocuments.count()
    println("Number of documents: " + num_of_documents)

    /* annotate documents with tf-idf
      in: (doc_id, frequencies_per_token)
      out: (docid, Map(term_string, (term_raw_freq, term_id, term_occurrence_in_docs, term_tfidf)))
    */
    val tfidAnnotatedDocuments = wordsRawFreqInDocuments.mapValues(annotations => {
      val query_tfidf_annotated_terms = tfIdf.getTfIDFAnnotatedVector(annotations.toMap, dictionary, num_of_documents)
      query_tfidf_annotated_terms
    })

    if (params.binary) {
      val dictionary_path = params.output + "/" + "DICTIONARY_BIN"
      wordOccurrenceInDocs.saveAsObjectFile(dictionary_path)

      val annotated_documents_path = params.output + "/" + "DOCUMENTS_BIN"
      tfidAnnotatedDocuments.saveAsObjectFile(annotated_documents_path)
    } else {
      val dictionary_path = params.output + "/" + "DICTIONARY"
      wordOccurrenceInDocs.saveAsTextFile(dictionary_path)

      val annotated_documents_path = params.output + "/" + "DOCUMENTS"
      tfidAnnotatedDocuments.saveAsTextFile(annotated_documents_path)
    }
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("TokenizeSentences") {
      head("Tokenize a list of sentences with spark")
      help("help").text("prints this usage text")
      opt[String]("input")
        .text(s"the input file or directory with input text" +
          s"  default: ${defaultParams.input}")
        .action((x, c) => c.copy(input = x))
      opt[String]("output")
        .text(s"the destination directory for the output" +
          s"  default: ${defaultParams.output}")
        .action((x, c) => c.copy(output = x))
      opt[Unit]("binary")
        .text(s"serialize objects in binary formats" +
          s"  default: ${defaultParams.output}")
        .action((x, c) => c.copy(binary = true))
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
          s"  default: ${defaultParams.stopwordsFile}")
        .action((x, c) => c.copy(stopwordsFile = Option(x)))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doTFIDFAnnotation(params)
      case _ =>
        sys.exit(1)
    }
  }
}
