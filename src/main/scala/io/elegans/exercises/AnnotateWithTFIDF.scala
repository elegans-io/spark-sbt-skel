package io.elegans.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scopt.OptionParser

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

    val stopWords = sc.broadcast(Set.empty[String]) //TODO: load stopwords

    /* process the lines of the input file, assigning to each line a progressive ID */
    val tokenizedSentences : RDD[Tuple2[String, List[String]]] = sc.parallelize(List.empty) //TODO: complete with tokens

    /* list of documents with word raw frequency annotation (doc_text, frequencies_per_token) */
    val wordsRawFreqInDocuments = tokenizedSentences.map(item => {
      val word_count : Seq[Tuple2[String, Long]] = Seq.empty //TODO: replace value with word count
      val doc = (item._1, word_count)
      doc
    })

    /* list of terms with id and occurrence in documents (term, (term_id, occurrence)) */
    //TODO: replace the value of wordOccurrenceInDocs with the list of terms, term id and num of documents where the term occur
    //TODO: use the function flatMap to prepare the data create (term, 1), then reduceByKey
    //TODO: sort the results by frequency (ascending order) and assign a unique id to each term
    val wordOccurrenceInDocs : RDD[Tuple2[String, Tuple2[Long, Long]]] = sc.parallelize(List.empty)

    /* broadcast the dictionary */
    val dictionary_map : Map[String, Tuple2[Long, Long]] = wordOccurrenceInDocs.collectAsMap().toMap
    val dictionary = sc.broadcast(dictionary_map)
    val dictionary_size = dictionary_map.size

    val num_of_documents = wordsRawFreqInDocuments.count()
    println("Total number of documents: " + num_of_documents)

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
      //TODO: add a function to serialize wordOccurrenceInDocs as a binary object on dictionary_path

      val annotated_documents_path = params.output + "/" + "DOCUMENTS_BIN"
      //TODO: add a function to serialize tfidAnnotatedDocuments as a binary object on annotated_documents_path
    } else {
      val dictionary_path = params.output + "/" + "DICTIONARY"
      //TODO: add a function to serialize wordOccurrenceInDocs in text format on dictionary_path
      wordOccurrenceInDocs.saveAsTextFile(dictionary_path)

      val annotated_documents_path = params.output + "/" + "DOCUMENTS"
      //TODO: add a function to serialize tfidAnnotatedDocuments in text format on annotated_documents_path
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
