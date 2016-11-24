package io.elegans.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scopt.OptionParser

import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object TokenizeSentences {

  lazy val textProcessingPipeline = new TextProcessingUtils

  private case class Params(
    input: String = "sentences.txt",
	  output: String = "TOKENIZED_SENTENCES",
    stopwordsFile: Option[String] = None)

  private def doTokenization(params: Params) {
    val conf = new SparkConf().setAppName("SentenceTokenizer")

    val sc = new SparkContext(conf)

    val inputfile = sc.textFile(params.input).map(_.trim)

    val stopWords = params.stopwordsFile match {
      case Some(stopwordsFile) => sc.broadcast(scala.io.Source.fromFile(stopwordsFile)
        .getLines().map(_.trim).toSet)
      case None => sc.broadcast(Set.empty)
    }

    val tokenizedSentences : RDD[Tuple2[String, List[String]]]= inputfile.zipWithIndex.map( line => {
      val s = line._1
      val id = line._2.toString
      try {
        val pipeline = textProcessingPipeline.createNLPPipeline()
        val doc_lemmas : Tuple2[String, List[String]] =
          (id, textProcessingPipeline.plainTextToLemmas(s, stopWords.value.toSet, pipeline))
        doc_lemmas
      } catch {
        case e: Exception => Tuple2(id, List.empty[String])
      }
    })

    tokenizedSentences.saveAsTextFile(params.output)
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
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
          s"  default: ${defaultParams.stopwordsFile}")
        .action((x, c) => c.copy(stopwordsFile = Option(x)))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doTokenization(params)
      case _ =>
        sys.exit(1)
    }
  }
}
