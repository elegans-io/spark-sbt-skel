package io.elegans.exercises

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scopt.OptionParser

import org.apache.spark.rdd.RDD

/** Read a list of sentences and generate a list of token */
object TokenizeSentences {

  lazy val textProcessingUtils = new TextProcessingUtils /* lazy initialization of TextProcessingUtils class */

  /** Case class for command line variables with default values
    *
    * @param input the input data
    * @param output the path for output data
    * @param stopwordsFile optionally, a file containing stopwords
    */
  private case class Params(
    input: String = "sentences.txt",
	  output: String = "TOKENIZED_SENTENCES",
    stopwordsFile: Option[String] = None)

  /** Do all the spark initialization, the logic of the program is here
    *
    * @param params the command line parameters
    */
  private def doTokenization(params: Params) {
    val conf = new SparkConf().setAppName("SentenceTokenizer") /* initialize the spark configuration */

    val sc = new SparkContext(conf) /* initialize the spark context passing the configuration object */

    val inputfile = sc.textFile(params.input).map(_.trim) /* read the input file in textual format */

    val stopWords = params.stopwordsFile match {  /* check the stopWord variale */
      case Some(stopwordsFile) => sc.broadcast(scala.io.Source.fromFile(stopwordsFile) /* load the stopWords if Option
                                                variable contains an existing value */
        .getLines().map(_.trim).toSet)
      case None => sc.broadcast(Set.empty[String]) /* set an empty string if Option variable is None */
    }

    /* process the lines of the input file, assigning to each line a progressive ID*/
    val tokenizedSentences : RDD[Tuple2[String, List[String]]] = inputfile.zipWithIndex.map( line => {
      val original_string = line._1 /* the original string as is on file */
      val id = line._2.toString /* the unique sentence id converted to string */
      val token_list = textProcessingUtils.tokenizeSentence(original_string, stopWords)
      (id, token_list)
    })

    tokenizedSentences.saveAsTextFile(params.output) /* write the output in plain text format */
  }

  /** The main function, this function must be given in order to run a stand alone program
    *
    * @param args will contain the command line arguments
    */
  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("TokenizeSentences") { /* initialization of the parser */
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

    parser.parse(args, defaultParams) match { /* parsing of the command line options */
      case Some(params) =>
        doTokenization(params)
      case _ =>
        sys.exit(1)
    }
  }
}
