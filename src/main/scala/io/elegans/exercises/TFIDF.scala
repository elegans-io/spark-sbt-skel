package io.elegans.exercises

import org.apache.spark.broadcast.Broadcast

class TFIDF {

  /** Calculate the value of TF-IDF for a term in a document
    *
    *  @param num_of_documents the total number of documents
    *  @param term_raw_freq how many time the term appear in the document
    *  @param max_term_raw_freq the term in the document with the higher raw freq. value
    *  @param num_docs_with_terms the number of documents which contains the term
    *  @return the TF-IDF value
    */
  private def calcTermTfIdf(num_of_documents: Long, term_raw_freq: Long, max_term_raw_freq : Long,
                            num_docs_with_terms: Long): Double = {
    val tf : Double = 0.5 + 0.5 * ((term_raw_freq: Double)/max_term_raw_freq)
    assert(tf > 0)
    val idf : Double = math.log((num_of_documents: Double)/(1 + num_docs_with_terms))
    assert(idf > 0)
    val tfidf : Double = tf * idf
    tfidf
  }

  /** Generate a vector for all terms in a sentence weighted with TF-IDF
    *
    *  @param annotated_sentence a map of terms with raw frequency Map(term -> raw_freq))
    *  @param dictionary a broadcast variable with the complete dictionary of terms:
    *                    (term, (term_unique_id, num_docs_with_terms))
    *  @param num_of_documents the total number of documents (i.e. sentences) in the corpus
    *  @return a tuple containing
    *          (<doc_id>, Map(<term_id> -> (<term_raw_freq>, <term_id>, <num_docs_with_terms>, <term_tfidf>)))
    */
  def getTfIDFAnnotatedVector(annotated_sentence: Map[String, Long],
                              dictionary: => Broadcast[Map[String, (Long, Long)]], num_of_documents: Long) :
  Map[String, Tuple4[Long, Long, Long, Double]] = {
    val freq_annotations = annotated_sentence
    val max_term_raw_freq: Long = if (freq_annotations.isEmpty) {
      0
    } else {
      freq_annotations.maxBy(_._2)._2
    }
    val tfidf_annotated_terms: Map[String, Tuple4[Long, Long, Long, Double]] =
      freq_annotations.map(term => {
        val term_string = term._1
        val term_raw_freq = term._2
        val dictionary_term: Tuple2[Long, Long] =
          dictionary.value.getOrElse(term_string, (0: Long, 0: Long))
        val term_id = dictionary_term._1
        val num_docs_with_terms = dictionary_term._2
        val term_tfidf = calcTermTfIdf(num_of_documents = num_of_documents,
          term_raw_freq = term_raw_freq,
          max_term_raw_freq = max_term_raw_freq,
          num_docs_with_terms = num_docs_with_terms)
        val dictionary_term_with_tfidf = (term_string, (term_raw_freq, term_id, num_docs_with_terms, term_tfidf))
        dictionary_term_with_tfidf
      })
    tfidf_annotated_terms
  }
}
