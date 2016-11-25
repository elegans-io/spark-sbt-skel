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
    val tf : Double = 0.0 //TODO: calculate tf
    val idf : Double = 0.0 //TODO: calculate idf
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
    val max_term_raw_freq: Long = 0 //TODO: get the max raw frequency from the list of terms
    val tfidf_annotated_terms: Map[String, Tuple4[Long, Long, Long, Double]] = // map on terms of the sentence
      freq_annotations.map(term => { // maps on terms annotations
        val term_string : String = "" //TODO: complete with the term string
        val term_raw_freq : Long = 0 //TODO: complete with term raw frequency in sentence
        val dictionary_term: Tuple2[Long, Long] = (0: Long, 0: Long) //TODO: get the pair (unique_id, num_docs_with_terms) from dictionary
        val term_id : Long = 0 //TODO: the term unique id
        val num_docs_with_terms : Long = 0 //TODO: the term num_docs_with_terms
        val term_tfidf : Double = 0 //TODO: the tfidf
        val dictionary_term_with_tfidf = (term_string, (term_raw_freq, term_id, num_docs_with_terms, term_tfidf))
        dictionary_term_with_tfidf
      })
    tfidf_annotated_terms
  }
}
