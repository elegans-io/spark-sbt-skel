package io.elegans.exercises

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}

class TermVectors {

  /** Calculate the cosine similarity between two vectors
    *
    * @param first_vector first vector
    * @param second_vector second vector
    * @return the cosine between the two input vectors
    */
  def cosineSimilarity(first_vector: SparseVector, second_vector: SparseVector) : Double = {
    //TODO: calculate the cosine similarity, transform the Sparse vector to Array
    val cs = 0.0 //TODO: replace with the cosine similarity between the two vectors
    cs
  }

  /** Generate a vector representation for a sentence
    *
    * @param doc_tfifd_terms a map with the following informations:
    *                        Map(<term_id> -> (<term_raw_freq>, <term_id>, <num_docs_with_terms>, <term_tfidf>))
    *  @param dictionary a broadcast variable with the complete dictionary of terms:
    *                    (term, (term_unique_id, num_docs_with_terms))
    * @return a SparseVector where each element represent a term and the value is the raw frequency weighted by
    *         the TF-IDF
    */
  def generateVector(doc_tfifd_terms: Map[String, Tuple4[Long, Long, Long, Double]],
                             dictionary: Broadcast[Map[String, Tuple2[Long, Long]]]
                            ): SparseVector = {
    val array_values : Seq[(Int, Double)] = doc_tfifd_terms.map(term => { /* map to all terms of the document */
      val term_id : Int = 0 //TODO: get the term id from dictionary
      val weight : Double = 0.0 //TODO: replace with the tfidf
      (term_id, weight)
    }).toSeq //TODO: filter elements with invalid id (not found on dictionary)
    val v : SparseVector = Vectors.sparse(dictionary.value.size, array_values).toSparse
    v
  }

}
