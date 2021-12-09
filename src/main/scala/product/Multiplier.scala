package product

import org.apache.spark.SparkContext
import product.SparseProduct.SparseRDD

trait Multiplier {
  def sparseProduct(a: SparseRDD, b: SparseRDD, n: Int, sc: SparkContext): SparseRDD
}
