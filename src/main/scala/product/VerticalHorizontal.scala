package product

import org.apache.spark.SparkContext
import product.SparseProduct.SparseRDD

object VerticalHorizontal extends Multiplier {
  override def sparseProduct(a: SparseRDD, b: SparseRDD, n: Int, sc: SparkContext): SparseRDD = {
    val a_col = a.map {
      case (i, j, v) => (j, (i, v))
    }
    val b_row = b.map {
      case (i, j, v) => (i, (j, v))
    }
    a_col.join(b_row).map {
      case (_, ((i, v), (j, w))) => ((i, j), v * w)
    }.reduceByKey(_ + _).map {
      case ((i, j), v) => (i, j, v)
    }
  }
}
