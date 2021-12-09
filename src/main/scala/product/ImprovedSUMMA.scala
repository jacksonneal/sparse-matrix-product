package product

import org.apache.spark.{SparkContext}
import product.SparseProduct.{P, HP, SparseRDD}

import scala.collection.mutable

object ImprovedSUMMA extends Multiplier {
  private def improvedSummaAPartitions(i: Int, j: Int, n: Int): List[Int] = {
    val p_dim = math.sqrt(P).toInt
    val row = (i / (n.toDouble / p_dim)).toInt
    List.range(0, p_dim).map(col => row * p_dim + col)
  }

  private def improvedSummaBPartitions(j: Int, k: Int, n: Long): List[Int] = {
    val p_dim = math.sqrt(P).toInt
    val col = (k / (n.toDouble / p_dim)).toInt
    List.range(0, p_dim).map(row => row * p_dim + col)
  }

  override def sparseProduct(a: SparseRDD, b: SparseRDD, n: Int, sc: SparkContext): SparseRDD = {
    val a_par = a.flatMap {
      case (i, j, v) =>
        improvedSummaAPartitions(i, j, n).map(p => (p, (i, j, v)))
    }.groupByKey(HP)
    //    a_par.coalesce(1, shuffle = true).saveAsTextFile("a_par")
    val b_par = b.flatMap {
      case (j, k, v) =>
        improvedSummaBPartitions(j, k, n).map(p => (p, (j, k, v)))
    }.groupByKey(HP).mapValues {
      b_itr =>
        // use map by col for easy lookup later
        val b = new mutable.HashMap[Int, Seq[(Int, Long)]]()
        for ((j, k, v) <- b_itr) {
          b(j) = b.getOrElse(j, Seq.empty[(Int, Long)]) :+ (k, v)
        }
        b
    }
    //    b_par.coalesce(1, shuffle = true).saveAsTextFile("b_par")
    val c_par = a_par.join(b_par).mapValues {
      case (a, b) =>
        val c = new mutable.HashMap[(Int, Int), Long]()
        for ((i, j, v) <- a) {
          for ((k, w) <- b.getOrElse(j, Seq.empty[(Int, Long)])) {
            c((i, k)) = c.getOrElse((i, k), 0L) + v * w
          }
        }
        c
    }
    c_par.flatMap {
      case (_, c) => c.map {
        case ((i, k), v) =>
          (i, k, v)
      }
    }
  }
}
