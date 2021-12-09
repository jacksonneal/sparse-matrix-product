package product

import org.apache.spark.SparkContext
import product.SparseProduct.{HP, P, SparseRDD}

import scala.collection.mutable

object NaiveBlockRow extends Multiplier {
  override def sparseProduct(a: SparseRDD, b: SparseRDD, n: Int, sc: SparkContext): SparseRDD = {
    // Partition a into block rows
    // (p, a(i, a_i(j, v), c_i(j, v)))
    var ac_par = a.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey(HP).mapValues {
      values =>
        // use map to store a_i values
        val a_i = new mutable.HashMap[Int, Long]()
        for ((j, v) <- values) {
          a_i(j) = v
        }
        a_i
    }.mapPartitionsWithIndex(preservesPartitioning = true, f = {
      case (p, a_itr) =>
        // use map to store ac, with a_i and corresponding c_i
        // a(i, (a_i(j, v), c_i(j, v)))
        val ac = new mutable.HashMap[Int, (mutable.HashMap[Int, Long], mutable.HashMap[Int, Long])]
        for ((i, a_i) <- a_itr) {
          ac(i) = (a_i, new mutable.HashMap[Int, Long]())
        }
        Seq((p, ac)).toIterator
    })

    // Group b by row
    // (j, b_j(k, v))
    val b_row = b.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey(HP).mapValues {
      values =>
        // use map to store b_j values
        val b_j = new mutable.HashMap[Int, Long]()
        for ((k, v) <- values) {
          b_j(k) = v
        }
        b_j
    }

    for (p <- 0 until P) {
      // Partition b into block rows
      // Iteratively pass each b block row to each partition
      // (p, b(j, b_j(k, v)))
      val b_par = b_row.map {
        case (j, b_j) => ((j + p) % P, (j, b_j))
      }.groupByKey(HP).mapValues {
        b_itr =>
          // use map to store b
          val b = new mutable.HashMap[Int, mutable.HashMap[Int, Long]]()
          for ((j, b_j) <- b_itr) {
            b(j) = b_j
          }
          b
      }

      // As a and b have partition as key and a has assigned partitioner, we can join them
      // use mapValues to keep a partitioned, avoid shuffle of a
      // (p, (i, a_i(i, v), c_i(i, v)))
      ac_par = ac_par.leftOuterJoin(b_par).mapValues {
        case (ac, b) =>
          // the a partition may not receive any non-zero b block row in a given iteration
          if (b.isDefined) {
            // Each a_i row corresponds to a c_i row
            for ((_, (a_i, c_i)) <- ac) {
              // Each b_j row may contribute to output
              for ((j, b_j) <- b.get) {
                val a_ij = a_i.get(j)
                // If a_ij is not defined, b_j cannot contribute to any value in c_i
                if (a_ij.isDefined) {
                  for (k <- 0 until n) {
                    val b_jk = b_j.get(k)
                    // If b_jk is not defined, it will not contribute to c_ik
                    if (b_jk.isDefined) {
                      c_i(k) = c_i.getOrElse(k, 0L) + a_ij.get * b_jk.get
                    }
                  }
                }
              }
            }
          }
          // maintain a and c
          ac
      }
    }

    // Reformat c to output SparseRDD
    ac_par.flatMap {
      case (_, ac) =>
        ac.flatMap {
          case (i, (_, c_i)) =>
            c_i.map {
              case (j, v) => (i, j, v)
            }
        }
    }
  }
}
