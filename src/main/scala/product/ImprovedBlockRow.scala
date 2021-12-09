package product

import org.apache.spark.SparkContext
import product.SparseProduct.{HP, SparseRDD}

import scala.collection.mutable

object ImprovedBlockRow extends Multiplier {
  override def sparseProduct(a: SparseRDD, b: SparseRDD, n: Int, sc: SparkContext): SparseRDD = {
    // Partition ac into block rows
    // (p, ac(i, a_i(j, v), c_i(j, v)))
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

    // Store the non-zero a block row sub-cols that are present in each partition
    // Will be used to efficiently send b blow rows
    val p_nz_col = ac_par.map {
      case (p, ac) =>
        // Make set of sub-col that have non-zeros
        // for each row in ac, add non-zero cols to set
        val nz_col = ac.map {
          case (_, (a_i, _)) =>
            a_i.keySet
        }.fold(Set.empty[Int])(_ union _)
        (p, nz_col)
    }.persist() // This metadata will be small and we can broadcast

    // Group b by row
    // (j, b_j(k, v))
    // Evaluate which partition each b row needs to be sent to
    // Group and partition b rows, using same partitioner as a
    val b_par = b.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey(HP).mapValues {
      values =>
        // use map to store b_j values
        val b_j = new mutable.HashMap[Int, Long]()
        for ((k, v) <- values) {
          b_j(k) = v
        }
        b_j
    }.cartesian(p_nz_col).filter {
      case ((j, _), (_, nz_col)) => nz_col.contains(j)
    }.map {
      case ((j, b_j), (p, _)) => (p, (j, b_j))
    }.groupByKey(HP)

    // Join ac block rows with b block rows in same partition
    ac_par = ac_par.join(b_par).mapValues {
      case (ac, b) =>
        // each a_i row corresponds to a c_i row
        for ((_, (a_i, c_i)) <- ac) {
          // each b_j row may contribute to output
          for ((j, b_j) <- b) {
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
        // maintain a and c
        ac
    }

    // Reformat c_i to output SparseRDD
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
