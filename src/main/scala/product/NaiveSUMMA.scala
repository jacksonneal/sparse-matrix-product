package product

import org.apache.spark.SparkContext
import product.SparseProduct.{HP, P, SparseRDD}

import scala.collection.mutable

object NaiveSUMMA extends Multiplier {
  private def getSummaAPartition(i: Int, j: Int, n: Int): Int = {
    val p_dim = math.sqrt(P).toInt
    val row = (i / (n.toDouble / p_dim)).toInt
    val col = ((j / (n.toDouble / p_dim)).toInt + (p_dim - row)) % p_dim
    row * p_dim + col
  }

  private def getSummaBPartition(j: Int, k: Int, n: Int): Int = {
    val p_dim = math.sqrt(P).toInt
    val col = (k / (n.toDouble / p_dim)).toInt
    val row = ((j / (n.toDouble / p_dim)).toInt + (p_dim - col)) % p_dim
    row * p_dim + col
  }

  override def sparseProduct(a: SparseRDD, b: SparseRDD, n: Int, sc: SparkContext): SparseRDD = {
    // c will be statically partitioned
    var c_par = sc.range(0, P).map {
      i => (i.toInt, new mutable.HashMap[(Int, Int), Long]())
    }.partitionBy(HP)
    //    c_par.coalesce(1, shuffle = true).saveAsTextFile("c_par")
    // store a and b in groups what will be iteratively passed around partitions
    val p_dim = math.sqrt(P).toInt
    var a_par = a.map {
      case (i, j, v) => (getSummaAPartition(i, j, n), (i, j, v))
    }.groupByKey(HP)
    //    a_par.coalesce(1, shuffle = true).saveAsTextFile("a_par")
    var b_par = b.map {
      case (j, k, v) => (getSummaBPartition(j, k, n), (j, k, v))
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
    // a and b will be iteratively passed to the correct c partition
    for (iter <- 0 until p_dim) {
      if (iter != 0) {
        a_par = a_par.map {
          case (p, a) =>
            var next_p = p + 1
            if (next_p % p_dim == 0) {
              next_p -= p_dim
            }
            (next_p, a)
        }
        b_par = b_par.map {
          case (p, b) =>
            ((p + p_dim) % P, b)
        }
      }

      //      a_par.coalesce(1, shuffle = true).saveAsTextFile("a_par" + iter.toString)
      //      b_par.coalesce(1, shuffle = true).saveAsTextFile("b_par" + iter.toString)

      c_par = c_par.leftOuterJoin(a_par).leftOuterJoin(b_par).mapValues {
        case ((c, a), b) =>
          // TODO: they should always be defined
          if (a.isDefined && b.isDefined) {
            for ((i, j, v) <- a.get) {
              for ((k, w) <- b.get.getOrElse(j, Seq.empty[(Int, Long)])) {
                c((i, k)) = c.getOrElse((i, k), 0L) + v * w
              }
            }
          }
          c
      }

      //      c_par.coalesce(1, shuffle = true).saveAsTextFile("c_par" + iter.toString)
    }

    c_par.flatMap {
      case (_, c) => c.map {
        case ((i, k), v) =>
          (i, k, v)
      }
    }
  }
}
