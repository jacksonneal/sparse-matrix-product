package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

object SparseProduct {
  private final val logger: org.apache.log4j.Logger = LogManager.getRootLogger
  private final val P = 1 // # partitions
  type SparseRDD = RDD[(Int, Int, Long)] // (i, j, v)

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      logger.error("Usage:\nproduct.SparseProduct <n> <a_dir> <b_dir> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Sparse Product").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Both a and b matrices have dimension nxn
    val n = args(0).toInt

    val aDir = args(1)
    val bDir = args(2)
    val output = args(3) + "product"

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(output + "vh"), true)
      hdfs.delete(new org.apache.hadoop.fs.Path(output + "nb"), true)
    } catch {
      case _: Throwable =>
    }
    // ================

    val a = parseSparse(sc, aDir)
    val b = parseSparse(sc, bDir)

    val product_vh = this.vhSparseProduct(a, b)
    val product_nb = this.naiveBlockRowSparseProduct(a, b, n)

    product_vh.coalesce(1, shuffle = true).saveAsTextFile(output + "vh")
    product_nb.coalesce(1, shuffle = true).saveAsTextFile(output + "nb")
  }

  private def parseSparse(sc: SparkContext, dir: String): SparseRDD = {
    sc.textFile(dir).map(line => {
      val split = line.substring(1, line.length() - 1).split(",")
      (split(0).toInt, split(1).toInt, split(2).toLong)
    })
  }

  // Block partition: Naive Block Row
  private def naiveBlockRowSparseProduct(a: SparseRDD, b: SparseRDD, n: Int): SparseRDD = {
    val hp = new HashPartitioner(P)

    // Keep a grouped by row and marked by partition
    // (p, (i, a_i(j, v), c_i(j, v)))
    var a_par = a.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey(hp).mapPartitionsWithIndex {
      case (p, a_p) => a_p.map {
        case (i, a_i_itr) =>
          // use map to track a_i
          // use map to track corresponding c_i
          val a_i = new mutable.HashMap[Int, Long]()
          for ((k, v) <- a_i_itr) {
            a_i(k) = v
          }
          (p, (i, a_i, new mutable.HashMap[Int, Long]()))
      }
    }

    // Group b by row
    // (j, b_j(k, v))
    val b_row = b.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey().mapValues {
      values =>
        // use map to store b_j values
        val b_j = new mutable.HashMap[Int, Long]()
        for ((k, v) <- values) {
          b_j(k) = v
        }
        b_j
    }

    for (p <- 0 until P) {
      // Iteratively send each each section of b to a different partition, mark as such
      // (p, b(j, b_j(k, v)))
      val b_cur = b_row.map {
        case (j, b_j) => ((j + p) % P, (j, b_j))
      }.groupByKey(hp).mapValues {
        b_itr =>
          // b section will be stored as a nested map
          val b = new mutable.HashMap[Int, mutable.HashMap[Int, Long]]()
          for ((j, b_j) <- b_itr) {
            b(j) = b_j
          }
          b
      }

      // As a and b have partition as key and a has assigned partitioner, we can join them
      // use mapValues to keep a partitioned
      // (p, (i, a_i(i, v), c_i(i, v)))
      a_par = a_par.join(b_cur).mapValues {
        case ((i, a_i, c_i), b) =>
          for ((j, b_j) <- b) {
            val a_ij = a_i.get(j)

            // If a_ij is not defined, b_j cannot contribute to any value in c_i
            if (a_ij.isDefined) {
              for (k <- 0 until n) {
                val b_jk = b_j.get(k)
                if (b_jk.isDefined) {
                  c_i(k) = c_i.getOrElse(k, 0L) + a_ij.get * b_jk.get
                }
              }
            }
          }

          // maintain row, a_i, and potentially updated c_i
          (i, a_i, c_i)
      }
    }

    // Reformat c_i to SparseRDD
    a_par.flatMap {
      case (_, (i, _, c_i)) =>
        c_i.map {
          case (j, v) => (i, j, v)
        }
    }
  }

  // Vertical-Horizontal partition
  private def vhSparseProduct(a: SparseRDD, b: SparseRDD): SparseRDD = {
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
