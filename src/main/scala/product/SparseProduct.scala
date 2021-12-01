package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

object SparseProduct {
  private final val logger: org.apache.log4j.Logger = LogManager.getRootLogger
  private final val P = 5 // # partitions
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
    val output = args(3)

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
    val product_nbr = this.naiveBlockRowSparseProduct(a, b, n)
    val product_ibr = this.improvedBlockRowSparseProduct(a, b, n)

    assert(this.equal(product_nbr, product_vh))
    assert(this.equal(product_ibr, product_vh))
  }

  private def equal(a: SparseRDD, b: SparseRDD): Boolean = {
    val a_coord = a.map {
      case (i, j, v) => ((i, j), v)
    }
    val b_coord = b.map {
      case (i, j, v) => ((i, j), v)
    }
    a_coord.fullOuterJoin(b_coord).filter {
      case (_, (v, w)) =>
        v.isEmpty || w.isEmpty || v.get != w.get
    }.count() == 0
  }

  private def parseSparse(sc: SparkContext, dir: String): SparseRDD = {
    sc.textFile(dir).map(line => {
      val split = line.substring(1, line.length() - 1).split(",")
      (split(0).toInt, split(1).toInt, split(2).toLong)
    })
  }

  // Block Partition: Improved Block Row
  private def improvedBlockRowSparseProduct(a: SparseRDD, b: SparseRDD, n: Int): SparseRDD = {
    val hp = new HashPartitioner(P)

    // Partition ac into block rows
    // (p, ac(i, a_i(j, v), c_i(j, v)))
    var ac_par = a.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey(hp).mapValues {
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
    }.groupByKey(hp).mapValues {
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
    }.groupByKey(hp)

    // Join ac block rows with b block rows in same partition
    ac_par = ac_par.leftOuterJoin(b_par).mapValues {
      case (ac, b) =>
        // the a partition may not receive any non-zero b block rows
        if (b.isDefined) {
          // each a_i row corresponds to a c_i row
          for ((_, (a_i, c_i)) <- ac) {
            // each b_j row may contribute to output
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

  // Block partition: Naive Block Row
  private def naiveBlockRowSparseProduct(a: SparseRDD, b: SparseRDD, n: Int): SparseRDD = {
    val hp = new HashPartitioner(P)

    // Partition a into block rows
    // (p, a(i, a_i(j, v), c_i(j, v)))
    var ac_par = a.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey(hp).mapValues {
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
    }.groupByKey(hp).mapValues {
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
      }.groupByKey(hp).mapValues {
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
