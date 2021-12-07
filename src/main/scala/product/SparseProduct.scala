package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.:+
import scala.collection.mutable

object SparseProduct {
  private final val logger: org.apache.log4j.Logger = LogManager.getRootLogger
  private final val P = 4 // # partitions, must be square
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
      hdfs.delete(new org.apache.hadoop.fs.Path(output + "iss"), true)
    } catch {
      case _: Throwable =>
    }
    // ================

    val a = parseSparse(sc, aDir)
    val b = parseSparse(sc, bDir)

    val product_vh = this.vhSparseProduct(a, b)
    val product_nbr = this.naiveBlockRowSparseProduct(a, b, n)
    val product_ibr = this.improvedBlockRowSparseProduct(a, b, n)
    val product_ss = this.sparseSummaProduct(sc, a, b, n)
    val product_iss = this.improvedSparseSummaProduct(sc, a, b, n)

    //    assert(this.equal(product_nbr, product_vh))
    //    assert(this.equal(product_ibr, product_vh))
    product_vh.coalesce(1, shuffle = true).saveAsTextFile(output + "vh")
    product_iss.coalesce(1, shuffle = true).saveAsTextFile(output + "iss")

    assert(this.equal(product_iss, product_vh))
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

  private def improvedSparseSummaProduct(sc: SparkContext, a: SparseRDD, b: SparseRDD, n: Int): SparseRDD = {
    val hp = new HashPartitioner(P)
    val a_par = a.flatMap {
      case (i, j, v) =>
        improvedSummaAPartitions(i, j, n).map(p => (p, (i, j, v)))
    }.groupByKey(hp)
//    a_par.coalesce(1, shuffle = true).saveAsTextFile("a_par")
    val b_par = b.flatMap {
      case (j, k, v) =>
        improvedSummaBPartitions(j, k, n).map(p => (p, (j, k, v)))
    }.groupByKey(hp).mapValues {
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

  // Block Partition: Sparse SUMMA
  private def sparseSummaProduct(sc: SparkContext, a: SparseRDD, b: SparseRDD, n: Int): SparseRDD = {
    val hp = new HashPartitioner(P)
    // c will be statically partitioned
    var c_par = sc.range(0, P).map {
      i => (i.toInt, new mutable.HashMap[(Int, Int), Long]())
    }.partitionBy(hp)
    //    c_par.coalesce(1, shuffle = true).saveAsTextFile("c_par")
    // store a and b in groups what will be iteratively passed around partitions
    val p_dim = math.sqrt(P).toInt
    var a_par = a.map {
      case (i, j, v) => (getSummaAPartition(i, j, n), (i, j, v))
    }.groupByKey(hp)
    //    a_par.coalesce(1, shuffle = true).saveAsTextFile("a_par")
    var b_par = b.map {
      case (j, k, v) => (getSummaBPartition(j, k, n), (j, k, v))
    }.groupByKey(hp).mapValues {
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
