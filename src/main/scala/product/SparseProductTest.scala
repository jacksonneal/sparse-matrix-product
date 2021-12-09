package product

import generator.MatrixGenerator
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import product.SparseProduct.SparseRDD

object SparseProductTest {
  private final val logger: org.apache.log4j.Logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logger.error("Usage:\nproduct.SparseProductTest <n> <density>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Sparse Product").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Both a and b matrices have dimension nxn
    val n = args(0).toInt
    val d = args(1).toDouble

    val a = MatrixGenerator.getSparseMatrix(sc, n, d)
    val b = MatrixGenerator.getSparseMatrix(sc, n, d)

    val product_vh = VerticalHorizontal.sparseProduct(a, b, n, sc)
    val product_nbr = NaiveBlockRow.sparseProduct(a, b, n, sc)
    val product_ibr = ImprovedBlockRow.sparseProduct(a, b, n, sc)
    val product_ns = NaiveSUMMA.sparseProduct(a, b, n, sc)
    val product_is = ImprovedSUMMA.sparseProduct(a, b, n, sc)

    assert(this.equal(product_vh, product_nbr))
    assert(this.equal(product_vh, product_ibr))
    assert(this.equal(product_vh, product_ns))
    assert(this.equal(product_vh, product_is))
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
}
