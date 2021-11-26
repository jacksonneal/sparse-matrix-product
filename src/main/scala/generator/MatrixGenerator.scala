package generator

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs

object MatrixGenerator {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Usage:\ngenerator.MatrixGenerator <i> <j> <k> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Matrix Generator")/*.setMaster("local[4]")*/
    val sc = new SparkContext(conf)

    // matrixLeft has dimension i * j, matrixRight has dimension j * k
    val i = args(0)
    val j = args(1)
    val k = args(2)

    val output = args(3)

    val leftMatrixRDD = RandomRDDs.uniformVectorRDD(sc, i.toInt, j.toInt)

    leftMatrixRDD.saveAsTextFile(output + "/left")

    val rightMatrixRDD = RandomRDDs.uniformVectorRDD(sc, j.toInt, k.toInt)

    rightMatrixRDD.saveAsTextFile(output + "/right")


  }
}
