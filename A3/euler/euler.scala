import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object EulerSpark {  

  def calculateV(iter: Iterator[Int]): Iterator[Int] = {
    val seedValue = Random.nextInt(1000)
    val rand = new Random(seedValue)

    iter.map { _ =>
      var iterations = 0
      var sum = 0.0
      while (sum < 1) {
        iterations += 1
        sum += rand.nextDouble()
      }
      iterations
    }
  }

  def main(args: Array[String]): Unit = {
    // Ensure input is provided
    if (args.length < 1) {
      println("Usage: EulerSpark <num_samples>")
      sys.exit(1)
    }

    // Main logic starts here
    val nSamples = args(0).toInt

    val conf = new SparkConf().setAppName("EulerSpark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    assert(sc.version >= "3.0") // Ensure Spark version is at least 3.0+

    val samples = sc.parallelize(1 to nSamples, numSlices = 32)
    val VValues = samples.mapPartitions(calculateV)
    val totalIterations = VValues.reduce(_ + _)

    println(totalIterations.toDouble / nSamples)

    sc.stop()
  }
}
