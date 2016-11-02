/**
  Date handling functions without ntime or joda time to avoid serialization exceptions.
  */
object idate {
  // COnvert a date to integer.
  val monthToNum = (d: String) => d.filterNot(_ == '-').toInt
  // Get the year and month part of the date
  def yearMonth(yyyymmdd: Int): Int = Math.floor(yyyymmdd/100.0).toInt
  // Get the month part
  def month(yyyymm: Int): Int = (yyyymm - Math.floor(yyyymm/100)*100).toInt
  // Get the year part
  def year(yyyymm: Int): Int = Math.floor(yyyymm/100).toInt
  // Compute the difference in months between 2 dates.
  def mDif(d1: Int, d2: Int): Int = {
    require(d2 >= d1, "Second date must be greater or equal than first one.")
    val ym1 = yearMonth(d1)
    val ym2 = yearMonth(d2)
    val yDif = year(ym2) - year(ym1)
    val mDif = month(ym2) - month(ym1)
    if (yDif != 0) return (12*yDif) - (mDif*(-1))
    else return mDif
  }
  // Add months to a date, and obtain a new date.
  def addMonths(yyyymmdd: Int, monthsToAdd: Int): Int = {
    val ym = yearMonth(yyyymmdd)
    val mm = month(ym) + monthsToAdd
    val yy = year(ym)
    if(mm > 12) 
      (yy+(mm/12))*100 + (mm % 12)
    else 
      (yy*100)+mm
  }
  // Convert a numeric date to string.
  val numToMonth = (d: Int) => {
    val ym = yearMonth(d)
    f"${year(ym)}-${month(ym)}%02d-01"
  }
}

/**
  Build a class to run the linear regression.
  Sample from http://bitingcode.blogspot.com.es/2012/01/simplest-olsmultiplelinearregression.html
  */
class Estimator extends java.io.Serializable {

  import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
  val mlr = new OLSMultipleLinearRegression

  def fit(predictors: Array[Double]) : Array[Double] = {
    mlr.newSampleData(predictors, predictors.length/2, 1)
    mlr.estimateRegressionParameters
  }

  def estimate(x: Double, coefs: Array[Double]): Double = {
    def factor(x: Double, coefs: Array[Double], index: Int, res: Double): Double = {
      if(index == coefs.length) res
      else factor(x, coefs, index+1, res + (coefs(index) * Math.pow(x, index)))
    }
    factor(x, coefs, 0, 0.0)
  }
}

// Grubbs test to detect outliers.
// http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
// Implementation details: http://bit.ly/1XL9Tg3
class GrubbsTest(alpha: Double = 0.05) extends java.io.Serializable {

  import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
  import org.apache.commons.math3.distribution.TDistribution

  def test(tuples: Array[(Int, Double)]): Array[(Int, Double)] = {
    val data = tuples.map(_._2)
    val stat = new DescriptiveStatistics
    data.foreach(stat.addValue)
    val meanValue = stat.getMean
    val stdev = stat.getStandardDeviation
    val dataSize = data.length
    val degreesOfFreedom = dataSize - 2
    val significance = alpha / dataSize
    val tCrit = new TDistribution(degreesOfFreedom).inverseCumulativeProbability(1-significance)
    val Gcrit = ((dataSize - 1) * tCrit) / Math.sqrt(dataSize * (degreesOfFreedom + Math.pow(tCrit, 2)))
    val minValue = data.reduce(_ min _)
    val Gmin = (meanValue - minValue)/stdev
    val maxValue = data.reduce(_ max _)
    val Gmax = (maxValue - meanValue)/stdev
    // First check if MIN is an outlier.
    val newTuples = if(Gmin > Gcrit) tuples.filter(d => d._2 != minValue) else tuples
    // Now check if the max value is an outlier.
    if(Gmax > Gcrit)
      return(newTuples.filter(d => d._2 != maxValue))
    else
      return(newTuples)
  }
}

/** 
  Predict for all users.
  */
import org.apache.spark.rdd._

def computeEstimations(users: RDD[(String, Seq[(Int, Double)])], minMonth: Int): RDD[(String, String, Double, Double)] = { 
  users.flatMap {
    case (user, values) => 
      // The predictors are int the format: <Yn, Xn, Yn-1, Xn-1, ... , Y1, X1>
      val removeOutliers = new GrubbsTest
      val predictors = removeOutliers.test(values.toArray).
        sortBy(_._1).
        flatMap(tuple => List(tuple._2, tuple._1)).
        toArray
      val numObservations = predictors.length/2
      val estimator = new Estimator
      val coefs = estimator.fit(predictors)
      //val error = Math.abs(predictors(numObservations*2 - 2) - estimator.estimate(predictors(numObservations*2 - 1), coefs))
      val estimation = estimator.estimate(numObservations, coefs)
      val average = predictors.sliding(2, 2).map(_(0)).sum / (predictors.length/2)
      val predMonth = (idate.addMonths(minMonth, 8)*100) + 1
      if(estimation < 0.0) Some(user, idate.numToMonth(predMonth), 0.0, average)
      else Some(user, idate.numToMonth(predMonth), estimation, average)
    case _ => None
  }
}

/**
  Read the users from the CSV file.
  */
val localFilePath = "/Users/renero/iCloudLink/Research/YOT/PredictBill/sample_traffic_voice.csv"
val HDFSPath = "/user/renero/sample_traffic_voice.csv"
val csvUsers = sc.textFile(HDFSPath).
  map(_.split(",")).
  map(e => (e(7),(idate.monthToNum(e(6)), e(0).toDouble+e(1).toDouble + e(2).toDouble+e(3).toDouble + 
    e(4).toDouble+e(5).toDouble)))
val minMonth = csvUsers.map(_._2._1).reduce(_ min _)
val users = csvUsers.map(e => ((e._1, idate.mDif(minMonth, e._2._1)), e._2._2)).
  reduceByKey(_ + _).
  map {
    case ((uid, month), bill) => (uid, (month, bill))
  }.
  groupByKey().
  mapValues(_.toSeq)
val estimations = computeEstimations(users, minMonth)



















def see(id: String) {
  val values = users.lookup(id).take(1)(0).sortBy(_._1).map(_._2)
  println(values.mkString(", "))
}
def data(id: String): Array[Double] = users.lookup(id).take(1)(0).sortBy(_._1).map(_._2).toArray
def tuples(id: String): Array[(Int, Double)] = {
  val d = data(id).toIndexedSeq
  (0 to d.length-1).zip(d).toArray
}
def flatTuples(id: String) =
  tuples(id).
  sortBy(_._1).
  flatMap(tuple => List(tuple._2, tuple._1)).
  toArray
