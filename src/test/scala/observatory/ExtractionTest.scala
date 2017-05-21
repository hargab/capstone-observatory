package observatory

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import Extraction._
import org.scalactic.source.Position

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite with DataFrameSuiteBase {

  test("loadStations") {
    val df = loadStations("/stations.csv")

    df.printSchema()

    df.show
  }(pos = Position.here)
}