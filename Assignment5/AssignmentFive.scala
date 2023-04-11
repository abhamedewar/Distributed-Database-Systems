package assignment.five

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object AssignmentFive extends App {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  var sparkSession:SparkSession = SparkSession.builder().
    config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.serializer",classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName).
    master("local[*]")
    .appName("lastassignment").getOrCreate()

  SedonaSQLRegistrator.registerAll(sparkSession)
  SedonaVizRegistrator.registerAll(sparkSession)

  val resourseFolder = System.getProperty("user.dir")+"/src/test/resources/"
  val csvPolygonInputLocation = resourseFolder + "testenvelope.csv"
  val csvPointInputLocation = resourseFolder + "testpoint.csv"
  val firstpointdata = resourseFolder + "outputdata/firstpointdata"
  val firstpolydata = resourseFolder + "outputdata/firstpolygondata"

  println("Q2.1")
  firstPointQuery()
  println("Q2.2")
  secondPointQuery()
  println("Q2.3")
  firstPloygonQuery()
  println("Q2.4")
  secondPolygonQuery()
  println("Q2.5")
  JoinQuery()

  println("Assignment Five Done!!!")

  def firstPointQuery(): Unit = {
    //Read the given testpoint.csv file in csv format and write in delta format and save named firstpointdata
    val p_df = sparkSession.read.option("inferSchema", "true").csv(csvPointInputLocation).toDF("x", "y")
    val dlt_format = p_df.where("x > 500 AND y > 500")
    dlt_format.write.format("delta").mode("overwrite").save(firstpointdata)
  }

  def secondPointQuery(): Unit = {
    //Read the firstpointdata in delta format. Print the total count of the points.
    val df = sparkSession.read.format("delta").load(firstpointdata)
    println("Total count: " + df.count())
  }

  def firstPloygonQuery(): Unit = {
    //Read the given testenvelope.csv in csv format and write in delta format and save it named firstpolydata

    val poly_df = sparkSession.read.option("inferSchema", "true").csv(csvPolygonInputLocation).toDF("column_1", "column_2", "column_3", "column_4")
    val dlt_format = poly_df.where("column_1 > 900 AND column_2 > 900 AND column_3 > 900 AND column_4 > 900")
    dlt_format.write.format("delta").mode("overwrite").save(firstpolydata)
  }

  def secondPolygonQuery(): Unit = {
    //Read the firstpolydata in delta format. Print the total count of the polygon
    val dlt_format = sparkSession.read.format("delta").load(firstpolydata)
    println("Count: " + dlt_format.count())
  }

  def JoinQuery(): Unit = {
    //Read the firstpointdata in delta format and find the total count for point pairs where distance between the points within a pair is less than 2.
    val first_pt_df = sparkSession.read.format("delta").load(firstpointdata)
    val first_poly_df = sparkSession.read.format("delta").load(firstpolydata)
    fiirst_pt_df.createOrReplaceTempView("points")
    first_poly_df.createOrReplaceTempView("rect")
    val df1 = sparkSession.sql("Select points.* from points, rect " +
      "where ST_Within(ST_Point(points.a, points.b), ST_PolygonFromEnvelope(rect.col1, rect.col2, rect.col3, rect.col4)) ")
    df1.createOrReplaceTempView("pt1")
    df1.createOrReplaceTempView("pt2")
    val df2 = sparkSession.sql("Select pt1.a as a1, pt1.b as b1, pt2.a as a2, pt2.b as b2 from pt1, pt2 " +
      "where ST_Distance(ST_Point(pt1.a, pt1.b), ST_Point(pt2.a, pt2.b)) > 0 " +
      "and ST_Distance(ST_Point(pt1.a, pt1.b), ST_Point(pt2.a, pt2.b)) < 2")
    println(df2.count() / 2)

  }
}