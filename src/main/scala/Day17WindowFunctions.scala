
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17WindowFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 17 Window Functions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // Scenario 1: Student rankings by course
    val students = Seq(
      ("S101", "Ravi", "Scala", 95),
      ("S102", "Priya", "Scala", 88),
      ("S103", "Arjun", "Scala", 88),
      ("S104", "Sneha", "Scala", 76),
      ("S105", "Kiran", "Scala", 68),
      ("S106", "Meena", "Spark", 98),
      ("S107", "Rahul", "Spark", 90),
      ("S108", "Divya", "Spark", 90),
      ("S109", "Asha", "Spark", 82),
      ("S110", "Manoj", "Spark", 75)
    ).toDF("student_id", "name", "course", "marks")

    val courseWindow = Window
      .partitionBy("course")
      .orderBy(desc("marks"))

    println("\n========== STUDENT RANKINGS ==========")

    students
      .withColumn("row_number", row_number().over(courseWindow))
      .withColumn("rank", rank().over(courseWindow))
      .withColumn("dense_rank", dense_rank().over(courseWindow))
      .orderBy("course", "row_number")
      .show()

    // Top 3 students per course
    println("\n========== TOP 3 STUDENTS PER COURSE ==========")

    val rankedStudents = students
      .withColumn("row_number", row_number().over(courseWindow))

    rankedStudents
      .filter(col("row_number") <= 3)
      .orderBy("course", "row_number")
      .show()

    // Scenario 2: Latest insurance policy per customer
    val policies = Seq(
      ("C101", "P1001", "2025-01-10", 12000.0),
      ("C101", "P1002", "2025-08-15", 15000.0),
      ("C101", "P1003", "2026-03-20", 18000.0),
      ("C102", "P2001", "2025-05-12", 9000.0),
      ("C102", "P2002", "2026-01-18", 11000.0),
      ("C103", "P3001", "2025-11-05", 14000.0),
      ("C103", "P3002", "2026-06-25", 16000.0)
    ).toDF("customer_id", "policy_id", "policy_date", "premium")

    val policyWindow = Window
      .partitionBy("customer_id")
      .orderBy(desc("policy_date"), desc("policy_id"))

    println("\n========== LATEST POLICY PER CUSTOMER ==========")

    policies
      .withColumn("row_number", row_number().over(policyWindow))
      .filter(col("row_number") === 1)
      .drop("row_number")
      .orderBy("customer_id")
      .show()

    // Scenario 3: Previous and next trip fares
    val trips = Seq(
      ("Ravi", "Route-A", "2026-01-01", 100.0),
      ("Ravi", "Route-A", "2026-01-02", 120.0),
      ("Ravi", "Route-A", "2026-01-03", 90.0),
      ("Priya", "Route-B", "2026-01-01", 200.0),
      ("Priya", "Route-B", "2026-01-02", 220.0),
      ("Priya", "Route-B", "2026-01-03", 210.0)
    ).toDF("driver", "route", "trip_date", "fare")

    val tripWindow = Window
      .partitionBy("route", "driver")
      .orderBy("trip_date")

    println("\n========== LAG AND LEAD ==========")

    trips
      .withColumn("previous_fare", lag("fare", 1).over(tripWindow))
      .withColumn("next_fare", lead("fare", 1).over(tripWindow))
      .withColumn(
        "fare_change",
        col("fare") - col("previous_fare")
      )
      .orderBy("route", "driver", "trip_date")
      .show()

    spark.stop()
  }
}
