import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.util.ByteString

import java.nio.file.{FileSystems, Path}
import scala.collection.immutable.TreeMap
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object FileProcessor extends App {
  val directory = args.head // directory path

  implicit val actorSystem: ActorSystem = ActorSystem()

  case class SensorData(sensorId: String, humidity: String)
  case class HumidityMetrics(min: Option[Int], average: Option[Int], max: Option[Int])

  /***
   * Persistent memory to store data
   */
  var result: TreeMap[String, HumidityMetrics] = TreeMap.empty[String, HumidityMetrics]

  /***
   * Metrics to calculate
   */
  var fileProcessCount = 0
  var processedMeasurementCount = 0
  var failedMeasurementCount = 0

  def calculateStats(persisted: Option[Int], current: Option[String], isMax: Boolean): Option[Int] = {
    (persisted, current) match {
      case (Some(persisted), None) => Some(persisted)
      case (Some(persisted), Some(current)) => if (isMax) Some(math.max(persisted, current.toInt)) else Some(math.min(persisted, current.toInt))
      case (None, Some(current)) => Some(current.toInt)
      case (None, None) => None
    }
  }

  val fs = FileSystems.getDefault

  val EmptyString = ""
  val header = List("sensor-id", "humidity ")

  import CsvParsing._

  val delimiter: Byte = Comma
  val quoteChar: Byte = DoubleQuote
  val escapeChar: Byte = Backslash

  /***
   * *********************************************************
   * SOURCE
   * **********************************************************
   * Source reads all the csv files from the provided directory
   * **********************************************************
   */
  val source: Source[ByteString, NotUsed] = Directory
    .ls(fs.getPath(directory))
    .flatMapConcat { path: Path =>
      fileProcessCount = fileProcessCount + 1
      FileIO.fromPath(path).map { data =>
        data ++ ByteString("\n")
      }
    }

  /***
   * *********************************************************************************************************************************
   * FLOWS
   * *********************************************************************************************************************************
   * Four types of flows are defined here
   * 1. parseAsCsvFlow : It parse the data read from source in the csv format
   * 2. convertCsvByteStringToStringFlow: It converts ByteString data to String Data
   * 3. deserializeRowsToSensorDataFlow: It deserialize the data to the cases class i.e SensorData(sensorId: String, humidity: String)
   * 4. calculateStatsFlow: It calculates the Metrics required on the streaming data
   * **********************************************************************************************************************************
   */

  val parseAsCsvFlow: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner(delimiter, quoteChar, escapeChar)

  val convertCsvByteStringToStringFlow: Flow[List[ByteString], List[String], NotUsed] = Flow[List[ByteString]].map(_.map {
    _.utf8String
  })
  val deserializeRowsToSensorDataFlow: Flow[List[String], SensorData, NotUsed] = Flow[List[String]].filter { row => row != List(EmptyString) && row != header }
    .map { case sensorId :: humidity :: _ => processedMeasurementCount = processedMeasurementCount + 1; SensorData(sensorId.trim, humidity.trim) }

  val calculateStatsFlow: Flow[SensorData, Unit, NotUsed] = Flow[SensorData].map { sensorData =>
    val humidity = if (sensorData.humidity == "NaN") {
      failedMeasurementCount = failedMeasurementCount + 1; None
    } else Some(sensorData.humidity)

    result.get(sensorData.sensorId).fold {
      result = result + (sensorData.sensorId -> HumidityMetrics(humidity.map(_.toInt), humidity.map(_.toInt), humidity.map(_.toInt)))
    } { persistentHumidityMetrics =>

      val min: Option[Int] = calculateStats(persistentHumidityMetrics.min, humidity, isMax = false)
      val max: Option[Int] = calculateStats(persistentHumidityMetrics.max, humidity, isMax = true)
      val average = if (min.isDefined && max.isDefined) Some((min.get + max.get) / 2) else None

      result = result + (sensorData.sensorId -> HumidityMetrics(min, average, max))
    }
  }

  /***
   * **********************************************************
   * SINK
   * **********************************************************
   * Ignored as we need to print data to STDOUT
   * **********************************************************
   */

  val sink = Sink.ignore

  /***
   * **********************************************************
   * GRAPH
   * **********************************************************
   * Wired up the flow!
   * Format: source ~> flows ~> sink
   * source ~> parseAsCsvFlow ~> convertCsvByteStringToStringFlow ~> deserializeRowsToSensorDataFlow ~> calculateStatsFlow ~> s
   * **********************************************************
   */

  val graph = RunnableGraph.fromGraph(GraphDSL.create(sink) { implicit builder =>
    s =>
      import GraphDSL.Implicits._
      source ~> parseAsCsvFlow ~> convertCsvByteStringToStringFlow ~> deserializeRowsToSensorDataFlow ~> calculateStatsFlow ~> s
      ClosedShape
  })

  val future = graph.run()
  Await.result(future, Duration.Inf)

  /**
   * LazyList: evaluates data only when required, treated as stream!
   */
  val lazyResult: LazyList[String] = "sensor-id,min,avg,max" +: result.map { case (sensorId, humidityMetrics) =>
    (sensorId, humidityMetrics.min, humidityMetrics.average, humidityMetrics.max)
  }.to(LazyList).sortBy(_._4)(Ordering[Option[Int]].reverse)
    .map { case (sensorId, min, avg, max) =>
      s"$sensorId,${min.map(_.toString).getOrElse("NaN")},${avg.map(_.toString).getOrElse("NaN")},${max.map(_.toString).getOrElse("NaN")}"
    }


  println("\n\n#############################################################")
  println(s"Num of processed files: $fileProcessCount")
  println(s"Num of processed measurements: $processedMeasurementCount")
  println(s"Num of failed measurements: $failedMeasurementCount\n")
  println(s"Sensors with highest avg humidity:")
  lazyResult.foreach(println(_))
}
