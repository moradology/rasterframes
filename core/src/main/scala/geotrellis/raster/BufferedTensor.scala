package geotrellis.raster

import geotrellis.vector.Extent
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, IntegerType}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.TensorUDT._

import org.locationtech.rasterframes.encoders.CatalystSerializerEncoder


/**
 * Container used to interpret the underlying cell data as having a buffer
 * region around the tile perimeter.
 *
 * @param tile: The raster data, including buffer
 * @param extent: The extent of the "core" data, not including the buffer cells
 * @param bufferSize: The number of cells around the boundary that are
 * considered as buffer data
 */
case class BufferedTensor(
  val tensor: ArrowTensor,
  val bufferCols: Int,
  val bufferRows: Int,
  val extent: Option[Extent]
) extends CellGrid {

  val cellType = DoubleCellType

  val cols = tensor.cols - bufferCols * 2
  val rows = tensor.rows - bufferRows * 2
  val rasterExtent = extent.map(RasterExtent(_, cols, rows))

  lazy val bufferedCols = tensor.cols
  lazy val bufferedRows = tensor.rows
  lazy val bufferedExtent = extent.map(_.expandBy(rasterExtent.get.cellwidth * bufferCols,
                                       rasterExtent.get.cellheight * bufferRows))
}


object BufferedTensor {
  import org.apache.spark.sql.rf.BufferedTensorUDT._
  implicit val arrowTensorEncoder: ExpressionEncoder[BufferedTensor] =
    CatalystSerializerEncoder[BufferedTensor](true)

  val schema: StructType = {
    val tensorSchema = StructField("tensor", schemaOf[ArrowTensor], false)
    val colSchema = StructField("columns", IntegerType, false)
    val rowSchema = StructField("rows", IntegerType, false)
    val extentSchema = StructField("extent", schemaOf[Extent], true)

    StructType(Seq(tensorSchema, colSchema, rowSchema, extentSchema))
  }
}
