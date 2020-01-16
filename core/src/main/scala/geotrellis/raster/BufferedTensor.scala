package geotrellis.raster

import geotrellis.vector.Extent

// trait CellAddressable[T] {
//   def get(col: Int, row: Int): Int
//   def getDouble(col: Int, row: Int): Int
// }

// implicit class cellAddressableTile(t: Tile) extends CellAddressable[Tile]

/**
 * Container used to interpret the underlying cell data as having a buffer
 * region around the tile perimeter.
 *
 * @param tile: The raster data, including buffer
 * @param extent: The extent of the "core" data, not including the buffer cells
 * @param bufferSize: The number of cells around the boundary that are
 * considered as buffer data
 */
case class BufferedTensor(val tile: ArrowTensor, val bufferCols: Int, val bufferRows: Int, val extent: Option[Extent]) extends CellGrid {

  val cellType = DoubleCellType

  val cols = tile.cols - bufferCols * 2
  val rows = tile.rows - bufferRows * 2
  val rasterExtent = extent.map(RasterExtent(_, cols, rows))

  lazy val bufferedCols = tile.cols
  lazy val bufferedRows = tile.rows
  lazy val bufferedExtent = extent.map(_.expandBy(rasterExtent.get.cellwidth * bufferCols,
                                       rasterExtent.get.cellheight * bufferRows))
}
