/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.ref

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, Tile, ArrowTensor, BufferedTensor}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.RasterSourceUDT
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, ArrayType}
import org.apache.spark.sql.Encoder
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer.{CatalystIO, _}
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.ref.RasterSource._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/**
 * A delayed-read projected raster implementation.
 *
 * @since 8/21/18
 */
case class TensorRef(sources: Seq[(RasterSource, Int)], subextent: Option[Extent], subgrid: Option[GridBounds])
  extends ProjectedRasterLike {
  def sample = sources.head._1
  def crs: CRS = sample.crs
  def extent: Extent = subextent.getOrElse(sample.extent)
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
  def cols: Int = grid.width
  def rows: Int = grid.height
  def cellType: CellType = sample.cellType
  //def tile: ProjectedRasterTile = RasterRefTile(this)

  protected lazy val grid: GridBounds =
    subgrid.getOrElse(sample.rasterExtent.gridBoundsFor(extent, true))

  lazy val realizedTensor: ArrowTensor = {
    //RasterRef.log.trace(s"Fetching $extent ($grid) from band $bandIndex of $sample")
    val readGrid = subgrid.getOrElse(grid)
    val tiles = sources.map({ case (rs, band) =>
      rs.read(readGrid, Seq(band)).tile.band(0)
    })
    ArrowTensor.stackTiles(tiles)
  }

  def realizedTensor(bufferPixels: Int): BufferedTensor = {
    //RasterRef.log.trace(s"Fetching $extent ($grid) from band $bandIndex of $sample")
    val readGrid = subgrid.map({ sg =>
      sg.buffer(bufferPixels)
    }).getOrElse(grid.buffer(bufferPixels))
    val tiles = sources.map({ case (rs, band) =>
      rs.read(readGrid, Seq(band)).tile.band(0)
    })
    BufferedTensor(ArrowTensor.stackTiles(tiles), bufferPixels, bufferPixels, Some(extent))
  }
}


object TensorRef extends LazyLogging {
  import RasterSourceUDT._
  private val log = logger

  implicit val rsBandSerializer: CatalystSerializer[(RasterSource, Int)] =
    new CatalystSerializer[(RasterSource, Int)] {
      override val schema: StructType =
        StructType(Seq(
          StructField("rasterSource", RasterSourceType, false),
          StructField("bandIndex", IntegerType, false)
        ))

    override def to[R](t: (RasterSource, Int), io: CatalystIO[R]): R = io.create(
      io.to(t._1),
      t._2
    )

    override def from[R](row: R, io: CatalystIO[R]): (RasterSource, Int) = (
      io.get[RasterSource](row, 0),
      io.getInt(row, 1)
    )
  }

  implicit val tensorRefSerializer: CatalystSerializer[TensorRef] = new CatalystSerializer[TensorRef] {
    override val schema: StructType = StructType(Seq(
      StructField("sources", ArrayType(schemaOf[(RasterSource, Int)]), false),
      StructField("subextent", schemaOf[Extent], true),
      StructField("subgrid", schemaOf[GridBounds], true)
    ))

    override def to[R](t: TensorRef, io: CatalystIO[R]): R = io.create(
      io.toSeq(t.sources),
      t.subextent.map(io.to[Extent]).orNull,
      t.subgrid.map(io.to[GridBounds]).orNull
    )

    override def from[R](row: R, io: CatalystIO[R]): TensorRef = TensorRef(
      io.getSeq[(RasterSource, Int)](row, 0),
      if (io.isNullAt(row, 1)) None
      else Option(io.get[Extent](row, 1)),
      if (io.isNullAt(row, 2)) None
      else Option(io.get[GridBounds](row, 2))
    )
  }

  implicit def rrEncoder: ExpressionEncoder[TensorRef] = CatalystSerializerEncoder[TensorRef](true)
}
