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
import geotrellis.raster.{CellType, GridBounds, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.RasterSourceUDT
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.locationtech.rasterframes.encoders.CatalystSerializer.{CatalystIO, _}
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/**
 * A delayed-read projected raster implementation.
 *
 * @since 8/21/18
 */
case class BandedRasterSource(source: RasterSource, bandIndex: Int)

object BandedRasterSource extends LazyLogging {
  private val log = logger

  implicit val bandedRasterSourceSerializer: CatalystSerializer[BandedRasterSource] = new CatalystSerializer[BandedRasterSource] {
    val rsType = new RasterSourceUDT()
    override val schema: StructType = StructType(Seq(
      StructField("source", rsType.sqlType, false),
      StructField("bandIndex", IntegerType, false)
    ))

    override def to[R](t: BandedRasterSource, io: CatalystIO[R]): R = io.create(
      io.to(t.source)(RasterSourceUDT.rasterSourceSerializer),
      t.bandIndex
    )

    override def from[R](row: R, io: CatalystIO[R]): BandedRasterSource = BandedRasterSource(
      io.get[RasterSource](row, 0)(RasterSourceUDT.rasterSourceSerializer),
      io.getInt(row, 1)
    )
  }

  implicit def brsEncoder: ExpressionEncoder[BandedRasterSource] =
    CatalystSerializerEncoder[BandedRasterSource](true)
}
