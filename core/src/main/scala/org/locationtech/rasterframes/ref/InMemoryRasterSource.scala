/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, MultibandTile, Raster, Tile}
import geotrellis.raster.io.geotiff.Tags
import geotrellis.vector.Extent
import org.locationtech.rasterframes.ref.RasterSource.EMPTY_TAGS
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

case class InMemoryRasterSource(tile: Tile, extent: Extent, crs: CRS) extends RasterSource {
  def this(prt: ProjectedRasterTile) = this(prt, prt.extent, prt.crs)

  override def rows: Int = tile.rows

  override def cols: Int = tile.cols

  override def cellType: CellType = tile.cellType

  override def bandCount: Int = 1

  override def tags: Tags = EMPTY_TAGS

  override protected def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    bounds
      .map(b => {
        val subext = rasterExtent.extentFor(b)
        Raster(MultibandTile(tile.crop(b)), subext)
      })
      .toIterator
  }
}