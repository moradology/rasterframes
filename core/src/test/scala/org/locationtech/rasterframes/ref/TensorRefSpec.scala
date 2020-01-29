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

import java.net.URI

import geotrellis.raster.{GridBounds}
import geotrellis.vector.Extent
import org.apache.spark.SparkException
import org.apache.spark.sql.Encoders
import org.locationtech.rasterframes.{TestEnvironment, _}
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.generators._
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/**
 *
 *
 * @since 8/22/18
 */
class TensorRefSpec extends TestEnvironment with TestData {

  describe("Buffered Gridbounds") {
    it("should add properly calculate extra buffering cols/rows at the edges of a tiled read") {
      import TensorRef.bufferedCropBounds
      val rsTotalBounds = GridBounds(0, 0, 1024, 1024)
      assert(bufferedCropBounds(rsTotalBounds, rsTotalBounds, 3) == GridBounds(-3, -3, 1027, 1027))

      val middleBounds = GridBounds(10, 10, 1000, 1000)
      assert(bufferedCropBounds(rsTotalBounds, middleBounds, 3) == GridBounds(0, 0, 990, 990))

      val leftBounds = GridBounds(0, 10, 1000, 1000)
      assert(bufferedCropBounds(rsTotalBounds, leftBounds, 3) == GridBounds(-3, 0, 1000, 990))
      
      val rightBounds = GridBounds(10, 10, 1024, 1000)
      assert(bufferedCropBounds(rsTotalBounds, rightBounds, 3) == GridBounds(0, 0, 1017, 990))
      
      val upBounds = GridBounds(10, 10, 1000, 1024)
      assert(bufferedCropBounds(rsTotalBounds, upBounds, 3) == GridBounds(0, 0, 990, 1017))
      
      val downBounds = GridBounds(10, 0, 1000, 1000)
      assert(bufferedCropBounds(rsTotalBounds, downBounds, 3) == GridBounds(0, -3, 990, 1000))
    }
  }
}
