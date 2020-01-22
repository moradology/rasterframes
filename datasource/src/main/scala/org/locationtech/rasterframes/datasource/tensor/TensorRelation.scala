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

package org.locationtech.rasterframes.datasource.tensor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.locationtech.rasterframes.datasource.tensor.TensorDataSource.RasterSourceCatalogRef
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.accessors.{GetCRS, GetExtent}
import org.locationtech.rasterframes.expressions.generators.{RasterSourcesToTensorRef, RasterSourceToTiles}
import org.locationtech.rasterframes.expressions.generators.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.expressions.transformers.{RasterRefToTile, URIToRasterSource, XZ2Indexer}
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/**
  * Constructs a Spark Relation over one or more RasterSource paths.
  * @param sqlContext Query context
  * @param catalogTable Specification of raster path sources
  * @param bandIndexes band indexes to fetch
  * @param subtileDims how big to tile/subdivide rasters info
  * @param lazyTiles if true, creates a lazy representation of tile instead of fetching contents.
  * @param spatialIndexPartitions Number of spatial index-based partitions to create.
  *                               If Option value > 0, that number of partitions are created after adding a spatial index.
  *                               If Option value <= 0, uses the value of `numShufflePartitions` in SparkContext.
  *                               If None, no spatial index is added and hash partitioning is used.
  */
case class TensorRelation(
  sqlContext: SQLContext,
  rspaths: Seq[String],
  subtileDims: Option[TileDimensions],
  spatialIndexPartitions: Option[Int]
) extends BaseRelation with TableScan {

  // lazy val inputColNames = catalogTable.bandColumnNames
  // println("bands", inputColNames)

  // def pathColNames = inputColNames
  //   .map(_ + "_path")

  // def srcColNames = inputColNames
  //   .map(_ + "_src")

  // def refColNames = srcColNames
  //   .flatMap(bandNames(_, bandIndexes))
  //   .map(_ + "_ref")

  // def tileColNames = inputColNames
  //   .flatMap(bandNames(_, bandIndexes))

  // lazy val extraCols: Seq[StructField] = {
  //   val catalog = sqlContext.table(catalogTable.tableName)
  //   catalog.schema.fields.filter(f => !catalogTable.bandColumnNames.contains(f.name))
  // }

  // lazy val indexCols: Seq[StructField] =
  //   if (spatialIndexPartitions.isDefined) Seq(StructField("spatial_index", LongType, false)) else Seq.empty

  protected def defaultNumPartitions: Int =
    sqlContext.sparkSession.sessionState.conf.numShufflePartitions

  override def schema: StructType = {
    // val tileSchema = schemaOf[ProjectedRasterTile]
    // val paths = for {
    //   pathCol <- pathColNames
    // } yield StructField(pathCol, StringType, false)
    // val tiles = for {
    //   tileColName <- tileColNames
    // } yield StructField(tileColName, tileSchema, true)

    // StructType(paths ++ tiles ++ extraCols ++ indexCols)
    ???
  }

  override def buildScan(): RDD[Row] = {
    import sqlContext.implicits._
    val numParts = spatialIndexPartitions.filter(_ > 0).getOrElse(defaultNumPartitions)

    import  org.locationtech.rasterframes.ref._
    import java.net.URI
    val inputs = rspaths
      .map(URI.create)
      .map(RasterSource.apply).flatMap {rs =>
        0 until rs.bandCount map { band =>
          (rs, band)
        }
      }

    val df: DataFrame = {
      // Expand RasterSource into multiple columns per band, and multiple rows per tile
      // There's some unintentional fragility here in that the structure of the expression
      // is expected to line up with our column structure here.
      val srcs = ???
      val bandIndexes = ???
      val refColNames = ???
      val refs = RasterSourcesToTensorRef(subtileDims, bandIndexes, srcs: _*) as refColNames

      // RasterSourceToRasterRef is a generator, which means you have to do the Tile conversion
      // in a separate select statement (Query planner doesn't know how many columns ahead of time).
      // val refsToTiles = for {
      //   (refColName, tileColName) <- refColNames.zip(tileColNames)
      // } yield RasterRefToTile(col(refColName)) as tileColName

      // withPaths
      //   .select(extras ++ paths :+ refs: _*)
      //   .select(paths ++ refsToTiles ++ extras: _*)
      ???
    }

    if (spatialIndexPartitions.isDefined) {
      val tileColNames: Seq[String] = ???
      val sample = col(tileColNames.head)
      val indexed = df
        .withColumn("spatial_index", XZ2Indexer(GetExtent(sample), GetCRS(sample)))
        .repartitionByRange(numParts,$"spatial_index")
      indexed.rdd
    }
    else df.rdd
  }
}
