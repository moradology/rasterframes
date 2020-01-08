package org.locationtech.rasterframes.expressions

import geotrellis.raster.ArrowTensor
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.util._
import spire.syntax.cfor.cfor

case class StackTensors(override val children: Seq[Expression]) extends Expression with CodegenFallback {

  override def nodeName: String = "rf_stack_tensors"
  override def dataType: DataType = new TensorUDT
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    if (input == null) null
    else {
      val tensors = Array.ofDim[ArrowTensor](children.length)
      cfor(0)(_ < tensors.length, _ + 1) { i =>
        val child = children(i)
        val row = child.eval(input).asInstanceOf[InternalRow]
        tensors(i) =
          if (row != null)
            DynamicExtractors.tensorExtractor(child.dataType)(row)._1
          else
            null  // TODO: is it better to throw here?
      }
      dataType.asInstanceOf[TensorUDT].serialize(ArrowTensor.stackTensors(tensors.filter(_ != null)))
    }
  }
}

object StackTensors {

  def apply(cols: Seq[Column]): Column = {
    val stacker = new StackTensors(cols.map(_.expr))
    new Column(stacker).as("rf_stack_tensors")
  }

}
