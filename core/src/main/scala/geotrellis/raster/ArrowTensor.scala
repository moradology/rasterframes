package geotrellis.raster

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

import com.google.flatbuffers.FlatBufferBuilder
import org.apache.arrow.flatbuf.{Buffer, Tensor, TensorDim, Type}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{Float8Vector, VectorSchemaRoot}
import spire.syntax.cfor._

import scala.collection.JavaConverters._

case class ArrowTensor(val vector: Float8Vector, val shape: Seq[Int]) extends CellGrid {
  // TODO: Should we be using ArrowBuf here directly, since Arrow Tensor can not have pages?
  def rows = shape(1)
  def cols = shape(2)
  val cellType = DoubleCellType

  // TODO: Figure out how to work this crazy thing
  // def copy(implicit alloc: BufferAllocator) = {
  //   val n = vector.getValueCount
  //   val copied = new Float8Vector("array", alloc)
  //   val tp = vector.makeTransferPair(copied)
  //   tp.copyValueSafe(0, n)
  //   copied.copyFromSafe(0, n, vector)
  //   copied.setValueCount(n)
  //   ArrowTensor(copied, shape)
  // }

  def map(fn: Double => Double): ArrowTensor = {
    val n = vector.getValueCount
    val result = new Float8Vector("array", ArrowTensor.allocator)
    result.allocateNew(n)
    result.setValueCount(n)
    cfor(0)(_ < n, _ + 1) { i =>
      if (vector.isSet(i) == 1)
        result.set(i, fn(vector.get(i)))
    }
    ArrowTensor(result, shape)
  }

  def zipWith(other: ArrowTensor)(fn: (Double, Double) => Double): ArrowTensor = {
    if (other.shape != shape)
      throw new IllegalArgumentException(s"Cannot zip tensors of differing sizes.  Got arguments of shape ${shape.mkString("×")} and ${other.shape.mkString("×")}")

    val n = vector.getValueCount
    val result = new Float8Vector("array", ArrowTensor.allocator)
    result.allocateNew(n)
    result.setValueCount(n)
    cfor(0)(_ < n, _ + 1){ i =>
      if (vector.isNull(i) || other.vector.isNull(i))
        result.setNull(i)
      else
        result.set(i, fn(vector.get(i), other.vector.get(i)))
    }
    ArrowTensor(result, shape)
  }

  /** Write Tensor to buffer, return offset of Tensor object in that buffer */
  def writeTensor(bufferBuilder: FlatBufferBuilder): Int = {
    val elementSize = 8

    // TODO: make work for more than 2 dimensions
    // Array[Long](shape(0) * elementSize, elementSize)
    val strides: Array[Long] = {
      shape.reverse.map(_.toLong).toArray
    }

    val shapeOffset: Int = {
      val rank = shape.length
      val tensorDimOffsets = new Array[Int](rank)
      val nameOffset = new Array[Int](rank)

      for (i <- shape.indices) {
        nameOffset(i) = bufferBuilder.createString("")
        tensorDimOffsets(i) = TensorDim.createTensorDim(bufferBuilder, shape(i), nameOffset(i))
      }

      Tensor.createShapeVector(bufferBuilder, tensorDimOffsets)
    }

    val typeOffset = org.apache.arrow.flatbuf.Int.createInt(bufferBuilder, 32,true)

    val stridesOffset = Tensor.createStridesVector(bufferBuilder, strides)
    Tensor.startTensor(bufferBuilder)
    Tensor.addTypeType(bufferBuilder, Type.Int)
    // pa.read_tensor also wants type, ND4j does not write this because it I'm guessing its not written to python
    Tensor.addType(bufferBuilder, typeOffset)
    Tensor.addShape(bufferBuilder, shapeOffset)
    Tensor.addStrides(bufferBuilder, stridesOffset)
    // Buffers offset is relative to memory page, not the IPC message.
    val tensorBodyOffset: Int = 0
    val tensorBodySize: Int = vector.getValueCount * 8
    val dataOffset = Buffer.createBuffer(bufferBuilder, tensorBodyOffset, tensorBodySize)
    Tensor.addData(bufferBuilder, dataOffset)
    Tensor.endTensor(bufferBuilder)
  }

  def toIpcMessage(): ByteBuffer = {
    val bufferBuilder = new FlatBufferBuilder(512)
    val tensorOffset = writeTensor(bufferBuilder)
    val tensorBodySize: Int = vector.getValueCount * 8

    MessageSerializer.serializeMessage(bufferBuilder, org.apache.arrow.flatbuf.MessageHeader.Tensor, tensorOffset, tensorBodySize);
  }

  def toArrowBytes(): Array[Byte] = {
    val bb = toIpcMessage()
    val bout = new ByteArrayOutputStream()
    val wbc = new WriteChannel(Channels.newChannel(bout))
    MessageSerializer.writeMessageBuffer(wbc, bb.remaining(), bb)
    // wbc.align
    wbc.write(vector.getDataBuffer)
    wbc.close()
    bout.toByteArray
  }

}

object ArrowTensor {
  val allocator = new RootAllocator(Long.MaxValue)

  val schema: Schema = {
    val fieldArr = new Field(
      "array",
      new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null, null),
      Nil.asJava)

    new Schema(List(fieldArr).asJava)
  }

  def fromArray(arr: Array[Double], shape: Int*): ArrowTensor = {
    val root = VectorSchemaRoot.create(schema, allocator)
    val vec = new Float8Vector("array", allocator)
    vec.allocateNew(arr.length)
    for (i <- arr.indices) vec.set(i, arr(i))
    vec.setValueCount(arr.length)
    root.setRowCount(arr.length)
    new ArrowTensor(vec, shape.toArray)
  }

  def fill(v: Double, shape: Int*)(implicit alloc: BufferAllocator): ArrowTensor = {
    val vec = new Float8Vector("array", alloc)
    val shp = shape.toSeq
    val n = shp.product
    vec.allocateNew(n)
    vec.setValueCount(n)
    cfor(0)(_ < n, _ + 1){ i =>
      vec.set(i, v)
    }
    ArrowTensor(vec, shp)
  }

  def fromArrowMessage(bytes: Array[Byte]): ArrowTensor = {
    val is = new ByteArrayInputStream(bytes)
    val channel = Channels.newChannel(is)
    val readChannel = new ReadChannel(channel)
    val msg = MessageSerializer.readMessage(readChannel)
    //println("msg BB: " + msg.getMessageBuffer)

    // TODO: use tensor information to build the right kind of tensor
    val tensor = new Tensor()
    msg.getMessage.header(tensor)

    val root = VectorSchemaRoot.create(schema, allocator)
    val vec = new Float8Vector("array", allocator)
    def shape = {
      for (i <- 0 until tensor.shapeLength()) yield tensor.shape(i).size().toInt
    }.toArray

    val tensorSize = shape.product

    val arrowBuf = MessageSerializer.readMessageBody(readChannel, msg.getMessageBodyLength.toInt, allocator)
    vec.setValueCount(tensorSize)
    // TODO: find a way to reference this buffer directly, this is obviously horrible
    for (i <- 0 until tensorSize) vec.set(i, Float8Vector.get(arrowBuf, i))

    new ArrowTensor(vec, shape)
  }
}
