package com.bsy.datalake.reo.utility

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.Types
import java.util.{HashSet => JHashSet, Set => JSet}

/**
 * Lignes d'une partition Spark lues par SQLServerBulkCopy (classe reprise du projet PUSHSQLSERVER).
 *
 * @param rows             lignes de la partition
 * @param schema           schema Spark des lignes
 * @param targetIsNumeric  par colonne : colonne cible SQL Server numerique. Une chaine vide ou non numerique y est
 *                         envoyee null (sinon "Error converting data type varchar to numeric")
 * @param maxStringLengths par colonne : CHARACTER_MAXIMUM_LENGTH de la colonne cible (N > 0 : chaine tronquee a N
 *                         caracteres ; 0 : colonne non caractere ; -1 : varchar(max))
 *
 * Les chaines sont declarees LONGNVARCHAR (nvarchar(max)) : avec VARCHAR(N), le pilote annonce N*2 octets (UTF-16)
 * et SQL Server rejette la colonne ("Received an invalid column length from the bcp client").
 */
class SparkIteratorBulkData(rows: Iterator[Row],
                            schema: StructType,
                            targetIsNumeric: Array[Boolean],
                            maxStringLengths: Array[Int]) extends ISQLServerBulkData {

  private val fields = schema.fields
  private val numFields = fields.length
  private var currentRow: Row = _
  // Tampon reutilise : SQLServerBulkCopy consomme chaque ligne avant d'appeler next().
  private val rowBuffer = new Array[AnyRef](numFields)

  private def stringToNumeric(idx: Int): Boolean = fields(idx).dataType == StringType && targetIsNumeric(idx)

  override def getColumnOrdinals: JSet[Integer] = {
    val ordinals = new JHashSet[Integer]()
    (1 to numFields).foreach(i => ordinals.add(Integer.valueOf(i)))
    ordinals
  }

  override def getColumnName(column: Int): String = fields(column - 1).name

  override def getColumnType(column: Int): Int = {
    val idx = column - 1
    if (stringToNumeric(idx)) {
      Types.DECIMAL
    } else {
      fields(idx).dataType match {
        case IntegerType => Types.INTEGER
        case LongType => Types.BIGINT
        case ShortType => Types.SMALLINT
        case ByteType => Types.TINYINT
        case StringType => Types.LONGNVARCHAR
        case FloatType => Types.REAL
        case DoubleType => Types.DOUBLE
        case BooleanType => Types.BIT
        case DateType => Types.DATE
        case TimestampType => Types.TIMESTAMP
        case _: DecimalType => Types.DECIMAL
        case BinaryType => Types.VARBINARY
        case _ => Types.LONGNVARCHAR
      }
    }
  }

  override def getPrecision(column: Int): Int = {
    val idx = column - 1
    if (stringToNumeric(idx)) 38
    else fields(idx).dataType match {
      case dt: DecimalType => dt.precision
      case _ => 0
    }
  }

  override def getScale(column: Int): Int = {
    val idx = column - 1
    if (stringToNumeric(idx)) 6
    else fields(idx).dataType match {
      case dt: DecimalType => dt.scale
      case _ => 0
    }
  }

  override def next(): Boolean =
    if (rows.hasNext) {
      currentRow = rows.next()
      true
    } else {
      false
    }

  override def getRowData: Array[AnyRef] = {
    var i = 0
    while (i < numFields) {
      rowBuffer(i) =
        if (currentRow.isNullAt(i)) {
          null
        } else {
          fields(i).dataType match {
            case StringType =>
              val s = currentRow.getString(i)
              if (s.trim.isEmpty) null
              else if (targetIsNumeric(i)) SparkIteratorBulkData.parseDecimal(s.trim)
              else {
                val maxLen = maxStringLengths(i)
                if (maxLen > 0 && s.length > maxLen) s.substring(0, maxLen) else s
              }
            case IntegerType => java.lang.Integer.valueOf(currentRow.getInt(i))
            case LongType => java.lang.Long.valueOf(currentRow.getLong(i))
            case DoubleType => java.lang.Double.valueOf(currentRow.getDouble(i))
            case FloatType => java.lang.Float.valueOf(currentRow.getFloat(i))
            case ShortType => java.lang.Short.valueOf(currentRow.getShort(i))
            case ByteType => java.lang.Byte.valueOf(currentRow.getByte(i))
            case BooleanType => java.lang.Boolean.valueOf(currentRow.getBoolean(i))
            case _: DecimalType => currentRow.getDecimal(i)
            case DateType => currentRow.getDate(i)
            case TimestampType => currentRow.getTimestamp(i)
            case BinaryType => currentRow.get(i).asInstanceOf[Array[Byte]]
            case _ => currentRow.get(i).asInstanceOf[AnyRef]
          }
        }
      i += 1
    }
    rowBuffer
  }
}

object SparkIteratorBulkData {
  /** BigDecimal, ou null si la chaine n'est pas un nombre (SQL Server recoit alors null). */
  def parseDecimal(s: String): java.math.BigDecimal =
    try new java.math.BigDecimal(s)
    catch {
      case _: NumberFormatException => null
    }
}
