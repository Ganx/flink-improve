package com.flink.dim

import org.apache.flink.table.sources.StreamTableSource
import com.flink.dim.CacheType.{CacheTypeBase, LRU}
/**
  * @author cj
  * @since 2020/3/23 17:16 
  */
trait DimensionTableSource[T] extends StreamTableSource[T] with Serializable {

  import com.flink.dim.externalconnectors.DBConnection

  private[this] var _primaryKey: Array[String] = _

  private[this] var _cacheType: CacheTypeBase = LRU

  private[this] var _cacheTTLMinutes: Int = _

  private[this] var _cacheMaximumSize: Int = _

  private[this] var _enableRemoveLog = false

  private[this] var _timers: Array[String] = _

  private[this] var _enableCleanup = false

  protected var _DBConnection: DBConnection

  def primaryKey: Array[String] = _primaryKey.distinct

  def primaryKeysIndices:Array[Int] = {
    getReturnType
  }
}

object CacheType{
  sealed trait CacheTypeBase

  case object ALL extends CacheTypeBase

  case object LRU extends CacheTypeBase

  case object NONE extends CacheTypeBase
}
