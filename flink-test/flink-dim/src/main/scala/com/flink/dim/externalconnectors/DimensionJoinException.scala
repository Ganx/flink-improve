package com.flink.dim.externalconnectors

class DimensionJoinException(message: String, e: Throwable) extends Throwable(message, e){
  def this(message: String) {
    this(message, null)
  }
}
