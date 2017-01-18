package exonode.clifton.node

import java.util

/**
  * Created by #ScalaTeam on 18-01-2017.
  */
trait FlyOption {

  def write(entry: AnyRef, leaseTime: Long): Long

  def writeMany[T](entries: util.Collection[T], leaseTime: Long): Long

  def read[T](template: T, waitTime: Long): Option[T]

  def readMany[T](template: T, matchLimit: Long): Iterable[T]

  def take[T](template: T, waitTime: Long): Option[T]

  def takeMany[T](template: T, matchLimit: Long): Iterable[T]
}
