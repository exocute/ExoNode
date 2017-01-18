package exonode.clifton.node

import java.util

import com.zink.fly.Fly

import scala.collection.JavaConverters._

/**
  * Created by #ScalaTeam on 18-01-2017.
  */
class FlyScala(fly: Fly) extends FlyOption {

  override def write(entry: AnyRef, leaseTime: Long): Long =
    fly.write(entry, leaseTime)

  override def writeMany[T](entries: util.Collection[T], leaseTime: Long): Long =
    fly.writeMany(entries, leaseTime)

  override def read[T](template: T, waitTime: Long): Option[T] =
    Option(fly.read(template, waitTime))

  override def readMany[T](template: T, matchLimit: Long): Iterable[T] =
    fly.readMany(template, matchLimit).asScala

  override def take[T](template: T, waitTime: Long): Option[T] =
    Option(fly.take(template, waitTime))

  override def takeMany[T](template: T, matchLimit: Long): Iterable[T] =
    fly.takeMany(template, matchLimit).asScala

}

object FlyScala {
  def apply(fly: Fly): FlyScala = new FlyScala(fly)
}