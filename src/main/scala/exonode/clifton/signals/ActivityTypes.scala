/**
  * Created by #GrowinScala
  */
package exonode.clifton.signals

/**
  * Created by #GrowinScala
  */
sealed trait ActivityType

case object ActivityMapType extends ActivityType {
  override def toString = "map"
}

case object ActivityFlatMapType extends ActivityType {
  override def toString = "flatMap"
}

case object ActivityFilterType extends ActivityType {
  override def toString = "filter"
}

