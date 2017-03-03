/**
  * Created by #ScalaTeam on 02-03-2017.
  */
package exonode.clifton.signals

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

