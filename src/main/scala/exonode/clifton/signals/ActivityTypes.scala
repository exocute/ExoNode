package exonode.clifton.signals

/**
  * Created by #GrowinScala
  */
sealed trait ActivityType

case object ActivityMapType extends ActivityType {
  override def toString = "Activity"
}

case object ActivityFilterType extends ActivityType {
  override def toString = "ActivityFilter"
}

case object ActivityFlatMapType extends ActivityType {
  override def toString = "ActivityFlatMap"
}
