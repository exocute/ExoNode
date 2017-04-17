package exonode.clifton.node.entries

/**
  * Created by #GrowinScala
  */
case class FlatMapEntry(graphId: String, orderId: String,
                        // needs to be an Integer from Java to allow null values
                        size: java.lang.Integer) {
}

object FlatMapEntry {
  def fromInjectId(injectId: String, orderId: String, size: Int): FlatMapEntry = {
    FlatMapEntry(injectId.substring(0, injectId.indexOf(":")), orderId, size)
  }
}