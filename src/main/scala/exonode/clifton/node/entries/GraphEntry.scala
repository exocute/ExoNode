package exonode.clifton.node.entries

/**
  * Created by #GrowinScala
  *
  * Entry that represents a graph in the FlySpace.
  * The analyser node will actively search for this entries to know which graphs are still valid.
  */
case class GraphEntry(graphId: String, activities: Vector[String])
