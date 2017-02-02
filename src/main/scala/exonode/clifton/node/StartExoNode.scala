package exonode.clifton.node

import scala.util.Try

/**
  * Created by #ScalaTeam on 05/01/2017.
  *
  * Interface that allows users to start nodes
  * All the host spaces can be defined by the user, if they are not defined default parameters are set to localhost
  */
object StartExoNode {

  val getHelpString: String = {
    """
      |Usage:
      |  exonode [options]
      |
      |options:
      |-j, -jar ip
      |  Sets the jarHost.
      |-s, -signal ip
      |  Sets the signalHost.
      |-d, -data ip
      |  Sets the dataHost.
      |--help
      |  display this help and exit.
      |--version
      |  output version information and exit.
    """.stripMargin
  }

  def printlnExit(msg: String): Unit = {
    println(msg)
    System.exit(1)
  }

  def main(args: Array[String]): Unit = {

    var numberOfNodes: Option[Int] = None

    def setHosts(): Unit = {
      val it = args.iterator
      while (it.hasNext) {
        val cmd = it.next
        cmd match {
          case "-j" | "-jar" =>
            if (it.hasNext) SpaceCache.jarHost = it.next()
            else printlnExit(s"Command $cmd needs an argument (ip)")
          case "-s" | "-signal" =>
            if (it.hasNext) SpaceCache.signalHost = it.next()
            else printlnExit(s"Command $cmd needs an argument (ip)")
          case "-d" | "-data" =>
            if (it.hasNext) SpaceCache.dataHost = it.next()
            else printlnExit(s"Command $cmd needs an argument (ip)")
          case "-nodes" =>
            numberOfNodes = Try {
              Some(it.next().toInt)
            }.fold(_ => None, identity)
            if (numberOfNodes.isEmpty)
              printlnExit(s"Command $cmd needs an argument")
          case "--help" =>
            println(getHelpString)
            System.exit(0)
          case "--version" =>
            //FIXME get version dynamically ?
            println("Exocute version: 0.1")
            System.exit(0)
          case _ =>
            println("Unknown command: " + cmd)
        }
      }
    }

    setHosts()

    println("  ______           _   _           _         _____ _             _            \n |  ____|         | \\ | |         | |       / ____| |           | |           \n | |__  __  _____ |  \\| | ___   __| | ___  | (___ | |_ __ _ _ __| |_ ___ _ __ \n |  __| \\ \\/ / _ \\| . ` |/ _ \\ / _` |/ _ \\  \\___ \\| __/ _` | '__| __/ _ \\ '__|\n | |____ >  < (_) | |\\  | (_) | (_| |  __/  ____) | || (_| | |  | ||  __/ |   \n |______/_/\\_\\___/|_| \\_|\\___/ \\__,_|\\___| |_____/ \\__\\__,_|_|   \\__\\___|_|   \n                                                                              \n                                                                              ")

    println("SignalHost -> " + SpaceCache.signalHost)
    println("JarHost    -> " + SpaceCache.jarHost)
    println("DataHost   -> " + SpaceCache.dataHost)

    while (numberOfNodes.isEmpty) {
      print("\nSelect the number of nodes you want to start:\n>")
      val nodes = scala.io.StdIn.readLine()
      if (isValidInt(nodes))
        numberOfNodes = Some(nodes.toInt)
      else
        println("Not a valid number of nodes.")
    }
    numberOfNodes.foreach(nodes => {
      for (x <- 1 to nodes) {
        new CliftonNode().start()
        Thread.sleep((math.random() * 25).toInt)
      }

      println("Started " + nodes + " nodes.")
    })
  }

  def isValidInt(x: String): Boolean = {
    x.forall(Character.isDigit) && {
      val long: Long = x.toLong
      long <= Int.MaxValue
    }
  }

}

