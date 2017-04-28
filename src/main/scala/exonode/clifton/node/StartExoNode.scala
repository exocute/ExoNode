package exonode.clifton.node

import scala.util.Try

/**
  * Created by #GrowinScala
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
      |-cleanspaces
      |  Clean all information from the spaces.
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
    var shouldClean = false

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
          case "-cleanspaces" =>
            shouldClean = true
          case "--help" =>
            println(getHelpString)
            System.exit(0)
          case "--DEBUG" =>
            CliftonNode.DEBUG = true
          case "--version" =>
            //FIXME get version dynamically ?
            println("ExoNode version: 1.2")
            System.exit(0)
          case _ =>
            println("Unknown command: " + cmd)
        }
      }
    }

    setHosts()
    if (shouldClean)
      SpaceCache.cleanAllSpaces()

    println("  ______           _   _           _         _____ _             _            \n |  ____|         | \\ | |         | |       / ____| |           | |           \n | |__  __  _____ |  \\| | ___   __| | ___  | (___ | |_ __ _ _ __| |_ ___ _ __ \n |  __| \\ \\/ / _ \\| . ` |/ _ \\ / _` |/ _ \\  \\___ \\| __/ _` | '__| __/ _ \\ '__|\n | |____ >  < (_) | |\\  | (_) | (_| |  __/  ____) | || (_| | |  | ||  __/ |   \n |______/_/\\_\\___/|_| \\_|\\___/ \\__,_|\\___| |_____/ \\__\\__,_|_|   \\__\\___|_|   \n                                                                              \n                                                                              ")

    println("SignalHost -> " + SpaceCache.signalHost)
    println("JarHost    -> " + SpaceCache.jarHost)
    println("DataHost   -> " + SpaceCache.dataHost)

    while (numberOfNodes.isEmpty) {
      print("\nSelect the number of nodes you want to start:\n>")
      val nodes = scala.io.StdIn.readLine()
      if (isValidNatNumber(nodes))
        numberOfNodes = Some(nodes.toInt)
      else
        println("Not a valid number of nodes.")
    }

    for (_ <- 1 to numberOfNodes.get) {
      new CliftonNode().start()
      Thread.sleep((math.random() * 25).toInt)
    }

    println("Started " + numberOfNodes.get + " nodes.")
  }

  private def isValidNatNumber(str: String): Boolean = {
    str.forall(Character.isDigit) && Try {
      val long: Long = str.toLong
      long >= 0 && long <= Int.MaxValue
    }.fold(_ => false, identity)
  }

}

