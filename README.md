# ExoNode

Scala Version of ExoNode Plataform 

-------------------------------------------------------------------------------
An ExoNode is an amount of logic coded and running in a closed loop inside a single thread which is
able to interpret the Space it is connected to and to pick-up an Activity and execute it when the
corresponding inputs are available.

For more information about the Exonode: http://bit.ly/2mBHBNd

## Releases
10/04/2017 <br />
--> **ExoNode v1.1** <br />

02/03/2017 <br />
--> **ExoNode v1.0** <br />

How to use? <br />
To launch the app run ExoNode.jar
  ```
java -jar ExoNode.jar [options]
```
Options available: <br />


| Option            | Meaning                                |
| ---               | ---                                    |
| `-s`              | sets signal space                      |
| `-d  `            | sets data space                        |
| `-j `             | sets jar space                         |
| `-nodes N `       | launchs N nodes into the space         |
| `-cleanspaces`    | cleans all the entries for every space | 
| `--help    `      | display help and exit                  | 
| `--version`       | ouput version information and exit     | 


-------------------------------------------------------------------------------

## Specs

###Versions and Programs used 

 
| Software       | Version       | Link                                   |
| ---------------|:-------------:| --------------------------------------:|
| Scala          | 2.12.1        |                                        |
| Java           | 1.8           | http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html                                       |
| FlyObjectSpace | 2.2.0      |  https://github.com/fly-object-space/fly-scala   |

