# NGA-Assignment

It is an akka-streams(alpakka) based application to read all csv files containing sensor data from a directory. The path to the directory is required to be passed
as an argument wile running application.
It produces following stats(sample) 


>Num of processed files: 2\
Num of processed measurements: 7\
Num of failed measurements: 2

>Sensors with highest avg humidity:\
sensor-id,min,avg,max\
s1,10,54,98\
s2,78,83,88\
s3,NaN,NaN,NaN


## To run the application

```sh
sbt "run <path to directory with csv files>"
```

example:

```sh
sbt "run C:\Users\barthwal\SensorData"
``` 