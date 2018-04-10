val lines = sc.textFile("twitter.edges")
val splitRdd = lines.map(line => line.split(": "))

val yourRdd = splitRdd.flatMap( arr => {
  val text = arr(1)
  val words = text.split(",")
  words.map(word => (word, 1))
})

val wordMap = yourRdd.map(word => (word))
val wordCounts = wordMap.reduceByKey((a,b) => a+b)
val swap = wordCounts.map(item => item.swap).sortByKey(false, 1).map(item => item.swap)

val result = swap.filter(pair => pair._2 > 1000)

result.saveAsTextFile("output")
System.exit(0)