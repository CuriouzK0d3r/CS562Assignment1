username = vagrant

all: run

download:
	mkdir shakespeare
	wget http://www.gutenberg.org/cache/epub/100/pg100.txt
	wget http://www.gutenberg.org/cache/epub/31100/pg31100.txt
	wget http://www.gutenberg.org/cache/epub/3200/pg3200.txt
	wget http://www.gutenberg.org/cache/epub/2253/pg2253.txt
	wget http://www.gutenberg.org/cache/epub/1513/pg1513.txt
	wget http://www.gutenberg.org/cache/epub/1120/pg1120.txt
	mv *txt shakespeare/

run: clean makedir shake compile_MrManager
	hadoop jar complete.jar exercise1.MrManager /user/$(username)/stopwords/input /user/$(username)/stopwords/output /user/$(username)/stopwords/intrm

# Create the input directory.
makedir:
	hadoop fs -mkdir /user/$(username)/stopwords
	hadoop fs -mkdir /user/$(username)/stopwords/input

# Compile the Map class.
compile_map: Map.java
	mkdir -p build/org/myorg/
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* CountMap.java -d build -Xlint
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* StopwordsMap.java -d build -Xlint

# Compile the Reduce class.
compile_reduce: Reduce.java
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* CountReduce.java -d build -Xlint
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* StopwordsReduce.java -d build -Xlint

# Compile the application. The interim build lets MrManager find its dependencies.
compile_MrManager: MrManager.java
	mkdir -p build/org/myorg/
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* CountMap.java -d build -Xlint
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* CountReduce.java -d build -Xlint
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* StopwordsMap.java -d build -Xlint
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* StopwordsReduce.java -d build -Xlint
	jar -cvf mapreduce.jar -C build/ .
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:./mapreduce.jar MrManager.java -d build -Xlint
	jar -cvf complete.jar -C build/ .
	rm mapreduce.jar

# Delete the local and remote files created by the application.
clean:
	rm -rf build complete.jar mapreduce.jar
	hadoop fs -rm -f -r /user/$(username)/stopwords/

# Upload the complete works of Shakespeare as input.
shake:
	hadoop fs -put shakespeare/* /user/$(username)/stopwords/input

# Write the results to the console.
result:
	hadoop fs -cat /user/$(username)/stopwords/output/*
