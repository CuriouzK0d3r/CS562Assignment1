run: clean makedir upload compile_MrManager
	hadoop jar complete.jar excercise3.MrManager /user/cloudera/stopwords/input /user/cloudera/stopwords/output /user/cloudera/stopwords/intrm /user/cloudera/stopwords/stopwords.csv

# Create the input directory.
makedir:
	hadoop fs -mkdir /user/cloudera/stopwords 
	hadoop fs -mkdir /user/cloudera/stopwords/input


compile_other: DocumentWritable.java
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocumentWritable.java -d build -Xlint

# Compile the application. The interim build lets MrManager find its dependencies. 
compile_MrManager: MrManager.java
	mkdir -p build/excercise3/
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* UniqueTermsReduce.java -d build -Xlint
#	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocumentWritable.java -d build -Xlint
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* UniqueTermsMap.java -d build -Xlint
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* IndexerMap.java -d build -Xlint
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* IndexerReduce.java -d build -Xlint
	
	jar -cvf mapreduce.jar -C build/ .
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:./mapreduce.jar MrManager.java -d build -Xlint
	jar -cvf complete.jar -C build/ .
	rm mapreduce.jar

# Delete the local and remote files created by the application.
clean:
	rm -rf build complete.jar mapreduce.jar
	hadoop fs -rm -f -r /user/cloudera/stopwords/

# Upload the complete works of Shakespeare as input.
upload:
	hadoop fs -put shakespeare/* /user/cloudera/stopwords/input
	hadoop fs -put stopwords.csv /user/cloudera/stopwords/stopwords.csv

# Write the results to the console.
showResult:
	hadoop fs -cat /user/cloudera/stopwords/output/*
