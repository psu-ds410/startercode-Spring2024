from mrjob.job import MRJob   

class WordCountC(MRJob):  # Choose a good class name
    def mapper(self, key, line): # when reading a text file from hdfs, key is None and value is the line of text
        words = line.split()
        for w in words:
            yield w, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    WordCountC.run()  # if you don't have these two lines, your code will not do anything 
