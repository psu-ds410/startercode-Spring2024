from mrjob.job import MRJob   

class City(MRJob):  # Choose a good class name
    def mapper(self, key, line): # when reading a text file from hdfs, key is None and value is the line of text
        parts = line.split("\t")
        city = parts[0]
        state = parts[2]
        population = parts[4]
        zipcodes = parts[5].split(" ") # parts[5] is a list of space separated zip codes
        yield state, (population, len(zipcodes))

    def reducer(self, state, values):
        num_cities = len(values)
        for (p,z) in values:
            population += p # update the population
            maxzips = max(maxzips, z) # update the maximum number of zip codes in a city in the state.
        yield state, (num_cities, population, maxzips)

if __name__ == '__main__':
    City.run()  # if you don't have these two lines, your code will not do anything 
