"""
Created on Thu Oct 12 06:41:26 2017

@author: arhumsavera
"""

from mrjob.job import MRJob
from mrjob.job import MRStep
import mrjob

class CountNodes(MRJob):
    #INPUT_PROTOCOL = mrjob.protocol.ReprProtocol
    def map(self, key, value):
        """
        Output incoming node pairs as 2 key,value pairs
        """
        if not value.startswith("#"):
            pair=value.split()
            
            yield pair[0], 1
            yield pair[1],1

    def reduce(self, key, value):
        # a simple sum function
        yield key,1
        
        
    def map2(self,key, value):
        
        yield (value,key)
        
    def reduce2(self,key,value):
        
        yield ("Nodes:", sum(1 for _ in value))

    def steps(self):
        return [MRStep(mapper=self.map, combiner=self.reduce,
                        reducer=self.reduce)
                        ,MRStep(mapper=self.map2,
                        reducer=self.reduce2)]

if __name__ == '__main__':
    CountNodes.run()
