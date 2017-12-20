"""
Created on Thu Oct 12 06:41:26 2017

@author: arhumsavera
"""

from mrjob.job import MRJob
from mrjob.job import MRStep
import mrjob
import statistics


class IndegreeCount(MRJob):
    nodes=0
    #INPUT_PROTOCOL = mrjob.protocol.ReprProtocol
    def map(self, key, value):
        """
        Output the incoming node pairs as key value pairs but only map the 'receiving' node.
        """        
        
        if not value.startswith("#"):
            pair=value.split()
            
            #yield pair[0], 1
            yield "in_"+pair[1],1

    def reduce(self, key, value):
        # a simple sum function
        count=sum(1 for _ in value)
        if count>100:
            yield None,str(key).split("in_")[1]
        
        
    def map2(self,key, value):

            yield 1,value
    
    def reduce2(self, key, value):
        # count nodes with indegree >100
        yield "Count: ",sum(1 for _ in value)
        

        

    def steps(self):
        """
        the steps can be modified to compose any number of map/reduce steps
        by including multiple instances of self.mr
        """
        return [MRStep(mapper=self.map,
                        reducer=self.reduce),MRStep(mapper=self.map2,
                        reducer=self.reduce2)]

if __name__ == '__main__':
    IndegreeCount.run()
