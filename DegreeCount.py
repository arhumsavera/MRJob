"""
Created on Thu Oct 12 06:41:26 2017

@author: arhumsavera
"""

from mrjob.job import MRJob
from mrjob.job import MRStep
import mrjob
import statistics


class DegreeCount(MRJob):
    nodes=0
    #INPUT_PROTOCOL = mrjob.protocol.ReprProtocol
    def map(self, k, v):
        """
        Output the incoming node pairs as key,value pairs with 'in_','out_' and 'node_' prefixes
        """
        if not v.startswith("#"):
            pair=v.split()
            key=pair[0]
            value=pair[1]
            yield "out_"+str(key), 1
            yield "in_"+str(value),1
            yield "node_"+str(key), 1
            yield "node_"+str(value),1

    def reduce(self, key, value):
        # a simple sum function
        yield key,sum(1 for _ in value)
        
        
    def map2(self,key, value):
        
        #k= "in" if "in" in key else "out"
        if "in" in key:
            yield "in",value
        elif "out" in key:
            yield "out",value
        elif "node_" in key:
            yield "A_node",str(key).split("node_")[1]
        

        
    def reduce2(self,key,value):
        if "node" in key:
            yield key,sum(1 for _ in value) #length 
        else:
            #print (list(value))
            v=list(value)
            yield key,v
            yield "A_node",v
            
    
    
    def reduce3(self,key,value):
        if "node" in key:
            node_vals=list(value)
            num_nodes=node_vals[0]
            node_list=node_vals[1]
            #print (num_nodes)
            #print ("sum:"+str(sum(node_list)))
            yield "average: ", sum(node_list)/num_nodes
            #print (self.nodes)
        else:
            vals=list(value)[0]
            #print (vals)
            #yield key,vals
            yield "median_"+key,int(statistics.median(vals))


        

    def steps(self):
        """
        the steps can be modified to compose any number of map/reduce steps
        by including multiple instances of self.mr
        """
        return [MRStep(mapper=self.map,
                        reducer=self.reduce)
                        ,MRStep(mapper=self.map2,
                        reducer=self.reduce2),
                        MRStep(reducer=self.reduce3)]

if __name__ == '__main__':
    DegreeCount.run()
