"""
Created on Thu Oct 12 06:41:26 2017

@author: arhumsavera
"""

from mrjob.job import MRJob
from mrjob.job import MRStep
import mrjob
import statistics


class HopCounter(MRJob):
    nodes=0
    #INPUT_PROTOCOL = mrjob.protocol.ReprProtocol
    def map(self, k, v):
        """
        Output the incoming node pairs as key,value pairs with 'node_' prefixes and in/out markers
        """
        if not v.startswith("#"):
            pair=v.split()
            key=pair[0]
            value=pair[1]
            yield key,'o'+value
            yield value,'i'+key
            yield 'node_'+key,key
            yield 'node_'+value,value
            
    def reduce(self, key, value):
        # a simple sum function
        vals=list(value)
        #print (key+str(vals))
        ins=[]
        outs=[]
        if 'node_' in key:
            node_name=str(key).split("node_")[1]
            #print (node_name)
            yield "A_node",node_name #Get All nodes
        else:
            for val in vals:
                #print(val[0])
                if val[0]=='i':
                    ins.append(val[1:])
                elif val[0]=='o':
                    outs.append(val[1:])
            for x in ins:
                for y in outs:
                    #print("yielding: "+x+","+y)
                    yield x,y #key,value pairs of 2 hop paths
                
        
        
    def map2(self,key, value):
        yield key,value

        
    def reduce2(self,key,value):
        vals=list(set(list(value)))#remove duplicates
        #print (key+str(vals))
        yield 'nodes',len(vals) #the 'reachability' count of a node. we dont care about node name now
                                #the number of nodes in the graph
    
    
    def reduce3(self,key,value):
        vals=list(value)
        import operator
        max_index, num_nodes = max(enumerate(vals), key=operator.itemgetter(1)) #remove 'number of nodes' from list. now only reachability counts are included
        #print (num_nodes)
        del vals[max_index]
        num_remaining=num_nodes-len(vals) # number of nodes which reach 0 nodes in 2 hops
        zeroes = [0]*num_remaining
        reachable=zeroes+vals #nodes with 0 reachable nodes are now accounted for
        #print (reachable)
        #print (len(reachable))
        yield "average: ", sum(reachable)/num_nodes
        yield "median: ",int(statistics.median(reachable))


        

    def steps(self):
        """
        the steps can be modified to compose any number of map/reduce steps
        by including multiple instances of self.mr
        """
        return [MRStep(mapper=self.map,
                        reducer=self.reduce)
                        ,MRStep(mapper=self.map2,
                        reducer=self.reduce2)
                        ,MRStep(reducer=self.reduce3)]

if __name__ == '__main__':
    HopCounter.run()
