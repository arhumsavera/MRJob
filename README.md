# MRJob
Solving some basic graph statistics using MRJob, which is a library for developing mapreduce jobs. For installation, refer to https://pythonhosted.org/mrjob/guides/quickstart.html#installation

This code works with a collection of e-mail data downloadable from: https://snap.stanford.edu/data/email-EuAll.txt.gz The data forms a graph G of e-mails between users, with each line being of the form sender receiver. The files compute the following on G:

• Number of nodes in the graph <br>
• Average (and median) indegree and out degree <br>
• Average (and median) number of nodes reachable in two hops <br>
• Number of nodes with indegree > 100 <br>

To run the code: <br>
CountNodes.py email-EuAll.txt

