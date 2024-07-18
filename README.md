# Yet Another Distributed File-System YADFS

This project tries to replicate the main functionality of HDFS which is distribution of data blocks into multiple data nodes via NameNode.

You can run the NameNode and then DataNode by using:

python3 namenode.py

python3 datanode.py 

python3 datanode1.py

python3 datanode2.py

It performs 2 operation:

1)Uploading of the file into blocks.

2)Downloading of the blocks from thier respective datanodes

3)Listing of the datanodes
