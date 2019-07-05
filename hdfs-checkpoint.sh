#!/bin/bash
#
# Use this script carefully. Checking point HDFS requires the following:
#   - Enter safe mode
#   - Stop Secondary Namenode
# Checking point is a I/O & network heavy operation, especially for large HDFS cluster.

FSIMAGE_DIR=/data/cdh/dfs/snn/current
OIV_OUT_DIR=/tmp

# Save name space on primary Namenode. We save latest metadata to the fsimage file.
echo "Save name space on primary Namenode."
hdfs dfsadmin -safemode enter
hdfs dfsadmin -saveNamespace
hdfs dfsadmin -safemode leave

# Check point name space on secondary Namenode.
echo "Checking point"
echo "Stop Secondary NameNode service from Cloudera Manager/Ambari UI"
echo
echo "Once SNN is stopped, run the following to check point."
echo "hdfs secondarynamenode -checkpoint force"
echo
echo "Start SNN after checkpoint is done."

# Convert binary Namenode fsiamge file to text format using the OIV tool.
echo "Convert binary Namenode fsiamge file to text format."
echo "Saving output to $OIV_OUT_DIR/fsimage.xml"
LATEST_FSIMAGE=`ls -tr $FSIMAGE_DIR/fsimage_* | grep -v 'md5' | tail -1`
hdfs oiv -i $LATEST_FSIMAGE -o $OIV_OUT_DIR/fsimage.xml -p XML

echo "Done."
