# Restart the SSH service to apply changes
sudo /etc/init.d/ssh restart

# Source the start-all.sh script to initiate the Hadoop daemons
source $HADOOP_HOME/sbin/start-all.sh