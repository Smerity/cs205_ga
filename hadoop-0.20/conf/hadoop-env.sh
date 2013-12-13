# Set Hadoop-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME="${JAVA_HOME:-/opt/jdk1.6.0_37}"

# The Kerberos client configuration to use. Required.
export KRB5_CONFIG="/etc/krb5.conf"

# convert relative path to absolute path
common_bin=`dirname "$this"`
script=`basename "$this"`
common_bin=`cd "$common_bin"; pwd`
this="$common_bin/$script"
# the root of the Hadoop installation
#TODO: change the env variable when dir structure is changed
export HADOOP_HOME=`dirname "$this"`/..

# Extra Java CLASSPATH elements.  Optional.
# export HADOOP_CLASSPATH="/usr/lib/hadoop/contrib/capacity-scheduler/hadoop-capacity-scheduler-0.20.2-cdh3u3.jar:$HADOOP_CLASSPATH"

# The maximum amount of heap to use, in MB. Default is 1000.
# export HADOOP_HEAPSIZE=2048

# Extra Java runtime options.  Empty by default.
if [ "$HADOOP_OPTS" == "" ]; then export HADOOP_OPTS="-server -Djava.security.krb5.conf=${KRB5_CONFIG}"; else HADOOP_OPTS+=" -server -Djava.security.krb5.conf=${KRB5_CONFIG}"; fi

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"
# export HADOOP_TASKTRACKER_OPTS=
# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
# export HADOOP_CLIENT_OPTS

# Extra ssh options.  Empty by default.
export HADOOP_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HADOOP_CONF_DIR -o StrictHostKeyChecking=no"

# Where log files are stored.  $HADOOP_HOME/logs by default.
# export HADOOP_LOG_DIR=/var/log/hadoop

# File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
# export HADOOP_SLAVES=${HADOOP_HOME}/conf/slaves

# host:path where hadoop code should be rsync'd from.  Unset by default.
# export HADOOP_MASTER=master:/home/$USER/src/hadoop

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HADOOP_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
# export HADOOP_PID_DIR=/var/hadoop/pids

# A string representing this instance of hadoop. $USER by default.
# export HADOOP_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export HADOOP_NICENESS=10
