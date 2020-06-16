#!/bin/bash
#
# Nybble Analytics installation scripts.

# Create nybble service group and account.
if ! getent group "nybble" >/dev/null; then
  echo "Creating nybble service group..."
  echo ""
  groupadd -r "nybble"
fi

if ! getent passwd "nybble" >/dev/null; then
  echo "Creating nybble service account..."
  echo ""
  useradd -r -g "nybble" -M -s /sbin/nologin -c "nybble service user" "nybble"
fi

# Set NYBBLE_HOME env variable permanently by creating a profile file.
SOURCE="$(readlink -f "${BASH_SOURCE[0]}")"
NYBBLE_HOME=$(dirname "$SOURCE")

if [ ! -f /etc/profile.d/nybble.sh ]; then
  echo "Creating nybble.sh profile file with environment variable..."
  echo ""
  touch /etc/profile.d/nybble.sh
  echo "export NYBBLE_HOME=$NYBBLE_HOME" >> /etc/profile.d/nybble.sh
  echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile.d/nybble.sh
  echo "NYBBLE_HOME environment variable has been set to $NYBBLE_HOME"
  echo ""
fi


# Make nybble owner of Nybble App folder.
chown -R nybble:nybble "$NYBBLE_HOME"

# Create Systemd script for nybble service.
echo "Creating nybble systemd script..."
echo ""
cat > /etc/systemd/system/nybble.service << HERE
[Unit]
Description=Nybble Analytics server
Documentation=https://docs.nybble-analytics.io/
After=network.target remote-fs.target

[Service]
Type=forking
User=nybble
Group=nybble
# Set Environment variables
Environment=NYBBLE_HOME=$NYBBLE_HOME
Environment=JAVA_HOME=$JAVA_HOME
WorkingDirectory=$NYBBLE_HOME
# Start Flink Cluster and then submit Nybble Analytics Job in detached mode
ExecStartPre=$NYBBLE_HOME/flink/bin/start-cluster.sh
ExecStart=$NYBBLE_HOME/flink/bin/flink run -d NybbleAnalytics
ExecStop=$NYBBLE_HOME/flink/bin/stop-cluster.sh

[Install]
WantedBy=multi-user.target
HERE

chmod 644 /etc/systemd/system/nybble.service
systemctl daemon-reload

# Modify environment varial in Flink configuration for Job and Task manager.
echo "Setting envrionement variable in Flink configuration..."
echo ""
sed -i 's|^\(containerized.master.env.NYBBLE_HOME:\s*\).*|\1'$NYBBLE_HOME'|' "$NYBBLE_HOME"/flink/conf/flink-conf.yaml
sed -i 's|^\(containerized.taskmanager.env.NYBBLE_HOME:\s*\).*|\1'$NYBBLE_HOME'|' "$NYBBLE_HOME"/flink/conf/flink-conf.yaml

# Install Redis for MISP and DNS cache.
if rpm -q redis; then
  echo "Redis is already installed."
  echo ""
else
  echo "Installing redis for MISP and DNS cache"
  echo ""
  sudo yum -y install redis
fi

echo "Starting and enabling redis"
echo ""
sudo systemctl start redis
sudo systemctl enable redis

echo "Testing if redis is running..."
echo "PING..."
redis-cli ping
echo ""
echo ""

echo "Nybble server is now ready."
echo "/!\ use 'source /etc/profile' to load new profile file with Nybble environment variables."
echo "Use 'systemctl start nybble' to launch Nybble Analytics"