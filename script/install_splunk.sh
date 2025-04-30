#!/bin/bash

# Splunk installation variables
SPLUNK_VERSION="9.1.2"
SPLUNK_BUILD="b6b9c8185839"
SPLUNK_PACKAGE="splunk-${SPLUNK_VERSION}-${SPLUNK_BUILD}-Linux-x86_64.tgz"
SPLUNK_URL="https://download.splunk.com/products/splunk/releases/${SPLUNK_VERSION}/${SPLUNK_PACKAGE}"
SPLUNK_HOME="/opt/splunk"
SPLUNK_USER="splunk"
SPLUNK_GROUP="splunk"

# Splunk admin credentials
SPLUNK_ADMIN_USER="hawk"
SPLUNK_ADMIN_PASS="pegandoAVisao"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# Create Splunk user and group
echo "Creating Splunk user and group..."
groupadd -r $SPLUNK_GROUP 2>/dev/null || true
useradd -r -m -g $SPLUNK_GROUP $SPLUNK_USER 2>/dev/null || true

# Download Splunk
echo "Downloading Splunk..."
wget -O /tmp/$SPLUNK_PACKAGE $SPLUNK_URL

# Install Splunk
echo "Installing Splunk..."
tar xzf /tmp/$SPLUNK_PACKAGE -C /opt
rm /tmp/$SPLUNK_PACKAGE

# Set permissions
echo "Setting permissions..."
chown -R $SPLUNK_USER:$SPLUNK_GROUP $SPLUNK_HOME

# Copy systemd service file
echo "Setting up systemd service..."
cp services/splunk.service /etc/systemd/system/
systemctl daemon-reload

# Create user-seed.conf to set up initial admin password
echo "Setting up initial admin credentials..."
cat > $SPLUNK_HOME/etc/system/local/user-seed.conf << EOF
[user_info]
USERNAME = admin
PASSWORD = $(openssl rand -base64 32)
EOF

# Configure web interface to listen on all interfaces
echo "Configuring web interface to listen on all interfaces..."
cat > $SPLUNK_HOME/etc/system/local/web.conf << EOF
[settings]
httpport = 8000
enableSplunkWebSSL = false
listenOnIPv6 = no
mgmtHostPort = 0.0.0.0:8089
EOF

# Initial Splunk setup
echo "Performing initial Splunk setup..."
sudo -u $SPLUNK_USER $SPLUNK_HOME/bin/splunk start --accept-license --answer-yes --no-prompt
sudo -u $SPLUNK_USER $SPLUNK_HOME/bin/splunk enable boot-start

# Create new admin user and disable default admin
echo "Creating new admin user and disabling default admin..."
sudo -u $SPLUNK_USER $SPLUNK_HOME/bin/splunk add user $SPLUNK_ADMIN_USER -password $SPLUNK_ADMIN_PASS -role admin -auth admin:$(grep PASSWORD $SPLUNK_HOME/etc/system/local/user-seed.conf | cut -d= -f2 | tr -d ' ')
sudo -u $SPLUNK_USER $SPLUNK_HOME/bin/splunk disable user admin -auth $SPLUNK_ADMIN_USER:$SPLUNK_ADMIN_PASS

# Start Splunk service
echo "Starting Splunk service..."
systemctl start splunk.service
systemctl enable splunk.service

echo "Splunk installation completed!"
echo "You can access the Splunk Web interface at $SPLUNK_HOME/bin/splunk web"
echo "Credentials:"
echo "Username: $SPLUNK_ADMIN_USER"
echo "Password: $SPLUNK_ADMIN_PASS"
