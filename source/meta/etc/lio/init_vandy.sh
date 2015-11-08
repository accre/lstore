#!/bin/bash

# Helper script to make this L-Store installation line up with the Vandy
# configuration

DEF_IP=$(hostname -I | cut -d ' ' -f 1)                                            
IP=${1:-$DEF_IP} 

# First, make the /lio tree
mkdir -p /lio/{log,lfs,osfile}
chmod 777 /lio/{log,lfs,osfile}

# Then, replace the configuration with the lstore tarball
(
    cd /etc/lio
    # Temporary, can move somewhere else if needed
    curl http://www.accre.vanderbilt.edu/repos/lstore-vandy-cfg.tgz | tar xvz
)

# If there's not already an osfile tree set up, set one up
if [ ! -d /lio/osfile/hardlink ]; then
    (
        cd /lio/osfile
        /etc/lio/init_osfile.sh
    )
fi

# Finally, update the config with our address
(
    cd /etc/lio
    ./reconfig_lserver.sh "$IP"
)

