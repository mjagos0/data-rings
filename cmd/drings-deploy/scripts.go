package main

import "fmt"

const (
	remoteProjDir		= "~/data-rings"
	remoteDataDir		= "~/.datarings"
	remoteMountPoint	= "/mnt/datarings"
	remoteUploadDir		= "~/drings-upload"
	daemonPIDFile		= "~/drings-daemon.pid"
	daemonLogFile		= "~/drings-daemon.log"
	dhtPort			= "7000"
)

func setupScript(goVersion, sshUser string) string {
	return fmt.Sprintf(`set -e
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -q
sudo apt-get install -y -q build-essential libfuse-dev fuse rsync chrony

# Point chrony at the AWS Time Sync Service (169.254.169.123): a stratum-1
# GPS-disciplined NTP source served from each region's hypervisor at
# microsecond round-trip times. This brings cross-region clock skew from
# the ~10-50ms typical of public-pool NTP down to ~1ms, which is the
# resolution at which the experiment eventlog can meaningfully measure
# inter-node lag.
sudo tee /etc/chrony/chrony.conf >/dev/null <<'CHRONY'
# Managed by drings-deploy setup. AWS Time Sync first; pool fallback.
server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4
pool 2.debian.pool.ntp.org iburst
driftfile /var/lib/chrony/chrony.drift
makestep 1.0 3
rtcsync
leapsectz right/UTC
logdir /var/log/chrony
CHRONY
sudo systemctl enable chrony >/dev/null 2>&1 || true
sudo systemctl restart chrony

# Add 2GB swap to handle Go compilation on low-memory instances
if ! sudo swapon --show 2>/dev/null | grep -q swapfile; then
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo "Swap created"
fi

GO_VERSION=%s
INSTALLED=$(/usr/local/go/bin/go version 2>/dev/null | awk '{print $3}' || echo "none")
if [ "$INSTALLED" != "go${GO_VERSION}" ]; then
    echo "Installing Go ${GO_VERSION} (have: ${INSTALLED})..."
    wget -q -O /tmp/go.tar.gz "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go.tar.gz
    rm /tmp/go.tar.gz
    echo "Go ${GO_VERSION} installed."
else
    echo "Go ${GO_VERSION} already installed."
fi

grep -qxF 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' ~/.bashrc || \
    echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.bashrc

sudo mkdir -p /mnt/datarings
sudo chown %s:%s /mnt/datarings 2>/dev/null || sudo chown %s /mnt/datarings

# Allow FUSE for non-root users
if grep -q '^#user_allow_other' /etc/fuse.conf 2>/dev/null; then
    sudo sed -i 's/^#user_allow_other/user_allow_other/' /etc/fuse.conf
fi

echo "Setup complete."
`, goVersion, sshUser, sshUser, sshUser)
}

func daemonStartScript(inst *Instance, bootstrapIP, metricsAddr string, extraFlags []string) string {
	return daemonStartScriptExperiment(inst, bootstrapIP, metricsAddr, "", extraFlags)
}

func daemonStartScriptExperiment(inst *Instance, bootstrapIP, metricsAddr, experimentID string, extraFlags []string) string {
	bootstrapFlag := "--bootstrap none"
	if bootstrapIP != "" && bootstrapIP != inst.IPv4 {
		bootstrapFlag = fmt.Sprintf("--bootstrap /ip4/%s/tcp/%s", bootstrapIP, dhtPort)
	}
	metricsFlag := ""
	if metricsAddr != "" {
		metricsFlag = fmt.Sprintf("--metrics-addr %s", metricsAddr)
	}
	extra := ""
	for _, f := range extraFlags {
		extra += " " + f
	}
	if experimentID != "" {
		extra += fmt.Sprintf(" --experiment %s --experiment-node %s", experimentID, inst.Name)
	}
	return fmt.Sprintf(`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
# Kill any existing daemon and wait for it to fully release LevelDB locks
if [ -f %s ]; then
    OLD_PID=$(cat %s)
    kill $OLD_PID 2>/dev/null && echo "Sent SIGTERM to PID $OLD_PID" || true
    rm -f %s
fi
pkill drings-daemon 2>/dev/null || true
# Wait up to 10s for the daemon process to exit and release LevelDB locks
for i in $(seq 1 10); do
    pgrep drings-daemon > /dev/null 2>&1 || break
    sleep 1
done
pkill -9 drings-daemon 2>/dev/null || true
sleep 1

# Clear any stale FUSE mount left over from a previous unclean shutdown.
# The daemon runs with --mount=false so this is strictly cleanup.
sudo umount %s 2>/dev/null || fusermount -u %s 2>/dev/null || true

mkdir -p %s

# Start daemon (stdin redirected so SSH session can close cleanly)
# OOM guards: bound Go heap and tighten GC pacing on 512 MB Lightsail nodes.
nohup env GOMEMLIMIT=350MiB GOGC=50 drings-daemon \
    --no-config \
    --debug \
    --dht-addr /ip4/0.0.0.0/tcp/%s \
    --advertise %s \
    %s \
    %s \
    %s \
    --mount=false \
    < /dev/null > %s 2>&1 &
DAEMON_PID=$!
disown $DAEMON_PID
echo $DAEMON_PID > %s

# Wait for the HTTP API to bind (up to 180s). The daemon prints
# "Daemon started" only when /roots is reachable, so join-groups and
# other follow-ups can assume an accepting API.
API_READY=0
for i in $(seq 1 180); do
    if ! kill -0 $DAEMON_PID 2>/dev/null; then
        echo "ERROR: Daemon exited during startup (PID: $DAEMON_PID)"
        tail -40 %s
        exit 1
    fi
    if curl -sf --max-time 1 http://localhost:7423/roots > /dev/null 2>&1; then
        API_READY=1
        break
    fi
    sleep 1
done

if [ "$API_READY" = "1" ]; then
    echo "Daemon started (PID: $DAEMON_PID, API ready after ${i}s)"
else
    echo "ERROR: Daemon API never came up within 180s, check %s"
    tail -40 %s
    exit 1
fi
`, daemonPIDFile, daemonPIDFile, daemonPIDFile,
		remoteMountPoint, remoteMountPoint,
		remoteDataDir,
		dhtPort, inst.IPv4, bootstrapFlag, metricsFlag, extra,
		daemonLogFile, daemonPIDFile,
		daemonLogFile, daemonLogFile, daemonLogFile)
}

func daemonStopScript() string {
	return fmt.Sprintf(`
if [ -f %s ]; then
    PID=$(cat %s)
    if kill $PID 2>/dev/null; then
        echo "Stopped daemon (PID: $PID)"
    else
        echo "PID $PID not running"
    fi
    rm -f %s
else
    pkill drings-daemon 2>/dev/null && echo "Stopped daemon" || echo "No daemon running"
fi
`, daemonPIDFile, daemonPIDFile, daemonPIDFile)
}

func cleanScript(wipeIdentity bool) string {
	extra := ""
	msg := "Cleaned: blocks, roots, groups, experiments"
	if wipeIdentity {
		extra = fmt.Sprintf("\nrm -f %s/identity\n", remoteDataDir)
		msg = "Cleaned: blocks, roots, groups, experiments, identity"
	}

	return fmt.Sprintf(`
# Stop daemon (thorough: SIGTERM, wait up to 10s, then SIGKILL)
if [ -f %s ]; then
    PID=$(cat %s)
    kill $PID 2>/dev/null || true
    rm -f %s
fi
pkill drings-daemon 2>/dev/null || true
for i in $(seq 1 10); do
    pgrep drings-daemon > /dev/null 2>&1 || break
    sleep 1
done
pkill -9 drings-daemon 2>/dev/null || true
sleep 1

# Unmount FUSE
sudo umount %s 2>/dev/null || fusermount -u %s 2>/dev/null || true

# Remove data (keep identity and config)
rm -rf %s/blocks %s/local-blocks %s/network-blocks %s/roots %s/groups %s/groups.json %s/experiments
%secho "%s"
`, daemonPIDFile, daemonPIDFile, daemonPIDFile,
		remoteMountPoint, remoteMountPoint,
		remoteDataDir, remoteDataDir, remoteDataDir, remoteDataDir, remoteDataDir, remoteDataDir, remoteDataDir,
		extra, msg)
}

func joinGroupScript(groupName, privateKey string) string {
	return fmt.Sprintf(`export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
# Safety probe: mega-deploy already waits for the API in daemonStartScript,
# but when join-groups is invoked standalone we still want to fail fast
# with a clear message rather than a generic "connection refused".
API_READY=0
for i in $(seq 1 180); do
    if curl -sf --max-time 1 http://localhost:7423/roots > /dev/null 2>&1; then
        API_READY=1
        break
    fi
    sleep 1
done
if [ "$API_READY" != "1" ]; then
    echo "ERROR: daemon API not reachable on localhost:7423 after 180s" >&2
    exit 1
fi

drings key add %s %s 2>/dev/null || true
drings ring join %s
`, groupName, privateKey, groupName)
}
