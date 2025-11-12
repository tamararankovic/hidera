package config

import (
	"log"
	"net"
	"os"
	"strconv"

	"github.com/tamararankovic/hidera/util"
)

type Config struct {
	ID         string
	ListenIP   string
	ListenPort int
	PeersIDs   []string
	PeersIPs   []string
}

func LoadFromEnv() Config {
	id := os.Getenv("ID")
	listenIP := os.Getenv("LISTEN_IP")
	listenHost := os.Getenv("LISTEN_HOST")
	listenPortStr := os.Getenv("LISTEN_PORT")

	if listenIP == "" && listenHost == "" {
		log.Fatalln("error: must set either LISTEN_IP or LISTEN_HOST")
	}

	if listenIP == "" {
		addrs, err := net.LookupHost(listenHost)
		if err != nil || len(addrs) == 0 {
			log.Fatalf("error resolving host %q: %v\n", listenHost, err)
		}
		listenIP = addrs[0]
	}

	if listenPortStr == "" {
		log.Fatalln("error: LISTEN_PORT is not set")
	}

	listenPort, err := strconv.Atoi(listenPortStr)
	if err != nil {
		log.Fatalf("invalid port %q: %v\n", listenPortStr, err)
	}

	c := Config{
		ID:         id,
		ListenIP:   listenIP,
		ListenPort: listenPort,
	}

	peerIDsStr := os.Getenv("PEER_IDS")
	peerIPsStr := os.Getenv("PEER_IPS")
	peerHostsStr := os.Getenv("PEER_HOSTS")

	if peerIDsStr == "" {
		log.Fatalln("error: PEER_IDS is not set")
	}

	peerIDs := util.SplitAndTrim(peerIDsStr)
	peerIPs := util.SplitAndTrim(peerIPsStr)
	peerHosts := util.SplitAndTrim(peerHostsStr)

	maxLen := len(peerIDs)
	util.EnsureSameLength(&peerIPs, maxLen)
	util.EnsureSameLength(&peerHosts, maxLen)

	for i := range maxLen {
		id := peerIDs[i]
		ip := peerIPs[i]
		host := peerHosts[i]

		if ip == "" && host == "" {
			log.Fatalf("error: peer %q has neither IP nor host\n", id)
		}
		if ip == "" {
			addrs, err := net.LookupHost(host)
			if err != nil || len(addrs) == 0 {
				log.Fatalf("error resolving host %q for peer %q: %v\n", host, id, err)
			}
			ip = addrs[0]
		}
		c.PeersIDs = append(c.PeersIDs, id)
		c.PeersIPs = append(c.PeersIPs, ip)
	}
	if !c.isValid() {
		log.Fatalln("config invalid", c)
	}
	return c
}

func (c Config) isValid() bool {
	return len(c.PeersIDs) == len(c.PeersIPs)
}
