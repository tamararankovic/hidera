package peers

import (
	"log"
	"net"

	"github.com/tamararankovic/hidera/config"
)

type Peers struct {
	Peers []Peer
	Conn  *net.UDPConn
}

func NewPeers(config config.Config) (*Peers, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(config.ListenIP), Port: config.ListenPort})
	if err != nil {
		return nil, err
	}
	ps := &Peers{
		Conn: conn,
	}
	for i := range config.PeersIDs {
		ps.Peers = append(ps.Peers, Peer{
			ID:   config.PeersIDs[i],
			Addr: &net.UDPAddr{IP: net.ParseIP(config.PeersIPs[i]), Port: config.ListenPort},
			Conn: conn,
		})
	}
	go ps.listen()
	return ps, nil
}

func (ps *Peers) listen() {
	buf := make([]byte, 1472)
	for {
		n, sender, err := ps.Conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("Read error:", err)
			continue
		}
		peer := ps.findPeer(sender)
		if peer == nil {
			log.Println("no peer found for address", sender)
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		// todo: notify of the new message
	}
}

func (ps *Peers) findPeer(addr *net.UDPAddr) *Peer {
	for _, p := range ps.Peers {
		if p.Addr.IP.Equal(addr.IP) {
			return &p
		}
	}
	return nil
}
