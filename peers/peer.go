package peers

import (
	"log"
	"net"
)

type Peer struct {
	ID   string
	Addr *net.UDPAddr
	Conn *net.UDPConn
}

func (p *Peer) Send() {
	go func() {
		_, err := p.Conn.WriteToUDP([]byte{}, p.Addr)
		if err != nil {
			log.Println(err)
		}
	}()
}
