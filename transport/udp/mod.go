package udp

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/internal/traffic"
	"go.dedis.ch/cs438/transport"
	"net"
	"sync"
	"time"
)

// @Author: Tiannan Sha
// @Email: tiannan.sha@epfl.ch

//const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{
		//incomings: make(map[string]*net.UDPConn),
		traffic:   traffic.NewTraffic(),
	}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	sync.RWMutex
	//incomings map[string]*net.UDPConn
	traffic   *traffic.Traffic  // not sure what this is for
}

// var counter uint32

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	//panic("to be implemented in HW0")
	//n.Lock() // probably unnecessary
	udpAddr,err := net.ResolveUDPAddr("udp",address)
	if (err!=nil) {
		return nil, errors.New("address invalid")
	}
	// for net.ListenUdp, if port=0, a port number will be auto chosen
	conn, _ := net.ListenUDP("udp", udpAddr)
	//n.incomings[address] = conn
	//n.Unlock()
	log.Info().Msgf("my address is %v",conn.LocalAddr().String())

	return &Socket{
		UDP: n,
		myAddr: conn.LocalAddr().String(),
		udpConn: conn,
		ins:  packets{},
		outs: packets{},
	}, nil
}

type packets struct {
	sync.Mutex
	data []transport.Packet
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()

	p.data = append(p.data, pkt.Copy())
}

func (p *packets) getAll() []transport.Packet {
	p.Lock()
	defer p.Unlock()

	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}


// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	*UDP
	myAddr string
	udpConn *net.UDPConn
	ins  packets
	outs packets
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	//panic("to be implemented in HW0")
	err := s.udpConn.Close()
	if err != nil {
		return err
	}
	return nil
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	//panic("to be implemented in HW0")
	destUDPAddr,err := net.ResolveUDPAddr("udp",dest)
	if err != nil {
		log.Info().Msgf("in Send(), Couldn't send response %v", err)
	}

	pktBytes,err:=pkt.Marshal()
	if err != nil {
		log.Info().Msgf("in Send(), Couldn't marshal. error: %v", err)
	}
	// set time out for writeToUDP() if needed
	if (timeout>0) {
		err = s.udpConn.SetWriteDeadline(time.Now().Add(timeout))
		if (err!=nil) {
			log.Info().Msgf("in Recv(), err:%s", err)
		}
	}

	_,err = s.udpConn.WriteToUDP(pktBytes, destUDPAddr)
	if err != nil {
		log.Info().Msgf("in Send(), Couldn't send response %v", err)
		return transport.TimeoutError(0)
	}

	log.Info().Msg("in Send(),successfully sent ")
	s.outs.add(pkt)
	s.traffic.LogSent(pkt.Header.RelayedBy, dest, pkt)
	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	//panic("to be implemented in HW0")
	log.Info().Msgf("in Recv() on socket %s", s.myAddr)

	// set timeout for readFromUDP if needed
	if (timeout>0) {
		err:= s.udpConn.SetReadDeadline(time.Now().Add(timeout))
		if (err!=nil) {
			log.Info().Msgf("in Recv(), err:%s", err)
		}
	}

	pktBytes := make([]byte, 80000)
	numBytes,_,err := s.udpConn.ReadFromUDP(pktBytes)
	log.Info().Msgf("in Recv(), %v bytes were read \n", numBytes)
	if (err!=nil) {
		log.Info().Msgf("err:%s", err)
		return transport.Packet{}, transport.TimeoutError(0)
	}

	log.Info().Msg("In recv(), successfully ReadFromUDP")
	var pkt transport.Packet
	err = pkt.Unmarshal(pktBytes[0:numBytes])
	log.Info().Msg("in Recv(), after Unmarshal")
	log.Info().Msgf("in Recv, pkt:%s", pkt)
	if (err!=nil) {
		log.Error().Msgf("line 152 in Recv(), error when unmarshaling, err: %s \n", err)
	}

	log.Info().Msg("!!!!!!!!!!!!!in Recv(), received a packet")
	s.traffic.LogRecv(pkt.Header.RelayedBy, s.myAddr, pkt)
	s.ins.add(pkt)
	return pkt, nil

}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.myAddr
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	//panic("to be implemented in HW0")
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	//panic("to be implemented in HW0")
	return s.outs.getAll()
}
