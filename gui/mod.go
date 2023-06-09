// Package main implements a simple CLI that can start the http proxy.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	urfave "github.com/urfave/cli/v2"
	"go.dedis.ch/cs438/gui/httpnode"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/peer/impl"
	"go.dedis.ch/cs438/registry/standard"

	"go.dedis.ch/cs438/transport/udp"
	"golang.org/x/xerrors"
)

const peerAddrMsg = "peer addr: '%s'"

var peerFactory = impl.NewPeer

var (
	// defaultLevel can be changed to set the desired level of the logger
	defaultLevel = zerolog.InfoLevel

	// logout is the logger configuration
	logout = zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	log zerolog.Logger
)

func init() {
	if os.Getenv("HTTPLOG") == "warn" {
		defaultLevel = zerolog.WarnLevel
	}

	if os.Getenv("HTTPLOG") == "no" {
		defaultLevel = zerolog.Disabled
	}

	log = zerolog.New(logout).
		Level(defaultLevel).
		With().Timestamp().Logger().
		With().Caller().Logger().
		With().Str("role", "cli node").Logger()

}

func main() {
	app := &urfave.App{
		Name:  "Node controller",
		Usage: "Please use the start command",

		Commands: []*urfave.Command{
			{
				Name:  "start",
				Usage: "starts the node and proxy",
				Flags: []urfave.Flag{
					&urfave.StringFlag{
						Name:  "proxyaddr",
						Usage: "addr of the proxy",
						Value: "127.0.0.1:0",
					},
					&urfave.StringFlag{
						Name:  "nodeaddr",
						Usage: "addr of the node",
						Value: "127.0.0.1:0",
					},
					&urfave.DurationFlag{
						Name:  "antientropy",
						Usage: "Antientropy interval",
						// 0 means the antientropy is not activated
						Value: 0,
					},
					&urfave.DurationFlag{
						Name:  "heartbeat",
						Usage: "Heartbeat interval",
						// 0 means the heartbeat is not activated
						Value: 0,
					},
					&urfave.DurationFlag{
						Name:  "acktimeout",
						Usage: "Timeout of ack message",
						// this is considered as a reasonable timeout value for
						// a small system.
						Value: time.Second * 3,
					},
					&urfave.Float64Flag{
						Name:  "continuemongering",
						Usage: "probability to continue mongering",
						// by default there is a 50% chance to continue
						// mongering.
						Value: 0.5,
					},
				},
				Action: start,
			},
		},

		Action: func(c *urfave.Context) error {
			urfave.ShowAppHelpAndExit(c, 1)
			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
}

// start starts the http proxy. It will create a UDP socket.
func start(c *urfave.Context) error {
	proxyAddr := c.String("proxyaddr")
	nodeAddr := c.String("nodeaddr")

	trans := udp.NewUDP()

	sock, err := trans.CreateSocket(nodeAddr)
	if err != nil {
		return xerrors.Errorf("failed to create socket")
	}

	// this message is used by the binary node to get the peer address
	log.Info().Msgf(peerAddrMsg, sock.GetAddress())

	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("socketaddress_%d", os.Getpid()))

	err = os.WriteFile(socketPath, []byte(sock.GetAddress()), os.ModePerm)
	if err != nil {
		return xerrors.Errorf("failed to write socket address file: %v", err)
	}

	conf := peer.Configuration{
		Socket:          sock,
		MessageRegistry: standard.NewRegistry(),

		AntiEntropyInterval: c.Duration("antientropy"),
		HeartbeatInterval:   c.Duration("heartbeat"),
		AckTimeout:          c.Duration("acktimeout"),
		ContinueMongering:   c.Float64("continuemongering"),
	}

	node := peerFactory(conf)

	httpnode := httpnode.NewHTTPNode(node, conf)

	notify := make(chan os.Signal, 1)
	signal.Notify(notify,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	err = httpnode.StartAndListen(proxyAddr)
	if err != nil {
		return xerrors.Errorf("failed to start and listen: %v", err)
	}

	<-notify
	log.Info().Msg("closing...")

	os.RemoveAll(socketPath)

	err = httpnode.StopAndClose()
	if err != nil {
		return xerrors.Errorf("failed to close: %v", err)
	}

	sock.Close()

	return nil
}
