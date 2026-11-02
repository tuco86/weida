// Command weida-smoke makes one Req/Rep round trip over QUIC on localhost
// with a pinned key and prints "go smoke: ok libweida <version>". The CI
// builds it against the static libweida and runs it with no libweida
// beside it.
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"git.doodleshnookie.net/tuco86/weida/crates/go/weida"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "go smoke:", err)
		os.Exit(1)
	}
	fmt.Println("go smoke: ok libweida", weida.Version())
}

func run() error {
	// A hang guard only.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	identity, err := weida.GenerateIdentity()
	if err != nil {
		return err
	}
	fingerprint, err := identity.Fingerprint()
	if err != nil {
		return err
	}
	rt, err := weida.NewRuntime(nil)
	if err != nil {
		return err
	}
	listener, err := rt.Listener()
	if err != nil {
		return err
	}
	defer listener.Close()
	serverTLS, err := weida.NewServerTLS(identity)
	if err != nil {
		return err
	}
	binding, err := listener.BindQUIC(ctx, "127.0.0.1:0", serverTLS)
	if err != nil {
		return err
	}
	defer binding.Close()
	replier, err := listener.Replier("/echo")
	if err != nil {
		return err
	}
	defer replier.Close()

	trust, err := weida.NewTrust()
	if err != nil {
		return err
	}
	if err := trust.AddPin(fingerprint); err != nil {
		return err
	}
	clientTLS, err := weida.NewClientTLS(trust)
	if err != nil {
		return err
	}
	requester, err := rt.Requester(clientTLS)
	if err != nil {
		return err
	}
	defer requester.Close()
	if err := requester.Connect(ctx, "weida://"+fingerprint+"@"+binding.LocalAddr()+"/echo"); err != nil {
		return err
	}

	served := make(chan error, 1)
	go func() { served <- echo(ctx, replier) }()
	reply, err := requester.Request(ctx, []byte("ping"))
	if err != nil {
		return err
	}
	msg, err := reply.Collect(ctx, 1024)
	if err != nil {
		return err
	}
	if err := <-served; err != nil {
		return fmt.Errorf("serving: %w", err)
	}
	if !bytes.Equal(msg.Payload, []byte("PING")) {
		return fmt.Errorf("reply %q, want \"PING\"", msg.Payload)
	}
	return rt.Shutdown(ctx)
}

// echo answers one request with its body in upper case.
func echo(ctx context.Context, replier *weida.Replier) error {
	request, err := replier.Accept(ctx)
	if err != nil {
		return err
	}
	defer request.Close()
	body, err := request.TakeBody()
	if err != nil {
		return err
	}
	msg, err := body.Collect(ctx, 1024)
	if err != nil {
		return err
	}
	reply, err := request.Reply(ctx, nil)
	if err != nil {
		return err
	}
	defer reply.Close()
	if _, err := reply.WriteContext(ctx, bytes.ToUpper(msg.Payload)); err != nil {
		return err
	}
	delivery, err := reply.Finish()
	if err != nil {
		return err
	}
	return delivery.Delivered(ctx)
}
