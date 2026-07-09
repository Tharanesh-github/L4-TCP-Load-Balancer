package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server represents a backend target
type Server struct {
	URL         *url.URL
	Alive       bool
	mux         sync.RWMutex
	ActiveConns int64 // NEW: Tracks current active TCP connections
}

// SetAlive safely updates the health status
func (s *Server) SetAlive(alive bool) {
	s.mux.Lock()
	s.Alive = alive
	s.mux.Unlock()
}

// IsAlive safely reads the health status
func (s *Server) IsAlive() (alive bool) {
	s.mux.RLock()
	alive = s.Alive
	s.mux.RUnlock()
	return
}

// GetActiveConns safely reads the current active connection count
func (s *Server) GetActiveConns() int64 {
	return atomic.LoadInt64(&s.ActiveConns)
}

// ServerPool holds all of our backend targets
type ServerPool struct {
	Servers []*Server
}

// NextPeer implements the LEAST CONNECTIONS routing algorithm
func (s *ServerPool) NextPeer() *Server {
	var bestServer *Server
	var minConns int64 = -1 // -1 acts as our initial "unassigned" state

	for _, server := range s.Servers {
		if server.IsAlive() {
			conns := server.GetActiveConns()
			// If we haven't picked a server yet, OR this server has fewer connections than our current best
			if bestServer == nil || conns < minConns {
				bestServer = server
				minConns = conns
			}
		}
	}
	return bestServer
}

// healthCheck pings the backend servers every 10 seconds
func healthCheck(serverPool *ServerPool) {
	t := time.NewTicker(10 * time.Second)
	for {
		<-t.C
		// FIXED: Changed 's.Servers' to 'serverPool.Servers'
		for _, server := range serverPool.Servers {
			conn, err := net.DialTimeout("tcp", server.URL.Host, 2*time.Second)
			if err != nil {
				server.SetAlive(false)
				log.Printf("HealthCheck: [ %s ] is DOWN\n", server.URL.Host)
				continue
			}
			conn.Close()
			if !server.IsAlive() {
				server.SetAlive(true)
				log.Printf("HealthCheck: [ %s ] is UP\n", server.URL.Host)
			}
		}
	}
}

// handleConnection proxies the raw TCP traffic
func handleConnection(clientConn net.Conn, serverPool *ServerPool) {
	defer clientConn.Close()

	// 1. Find the server with the LEAST connections
	targetServer := serverPool.NextPeer()
	if targetServer == nil {
		log.Println("❌ No alive servers available")
		return
	}

	// 2. Increment active connections atomically
	atomic.AddInt64(&targetServer.ActiveConns, 1)

	// 3. Ensure we decrement the counter when the client disconnects
	defer atomic.AddInt64(&targetServer.ActiveConns, -1)

	log.Printf("Routing %s -> %s (Active Connections on target: %d)\n", clientConn.RemoteAddr(), targetServer.URL.Host, targetServer.GetActiveConns())

	// 4. Dial the backend server
	serverConn, err := net.DialTimeout("tcp", targetServer.URL.Host, 5*time.Second)
	if err != nil {
		log.Printf("Error connecting to backend %s: %s\n", targetServer.URL.Host, err)
		targetServer.SetAlive(false)
		return
	}
	defer serverConn.Close()

	// 5. Proxy the bidirectional traffic using wait groups
	var wg sync.WaitGroup
	wg.Add(2)

	// Copy from Client -> Server
	go func() {
		defer wg.Done()
		io.Copy(serverConn, clientConn)
		if tcpConn, ok := serverConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
	}()

	// Copy from Server -> Client
	go func() {
		defer wg.Done()
		io.Copy(clientConn, serverConn)
		if tcpConn, ok := clientConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
	}()

	wg.Wait()
}

func main() {
	var port int
	var backends string

	flag.IntVar(&port, "port", 8080, "Port to serve load balancer on")
	flag.StringVar(&backends, "backends", "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083", "Comma separated list of backends")
	flag.Parse()

	backendList := strings.Split(backends, ",")
	serverPool := ServerPool{}

	for _, backend := range backendList {
		serverURL, err := url.Parse(fmt.Sprintf("http://%s", backend))
		if err != nil {
			log.Fatal(err)
		}
		serverPool.Servers = append(serverPool.Servers, &Server{
			URL:   serverURL,
			Alive: true,
		})
	}

	// Start health checker in the background
	go healthCheck(&serverPool)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Printf("🚀 Layer 4 Load Balancer started on port %d\n", port)
	log.Printf("⚖️  Routing Algorithm: LEAST CONNECTIONS\n")

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s\n", err)
			continue
		}
		// Spawn a lightweight goroutine for every single connection
		go handleConnection(clientConn, &serverPool)
	}
}
