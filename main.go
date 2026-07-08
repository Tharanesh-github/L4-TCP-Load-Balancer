package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ==============================================================================
// ==============================================================================

// Backend represents a single downstream server instance.
type Backend struct {
	Address string
	Alive   bool
	mux     sync.RWMutex // Protects the 'Alive' state from concurrent read/writes
}

// SetAlive safely updates the backend's status using a Write Lock.
func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

// IsAlive safely reads the backend's status using a Read Lock.
func (b *Backend) IsAlive() bool {
	b.mux.RLock()
	alive := b.Alive
	b.mux.RUnlock()
	return alive
}

// ServerPool manages the list of backends and the routing state.
type ServerPool struct {
	backends []*Backend
	current  uint64 // Atomic counter for thread-safe Round Robin routing
}

// ==============================================================================
// ==============================================================================

// NextIndex atomically increments the counter and returns a safe index.
// Using atomic operations avoids lock contention during high throughput.
func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

// GetNextPeer returns the next available backend server.
func (s *ServerPool) GetNextPeer() *Backend {
	// Loop through backends to find an alive one
	next := s.NextIndex()
	l := len(s.backends) + next // start from next and do a full cycle
	for i := next; i < l; i++ {
		idx := i % len(s.backends)
		if s.backends[idx].IsAlive() {
			if i != next {
				// Atomically update the current index if we had to skip dead servers
				atomic.StoreUint64(&s.current, uint64(idx))
			}
			return s.backends[idx]
		}
	}
	return nil // Total failure: No backends are alive
}

// ==============================================================================
// ==============================================================================

// HealthCheck pings the backends every few seconds to update their status.
func (s *ServerPool) HealthCheck() {
	for {
		for _, b := range s.backends {
			status := "UP"
			// TCP Ping with a 2-second timeout
			conn, err := net.DialTimeout("tcp", b.Address, 2*time.Second)
			if err != nil {
				b.SetAlive(false)
				status = "DOWN"
			} else {
				b.SetAlive(true)
				conn.Close()
			}
			log.Printf("HealthCheck: [ %s ] is %s", b.Address, status)
		}
		time.Sleep(10 * time.Second)
	}
}

// ==============================================================================
// ==============================================================================

// proxy handles bidirectional data transfer between the client and the chosen backend.
func proxy(clientConn net.Conn, backend *Backend) {
	// Attempt to connect to the backend server
	backendConn, err := net.DialTimeout("tcp", backend.Address, 5*time.Second)
	if err != nil {
		log.Printf("Failed to connect to backend %s: %s\n", backend.Address, err)
		clientConn.Close()
		backend.SetAlive(false) // Mark dead immediately on connection refused
		return
	}

	// Ensure both connections are closed when the function exits
	defer clientConn.Close()
	defer backendConn.Close()

	// Bidirectional copy using Goroutines
	// io.Copy handles the raw byte streaming at the transport layer
	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Backend
	go func() {
		defer wg.Done()
		io.Copy(backendConn, clientConn)
		backendConn.(*net.TCPConn).CloseWrite() // Signal EOF
	}()

	// Backend -> Client
	go func() {
		defer wg.Done()
		io.Copy(clientConn, backendConn)
		clientConn.(*net.TCPConn).CloseWrite() // Signal EOF
	}()

	wg.Wait()
}

// handleConnection assigns an incoming connection to a backend and proxies it.
func handleConnection(clientConn net.Conn, serverPool *ServerPool) {
	backend := serverPool.GetNextPeer()
	if backend == nil {
		log.Println("503 Service Unavailable: No backends alive")
		clientConn.Close()
		return
	}

	log.Printf("Routing %s -> %s\n", clientConn.RemoteAddr(), backend.Address)
	proxy(clientConn, backend)
}

// ==============================================================================
// ==============================================================================

func main() {
	// Define our cluster of backend servers
	servers := []string{
		"127.0.0.1:8081",
		"127.0.0.1:8082",
		"127.0.0.1:8083",
	}

	serverPool := ServerPool{
		backends: make([]*Backend, 0),
	}

	for _, addr := range servers {
		serverPool.backends = append(serverPool.backends, &Backend{
			Address: addr,
			Alive:   true,
		})
	}

	// Launch the health checker as a background Goroutine
	go serverPool.HealthCheck()

	// Bind the Load Balancer to a port
	port := 8080
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d: %s\n", port, err)
	}
	defer listener.Close()

	log.Printf("🚀 Layer 4 Load Balancer started on port %d\n", port)

	// The Accept Loop
	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s\n", err)
			continue
		}
		// Spawn a new Goroutine for EVERY incoming connection (Highly concurrent!)
		go handleConnection(clientConn, &serverPool)
	}
}
