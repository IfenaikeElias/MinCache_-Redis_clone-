package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const CRLF = "\r\n"

type Server struct {
	Listener   net.Listener
	Commands   map[string]CommandHandler
	port       string
	isReplica  bool
	masterHost string
	masterPort string
}

var DB sync.Map

// ------- Connection helpers ----------

func (s *Server) ListenForConn() {
	port := "0.0.0.0:" + s.port
	ln, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("Error starting server:", err.Error())
		os.Exit(1)
	}
	s.Listener = ln
}

func (s *Server) AcceptConn() (net.Conn, error) {
	conn, err := s.Listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err.Error())
		os.Exit(1)
	}
	return conn, nil
}

func (s *Server) CloseConn() {
	// function to close connection
	err := s.Listener.Close()
	if err != nil {
		fmt.Println("Failed to Close connection", err.Error())
	}
}

// ------- Utils for Command response --------

func writeBulkStr(conn net.Conn, s string) {
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	conn.Write([]byte(resp))
}

func writeStr(conn net.Conn, s string) {
	conn.Write([]byte("+" + s + CRLF))
}

func writeErr(conn net.Conn, msg string) {
	conn.Write([]byte("-ERR " + msg + CRLF))
}

func writeNullBulk(conn net.Conn) {
	conn.Write([]byte("$-1\r\n"))
}

// ------ Commands ----------

type EchoHandler struct{}
type PingHandler struct{}
type SetHandler struct{}
type GetHandler struct{}
type RpushHandler struct{}
type LrangeHandler struct{}
type LpushHandler struct{}
type IncrHandler struct{}
type InfoHandler struct{}

type CommandHandler interface {
	Execute(conn net.Conn, args ...string)
}

var commands = map[string]CommandHandler{
	"PING":   PingHandler{},
	"ECHO":   EchoHandler{},
	"SET":    SetHandler{},
	"GET":    GetHandler{},
	"RPUSH":  RpushHandler{},
	"LPUSH":  LpushHandler{},
	"LRANGE": LrangeHandler{},
	"INCR":   IncrHandler{},
	"INFO":   InfoHandler{}, // Placeholder for INFO command
}

func (i InfoHandler) Execute(conn net.Conn, args ...string) {
	if len(args) > 1 || args[0] != "replication" {
		writeErr(conn, "ERR invalid input for info")
		return
	}
	var infor strings.Builder
	infor.Grow(256)
	info := &infor
	if *replicaFlag != "" {
		info.WriteString("# Replication\r\n")
		info.WriteString("role:slave \r\n")
		info.WriteString("master_repl_offset:0\r\n")
		info.WriteString("master_replid: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n")
	}
	
	info.WriteString("role:master")
	writeBulkStr(conn, string(string(info.String())))
}

func (e EchoHandler) Execute(conn net.Conn, args ...string) {
	if len(args) == 0 {
		writeErr(conn, "echo needs an argument but none was provided")
		return
	}
	writeBulkStr(conn, args[0])
}

func (p PingHandler) Execute(conn net.Conn, args ...string) {
	if len(args) == 0 {
		writeStr(conn, "PONG")
	} else {
		writeErr(conn, "unexpected argument for PING")
	}
}

func (h IncrHandler) Execute(conn net.Conn, args ...string) {
	if len(args) != 1 {
		writeErr(conn, "INCR requires only one arguement")
		return
	}

	key := args[0]
	buck, exist := DB.Load(key)
	var b *Bucket
	if !exist {
		b = &Bucket{Val: "0"}
		DB.Store(key, b)
	} else {
		b = buck.(*Bucket)
		if b.HasExpiry && time.Now().After(b.ExpiryTime) {
			b.Val = "0"
			b.HasExpiry = false
			DB.Store(key, b)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	curr, err := strconv.Atoi(b.Val)
	if err != nil {
		writeErr(conn, "ERR value is not an integer")
		return
	}
	curr++
	b.Val = strconv.Itoa(curr)
	writeStr(conn, b.Val)
}

type Bucket struct {
	mu         sync.Mutex
	Val        string
	PX         int       // TTL in ms
	ExpiryTime time.Time // expiry
	HasExpiry  bool      // whether to expire
}

func (s SetHandler) Execute(conn net.Conn, args ...string) {
	if len(args) < 2 || len(args) > 4 {
		writeErr(conn, "ERR wrong number of arguments for 'set' command")
		return
	}
	key, value := args[0], args[1]

	var (
		d         time.Duration
		hasExpiry bool
	)
	if len(args) == 4 {
		if strings.ToUpper(args[2]) != "PX" {
			writeErr(conn, "ERR syntax error")
			return
		}
		ms, err := strconv.Atoi(args[3])
		if err != nil || ms <= 0 {
			writeErr(conn, "ERR PX value must be a positive integer")
			return
		}
		d = time.Duration(ms) * time.Millisecond
		hasExpiry = true
	}

	now := time.Now()
	var bucket *Bucket
	if hasExpiry {
		bucket = &Bucket{
			Val:        value,
			PX:         int(d.Milliseconds()),
			ExpiryTime: now.Add(d),
			HasExpiry:  true,
		}
	} else {
		bucket = &Bucket{Val: value}
	}

	DB.Store(key, bucket)
	writeStr(conn, "OK")
}

func (g GetHandler) Execute(conn net.Conn, args ...string) {
	if len(args) != 1 {
		writeErr(conn, "GET requires only one arguement")
		return
	}
	key := args[0]
	value, ok := DB.Load(key)

	if !ok {
		writeNullBulk(conn)
		return
	}
	bucket, Ok := value.(*Bucket)
	if !Ok {
		writeErr(conn, "stored value is invalid")
		return
	}
	if bucket.HasExpiry && time.Now().After(bucket.ExpiryTime) {
		DB.Delete(key)
		writeNullBulk(conn)
		return
	}
	writeBulkStr(conn, bucket.Val)
}

func (r RpushHandler) Execute(conn net.Conn, args ...string) {
	if len(args) < 2 {
		writeErr(conn, "RPUSH requires at least 2 arguements")
		return
	}
	key := args[0]
	items := args[1:]
	value, _ := DB.LoadOrStore(key, []string{})
	list, ok := value.([]string)
	if !ok {
		writeErr(conn, "Invalid data type for the key")
		return
	}
	list = append(list, items...)
	DB.Store(key, list)
	writeBulkStr(conn, fmt.Sprintf("(%d)", len(list)))
}

// ------ Utility function for Lpush to turn around list ------
func reverse(s []string) []string {
	i, j := 0, len(s)-1
	for i < j {
		s[i], s[j] = s[j], s[i]
		i++
		j--
	}
	return s
}
func (l LpushHandler) Execute(conn net.Conn, args ...string) {
	if len(args) < 2 {
		writeErr(conn, "LPUSH requires at least 2 arguements")
		return
	}
	key := args[0]
	items := args[1:]
	value, _ := DB.LoadOrStore(key, []string{})
	list, ok := value.([]string)
	if !ok {
		writeErr(conn, "Invalid data type for the key")
		return
	}
	temp_list := append([]string{}, items...)
	list = append(reverse(temp_list), list...)
	DB.Store(key, list)
	writeBulkStr(conn, fmt.Sprintf("(%d)", len(list)))
}

func (l LrangeHandler) Execute(conn net.Conn, args ...string) {
	if len(args) < 3 {
		writeErr(conn, "LRANGE requires a start and end index")
		return
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		writeErr(conn, "Invalid start index")
		return
	}

	end, err := strconv.Atoi(args[2])
	if err != nil {
		writeErr(conn, "Invalid end index")
		return
	}
	// Load the list from the database
	value, ok := DB.Load(key)
	if !ok {
		writeNullBulk(conn)
		return
	}
	list, ok := value.([]string)
	if !ok {
		writeErr(conn, "Invalid data type for the key")
		return
	}

	// Handle negative indexes
	if start < 0 {
		start = len(list) + start
	}
	if end < 0 {
		end = len(list) + end
	}

	// Check if the range is valid
	if start >= len(list) || end < 0 || start > end {
		writeNullBulk(conn)
		return
	}
	if end >= len(list) {
		end = len(list) - 1
	}
	result := list[start : end+1]
	if len(result) == 0 {
		writeNullBulk(conn)
		return
	}
	// write the length of the result first (for RESP array format)
	writeBulkStr(conn, fmt.Sprintf("%d", len(result)))
	// Write each item in the result
	for _, item := range result {
		writeBulkStr(conn, item)
	}
}

// ------ Command Parser --------

func (s *Server) parseCommand(line string) (CommandHandler, []string, error) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, nil, fmt.Errorf("empty input")
	}
	cmd := strings.ToUpper(parts[0])
	handler, ok := s.Commands[cmd]
	if !ok {
		return nil, nil, fmt.Errorf("unknown command %s", cmd)
	}
	return handler, parts[1:], nil
}

func startReaper(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for now := range ticker.C {
			DB.Range(func(k, v interface{}) bool {
				b := v.(*Bucket)
				if b.HasExpiry && now.After(b.ExpiryTime) {
					DB.Delete(k)
				}
				return true
			})
		}
	}()
}

var portFlag = flag.String("port", "6379", "Port for server to listen on")
var replicaFlag = flag.String("replicaof", "localhost 6379", "flag to assume slave role")

func (s *Server) handleMasterConnection(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println("Received from master:", line)
		// Here you would implement logic to handle data from master
		// For simplicity, we just print the received line
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading from master:", err)
	}
}

func (s *Server) connectToMaster() {
	// Implement connection logic to master here
	addr := fmt.Sprintf("%v:%v", s.masterHost, s.masterPort)
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Error connecting to master:", err)
			time.Sleep(3 * time.Second) // Retry after a delay
			continue
		}
		fmt.Println("Connected to master at", addr)
		defer conn.Close()

		conn.Write([]byte("PING\r\n"))
		// Handle communication with master		
		s.handleMasterConnection(conn)

		break // Exit the loop if connection is successful
	}
}
	
// ----------- main ------------

func main() {
	flag.Parse()

	startReaper(1 * time.Second)
	s := &Server{
		Commands: commands,
		port:     *portFlag,
		isReplica: replicaFlag != nil && *replicaFlag != "",
	}

	if s.isReplica {
		parts := strings.Split(*replicaFlag, " ")
		if len(parts) != 2 {
			fmt.Println("Invalid replicaof format. Use 'host port'")
			os.Exit(1) // if firmat or host port is invalidjust close the connection and continue
		}
		s.masterHost = parts[0]
		s.masterPort = parts[1]
		fmt.Printf("Configured as replica of %s:%s\n", s.masterHost, s.masterPort)
		go s.connectToMaster()
	}

	s.ListenForConn()
	defer s.CloseConn()
	fmt.Println("Listening on 0.0.0.0:" + s.port)

	if s.isReplica {
		masterConn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", s.masterHost, s.masterPort))
		fmt.Printf("Running as replica of %s:%s\n", s.masterHost, s.masterPort)
		if err != nil {
			fmt.Println("Error connecting to master:", err)
			os.Exit(1)
		}
		defer masterConn.Close()
		masterConn.Write([]byte("PING\r\n"))
		// Send SYNC command to master
		// _, err = masterConn.Write([]byte("SYNC\r\n"))
		// if err != nil {
		// 	fmt.Println("Error sending SYNC command:", err)
		// 	os.Exit(1)
		// }
		// Here you would implement the logic to read and apply data from master
		// For simplicity, we just print a message
		fmt.Println("Sent SYNC command to master")
	}
	for {
		conn, err := s.AcceptConn()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		fmt.Println("Accepted connection from", conn.RemoteAddr())

		// Handle each connection in a new goroutine
		go func(c net.Conn) {
			Scanner := bufio.NewScanner(conn)
			for Scanner.Scan() {
				Text := Scanner.Text()
				handler, args, err := s.parseCommand(Text)
				if err != nil {
					writeErr(c, err.Error())
					continue
				}
				handler.Execute(c, args...)
			}
		}(conn)
	}
}
