package main

import (
	"bufio"
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
	Listener net.Listener
	Commands map[string]CommandHandler
	
}

var DB sync.Map

// ------- Connection helpers ----------

func (s *Server) ListenForConn() {
	ln, err := net.Listen("tcp", "0.0.0.0:6379")
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
	"INCR": IncrHandler{},
	
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

func (i IncrHandler) Execute(conn net.Conn, args ...string){
	if len(args) != 1 {
		writeErr(conn, "INCR requires only one arguement")
		return
	}
	key := args[0]
	val, ok := DB.Load(key)
	var curr int
	if !ok {
       curr = 0
	   
	} else {
		bucket, ok := val.(Bucket)
		if !ok {
			writeErr(conn, "ERR Operation against wrong type")
			return
		}
		i, err := strconv.Atoi(bucket.Val)
		if err != nil {
			writeErr(conn, "ERR value is not an integer")
			return
		}
		curr = i
	}
	curr ++
	DB.Store(key, Bucket{Val: strconv.Itoa(curr)})
	writeStr()
}

type Bucket struct {
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
    var bucket Bucket
    if hasExpiry {
        bucket = Bucket{
            Val:        value,
            PX:         int(d.Milliseconds()),
            ExpiryTime: now.Add(d),
            HasExpiry:  true,
        }
    } else {
        bucket = Bucket{Val: value}
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
	bucket, Ok := value.(Bucket)
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
                b := v.(Bucket)
                if b.HasExpiry && now.After(b.ExpiryTime) {
                    DB.Delete(k)
                }
                return true
            })
        }
    }()
}


// ----------- main ------------

func main() {
	startReaper(1 * time.Second)
	s := &Server{
		Commands: commands,
	}
	s.ListenForConn()
	defer s.CloseConn()
	fmt.Println("Listening on 0.0.0.0:6379")

	for {
		conn, err := s.AcceptConn()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		fmt.Println("Accepted connection from", conn.RemoteAddr())
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
