package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
	"strings"
	"time"
	"strconv"
)

const CRLF = "\r\n"
type Server struct {
	Listener net.Listener
	Commands map[string] CommandHandler
}


// ------- Connection helpers ----------

func (s *Server) ListenForConn(){
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

func writeBulkStr(conn net.Conn, s string){
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

type EchoHandler struct {}
type PingHandler struct {}
type SetHandler struct {}
type GetHandler struct {}
type RpushHandler struct{}


type CommandHandler interface {
	Execute(conn net.Conn, args ...string)
}

var commands = map[string] CommandHandler{
	"PING": PingHandler{},
	"ECHO": EchoHandler{},
	"SET": SetHandler{},
	"GET": GetHandler{},
	"RPUSH": RpushHandler{},
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

type Bucket struct {
    Val        string
    PX         int           // TTL in ms
    ExpiryTime time.Time     // absolute expiry
    HasExpiry  bool          // whether to expire
}

var DB sync.Map

func (s SetHandler) Execute(conn net.Conn, args ...string) {
    if len(args) < 2 {
        writeErr(conn, "SET command needs at least 2 arguments: key and value")
        return
    }
    if len(args) > 4 {
        writeErr(conn, "SET command accepts at most 4 arguments")
        return
    }

    key, value := args[0], args[1]

    var (
        d         time.Duration
        hasExpiry bool
    )
    if len(args) == 4 {
        if strings.ToUpper(args[2]) != "PX" {
            writeErr(conn, "SET command expects PX as third argument")
            return
        }
        ms, err := strconv.Atoi(args[3])
        if err != nil || ms <= 0 {
            writeErr(conn, "PX value must be a positive integer")
            return
        }
        d = time.Duration(ms) * time.Millisecond
        hasExpiry = true
    }

    now := time.Now()
    bucket := Bucket{
        Val:        value,
        PX:         int(d.Milliseconds()),
        ExpiryTime: now.Add(d),
        HasExpiry:  hasExpiry,
    }

    DB.Store(key, bucket)

    if hasExpiry {
        time.AfterFunc(d, func() {
            DB.Delete(key)
        })
    }

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

func (r RpushHandler) Execute(conn net.Conn, args... string ){
	if len(args) < 2 {
		writeErr(conn, "RPUSH requires at least 2 t arguements")
	}
	key := args[0]
	items := args[1:]
	value, _:= DB.LoadOrStore(key, []string{})
	list := value.([]string)
	list = append(list, items...)
	DB.Store(key, list)

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

// ----------- main ------------

func main() {
	s := &Server{
		Commands: commands,

	}
	s.ListenForConn()
	defer s.CloseConn()
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
				if  err != nil {
					writeErr(c, err.Error())
					continue
				}
				handler.Execute(c, args...)
			}
		}(conn)
	}
}



















