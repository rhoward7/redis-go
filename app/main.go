package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type entry struct {
	value     string
	expiresAt time.Time // zero if no expiry
}

var db = make(map[string]entry)

func set(key, value string, ttl time.Duration) {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	db[key] = entry{
		value:     value,
		expiresAt: exp,
	}
}

func get(key string) (string, bool) {
	e, ok := db[key]
	if !ok {
		return "", false
	}

	// Check expiry
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		delete(db, key) // remove expired key
		return "", false
	}

	return e.value, true
}

func bulkString(msg string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(msg), msg)
}

func readRESPCommand(reader *bufio.Reader) (string, []string, error) {
	// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	line, err := reader.ReadString('\n')
	// *2\r
	if err != nil {
		return "", nil, err
	}

	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '*' {
		return "", nil, fmt.Errorf("invalid RESP array header: %q", line)
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", nil, fmt.Errorf("invalid array length: %v", err)
	}

	parts := make([]string, 0, count)

	for range count {
		// Expect a bulk string header: $<len>\r\n
		header, err := reader.ReadString('\n')
		//$4\r\nECHO\r
		if err != nil {
			return "", nil, err
		}
		header = strings.TrimSpace(header)
		if len(header) == 0 || header[0] != '$' {
			return "", nil, fmt.Errorf("expected bulk string, got: %q", header)
		}

		n, err := strconv.Atoi(header[1:])
		if err != nil {
			return "", nil, fmt.Errorf("invalid bulk string length: %v", err)
		}

		// Read the next n bytes (the string payload)
		//$4\r\nECHO\r
		buf := make([]byte, n)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return "", nil, err
		}

		// Discard the trailing \r\n
		if _, err = reader.Discard(2); err != nil {
			return "", nil, err
		}

		parts = append(parts, string(buf))
	}

	if len(parts) == 0 {
		return "", nil, fmt.Errorf("empty command")
	}

	cmd := strings.ToUpper(parts[0])
	args := parts[1:]
	return cmd, args, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		cmd, args, err := readRESPCommand(reader)
		if err != nil {
			fmt.Println("Client disconnected or read error:", err)
			return
		}

		switch strings.ToUpper(cmd) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			msg := args[0]
			conn.Write(bulkString(msg))
		case "SET":
			key := args[0]
			val := args[1]
			var ttl time.Duration

			if len(args) >= 4 {
				switch strings.ToUpper(args[2]) {
				case "EX":
					seconds, _ := strconv.Atoi(args[3])
					ttl = time.Duration(seconds) * time.Second
				case "PX":
					ms, _ := strconv.Atoi(args[3])
					ttl = time.Duration(ms) * time.Millisecond
				}
			}

			set(key, val, ttl)
			conn.Write([]byte("+OK\r\n")) // conn.write more performant but can do this
		case "GET":
			if val, ok := get(args[0]); ok {
				conn.Write(bulkString(val))
			} else {
				conn.Write([]byte("$-1\r\n")) // Redis nil
			}
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}

}
