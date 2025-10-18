package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

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
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)))
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
