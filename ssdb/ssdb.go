package ssdb

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)
//Client : SSDB client
type Client struct {
	rSock    *net.TCPConn
	wSock    *net.TCPConn
	recv_buf bytes.Buffer
	process  chan []interface{}
	result   chan ClientResult
	quit     chan int
	mu       *sync.Mutex
	Closed   bool
}

//ClientResult : represent result

type ClientResult struct {
	ID    string
	Data  []string
	Error error
}

// Connect : return *ssdb.Client
func Connect(ip string, port int) (*Client, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port);
	if err != nil {
		return nil, err
	}
	readSock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	writeSock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Println("SSDB Client dial failed:", err, c.Id)
		return err
	}
	var c Client
	c.rSock = readSock
	c.wSock = writeSock
	c.process = make(chan []interface{})
	c.result = make(chan ClientResult)
	c.quit = make(chan int)
	c.Closed = false
	c.mu = &sync.Mutex{}
	go func() {
		for {
			select {
			default:
				args := <-c.process
				runID := args[0].(string)
				runArgs := args[1:]
				rs, err := c.do(runArgs)
				c.result <- ClientResult{ID: runID, Data: rs, Error: err}
			case <-c.quit:
				log.Println("close Do routine")
			}
		}
	}()
	return &c, nil
}

// Close The Client Connection
func (c *Client) Close() error {
	if !c.Closed {
		c.mu.Lock()
		c.Closed = true
		close(c.process)
		close(c.quit)
		close(c.result)
		c.rSock.Close()
		c.wSock.Close()
		c.mu.Unlock()
	}
	return nil
}

//Do : exposed API for unimplemented command
func (c *Client) Do(args ...interface{}) ([]string, error) {
	var rs []string
	var err error
	if c.Closed {
		return nil, fmt.Errorf("Connection has been closed.")
	}
	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	cmd := append([]interface{}{runID}, args)
	c.process <- cmd
	for result := range c.result {
		if result.ID == runID {
			rs = result.Data
			err = result.Error
			break
		}
		c.result <- result
	}
	return rs, err
}

//Auth : auth
func (c *Client) Auth(pwd string) (interface{}, error) {
	return c.processCmd("auth", []interface{}{pwd})
}

//Set : set
func (c *Client) Set(key string, val string) (interface{}, error) {
	params := []interface{}{key, val}
	return c.processCmd("set", params)
}

//Get : get
func (c *Client) Get(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.processCmd("get", params)
}

//Del : del
func (c *Client) Del(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.processCmd("del", params)
}

func (c *Client) processCmd(cmd string, args []interface{}) (interface{}, error) {
	if c.Closed {
		return nil, fmt.Errorf("Connection has been closed.")
	}
	args = append([]interface{}{cmd}, args...)
	log.Printf("XXXXX-> %v", args)
	resp, _ := c.Do(args)
	return resp, nil
	/*
		if len(resp) == 2 && resp[0] == "ok" {
			switch cmd {
			case "set", "del":
				return true, nil
			case "expire", "setnx", "auth", "exists", "hexists":
				if resp[1] == "1" {
					return true, nil
				}
				return false, nil
			case "hsize":
				val, err := strconv.ParseInt(resp[1], 10, 64)
				return val, err
			default:
				return resp[1], nil
			}
		} else if len(resp) == 1 && resp[0] == "not_found" {
			return nil, fmt.Errorf("%v", resp[0])
		} else {
			if len(resp) >= 1 && resp[0] == "ok" {
				switch cmd {
				case "hgetall", "hscan", "hrscan", "multi_hget", "scan", "rscan":
					list := make(map[string]string)
					length := len(resp[1:])
					data := resp[1:]
					for i := 0; i < length; i += 2 {
						list[data[i]] = data[i+1]
					}
					return list, nil
				default:
					return resp[1:], nil
				}
			}
		}
		return nil, fmt.Errorf("sholdn't be here")
	*/
}

//need improve batch mode
func (c *Client) do(args []interface{}) ([]string, error) {
	err := c.send(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.recv()
	if err != nil {
		return nil, err
	}
	return resp, nil
}
func (c *Client) send(args []interface{}) error {
	var buf bytes.Buffer
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case []string:
			for _, s := range arg {
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case int:
			s = fmt.Sprintf("%d", arg)
		case int64:
			s = fmt.Sprintf("%d", arg)
		case float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	_, err := c.wSock.Write(buf.Bytes())
	return err
}

func (c *Client) recv() ([]string, error) {
	var tmp [8192]byte
	for {
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
		n, err := c.wSock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.recv_buf.Write(tmp[0:n])
	}
}

func (c *Client) parse() []string {
	resp := []string{}
	buf := c.recv_buf.Bytes()
	var Idx, offset int
	Idx = 0
	offset = 0
	for {
		Idx = bytes.IndexByte(buf[offset:], '\n')
		if Idx == -1 {
			break
		}
		p := buf[offset : offset+Idx]
		offset += Idx + 1
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.recvBuf.Next(offset)
				return resp
			}
		}
		pIdx := strings.Replace(strconv.Quote(string(p)), `"`, ``, -1)
		size, err := strconv.Atoi(pIdx)
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= c.recv_buf.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}
	return []string{}
}

func (c *Client) UnZip(data []byte) []string {
	var buf bytes.Buffer
	buf.Write(data)
	zipReader, err := gzip.NewReader(&buf)
	if err != nil {
		log.Println("[ERROR] New gzip reader:", err)
	}
	defer zipReader.Close()

	zipData, err := ioutil.ReadAll(zipReader)
	if err != nil {
		fmt.Println("[ERROR] ReadAll:", err)
		return nil
	}
	var resp []string

	if zipData != nil {
		Idx := 0
		offset := 0
		hiIdx := 0
		for {
			Idx = bytes.IndexByte(zipData, '\n')
			if Idx == -1 {
				break
			}
			p := string(zipData[:Idx])
			size, err := strconv.Atoi(string(p))
			if err != nil || size < 0 {
				zipData = zipData[Idx+1:]
				continue
			} else {
				offset = Idx + 1 + size
				hiIdx = size + Idx + 1
				resp = append(resp, string(zipData[Idx+1:hiIdx]))
				zipData = zipData[offset:]
			}

		}
	}
	return resp
}
