package net

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	stdnet "net"
	"syscall"

	"aetherdb/aether/api"
)

const (
	CmdPut byte = 1
	CmdGet byte = 2
	CmdDel byte = 3
)

// RequestHeader is a compact framing header for our binary protocol.
type RequestHeader struct {
	Cmd      byte
	KeyLen   uint32
	ValueLen uint32
}

// ResponseHeader encodes status and optional payload length.
type ResponseHeader struct {
	Status   byte
	ValueLen uint32
}

type Server struct {
	Addr string
	DB   *api.DB
}

func NewServer(addr string, db *api.DB) *Server {
	return &Server{Addr: addr, DB: db}
}

func (s *Server) ListenAndServe() error {
	ln, err := stdnet.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn stdnet.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	for {
		hdr, err := readHeader(br)
		if err != nil {
			if err != io.EOF {
				_ = writeStatus(conn, 2, 0)
			}
			return
		}
		key := make([]byte, hdr.KeyLen)
		if _, err := io.ReadFull(br, key); err != nil {
			_ = writeStatus(conn, 2, 0)
			return
		}
		switch hdr.Cmd {
		case CmdPut:
			val := make([]byte, hdr.ValueLen)
			if _, err := io.ReadFull(br, val); err != nil {
				_ = writeStatus(conn, 2, 0)
				return
			}
			if err := s.DB.Put(string(key), val); err != nil {
				_ = writeStatus(conn, 2, 0)
				continue
			}
			_ = writeStatus(conn, 0, 0)
		case CmdGet:
			meta, ok := s.DB.GetMeta(string(key))
			if !ok {
				_ = writeStatus(conn, 1, 0)
				continue
			}
			if err := writeStatus(conn, 0, meta.Length); err != nil {
				return
			}
			if err := zeroCopySend(conn, s.DB.FileDescriptor(), int64(meta.Offset), int(meta.Length)); err != nil {
				return
			}
		case CmdDel:
			if s.DB.Delete(string(key)) {
				_ = writeStatus(conn, 0, 0)
			} else {
				_ = writeStatus(conn, 1, 0)
			}
		default:
			_ = writeStatus(conn, 2, 0)
		}
	}
}

func readHeader(r io.Reader) (RequestHeader, error) {
	var b [9]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return RequestHeader{}, err
	}
	return RequestHeader{Cmd: b[0], KeyLen: binary.LittleEndian.Uint32(b[1:5]), ValueLen: binary.LittleEndian.Uint32(b[5:9])}, nil
}

func writeStatus(w io.Writer, status byte, ln uint32) error {
	var b [5]byte
	b[0] = status
	binary.LittleEndian.PutUint32(b[1:5], ln)
	_, err := w.Write(b[:])
	return err
}

// zeroCopySend uses sendfile so payload bytes move from page cache to socket without user-space copy.
func zeroCopySend(conn stdnet.Conn, fileFD int, off int64, length int) error {
	tcp, ok := conn.(*stdnet.TCPConn)
	if !ok {
		return fmt.Errorf("expected TCPConn")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}

	var opErr error
	remaining := length
	offset := off
	err = raw.Write(func(fd uintptr) bool {
		for remaining > 0 {
			n, err := syscall.Sendfile(int(fd), fileFD, &offset, remaining)
			if err == syscall.EINTR {
				continue
			}
			if err == syscall.EAGAIN {
				return false
			}
			if err != nil {
				opErr = err
				return true
			}
			remaining -= n
			if n == 0 {
				opErr = io.ErrUnexpectedEOF
				return true
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	if opErr != nil {
		return opErr
	}
	return nil
}
