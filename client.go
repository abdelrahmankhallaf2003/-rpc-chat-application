package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Message struct {
	Sender string
	Text   string
}

type ChatServer struct {
	mu          sync.Mutex
	users       map[string]bool
	messageLogs []Message
}

type JoinArgs struct{ Username string }
type LeaveArgs struct{ Username string }
type SendMsgArgs struct {
	Sender string
	Text   string
}
type PollArgs struct {
	Username  string
	LastIndex int
}

type PollReply struct {
	Messages []Message
	NewIndex int
}

func NewChatServer() *ChatServer {
	return &ChatServer{
		users:       make(map[string]bool),
		messageLogs: make([]Message, 0),
	}
}

func (s *ChatServer) Join(args *JoinArgs, reply *bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.users[args.Username] {
		*reply = false
		return fmt.Errorf("username '%s' is already taken", args.Username)
	}

	s.users[args.Username] = true
	*reply = true
	s.messageLogs = append(s.messageLogs, Message{
		Sender: "SYSTEM",
		Text:   fmt.Sprintf("User <%s> joined the chat.", args.Username),
	})
	fmt.Printf("User <%s> joined.\n", args.Username)
	return nil
}

func (s *ChatServer) Leave(args *LeaveArgs, reply *bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.users[args.Username] {
		delete(s.users, args.Username)
		s.messageLogs = append(s.messageLogs, Message{
			Sender: "SYSTEM",
			Text:   fmt.Sprintf("User <%s> left the chat.", args.Username),
		})
		fmt.Printf("User <%s> left.\n", args.Username)
		*reply = true
	} else {
		*reply = false
	}
	return nil
}

func (s *ChatServer) SendMessage(args *SendMsgArgs, reply *bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.users[args.Sender] {
		*reply = false
		return fmt.Errorf("user not registered")
	}

	s.messageLogs = append(s.messageLogs, Message{
		Sender: args.Sender,
		Text:   args.Text,
	})
	*reply = true
	return nil
}

func (s *ChatServer) GetUpdates(args *PollArgs, reply *PollReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply.Messages = make([]Message, 0)
	if args.LastIndex < len(s.messageLogs) {
		for i := args.LastIndex; i < len(s.messageLogs); i++ {
			msg := s.messageLogs[i]
			if msg.Sender != args.Username {
				reply.Messages = append(reply.Messages, msg)
			}
		}
		reply.NewIndex = len(s.messageLogs)
	} else {
		reply.NewIndex = args.LastIndex
	}

	return nil
}

func main() {
	chatServer := NewChatServer()
	err := rpc.Register(chatServer)
	if err != nil {
		fmt.Println("Error registering RPC server:", err)
		return
	}

	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Println("Error starting server listener:", err)
		return
	}
	defer listener.Close()

	fmt.Println("RPC Chat Server is running on port 1234...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down RPC server...")
		listener.Close()
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
