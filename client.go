package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Message struct {
	Sender string
	Text   string
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

func pollMessages(client *rpc.Client, username string, lastIndex *int, stopChan chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			args := PollArgs{Username: username, LastIndex: *lastIndex}
			var reply PollReply
			err := client.Call("ChatServer.GetUpdates", &args, &reply)
			if err != nil {
				return
			}

			if reply.NewIndex > *lastIndex {
				*lastIndex = reply.NewIndex
				for _, msg := range reply.Messages {
					if msg.Sender == "SYSTEM" {
						fmt.Printf("\n[NOTIFICATION]: %s\n> ", msg.Text)
					} else {
						fmt.Printf("\n[%s]: %s\n> ", msg.Sender, msg.Text)
					}
				}
			}
		}
	}
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		fmt.Println("Error connecting to server. Is the server running?")
		return
	}
	defer client.Close()

	scanner := bufio.NewScanner(os.Stdin)
	var username string

	for {
		fmt.Print("Enter your username: ")
		if !scanner.Scan() {
			return
		}
		username = strings.TrimSpace(scanner.Text())
		if username == "" {
			fmt.Println("Username cannot be empty.")
			continue
		}

		var success bool
		err := client.Call("ChatServer.Join", &JoinArgs{Username: username}, &success)
		if err != nil || !success {
			fmt.Printf("Could not join: %v\n", err)
			continue
		}
		break
	}

	fmt.Printf("--- Welcome to RPC Chat, %s! (Type '/quit' to leave) ---\n", username)

	lastIndex := 0
	stopChan := make(chan struct{})
	go pollMessages(client, username, &lastIndex, stopChan)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		var reply bool
		_ = client.Call("ChatServer.Leave", &LeaveArgs{Username: username}, &reply)
		os.Exit(0)
	}()

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}

		if text == "/quit" {
			close(stopChan)
			var reply bool
			_ = client.Call("ChatServer.Leave", &LeaveArgs{Username: username}, &reply)
			fmt.Println("Disconnected from chat.")
			break
		}

		var reply bool
		err := client.Call("ChatServer.SendMessage", &SendMsgArgs{Sender: username, Text: text}, &reply)
		if err != nil {
			fmt.Println("Error sending message:", err)
		}
	}
}
