package main

import (
	"bufio"
	"fmt"
	"go-broker/pkg/config"
	"go-broker/pkg/controller"
	"go-broker/pkg/disk"
	"go-broker/pkg/topic"
	"os"
	"strings"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Println("❌ Failed to load config:", err)
		os.Exit(1)
	}

	dm := disk.NewDiskManager(cfg.LogDir, cfg.BufferSize)
	tm := topic.NewTopicManager(cfg, dm)
	ctx := controller.NewClientContext("cli-group", 0)
	ch := controller.NewCommandHandler(tm, dm)

	fmt.Println("🔹 Broker ready. Type HELP for commands.")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.EqualFold(line, "EXIT") {
			break
		}
		result := ch.HandleCommand(line, ctx)
		fmt.Println(result)
	}
}
