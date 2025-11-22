package e2e

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// BrokerIsHealthy verifies the broker health check endpoint responds successfully
func BrokerIsHealthy() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("curl", "-f", healthCheckURL)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("broker health check failed: %w", err)
		}
		return nil
	}
}

// MessagesPublished verifies the expected number of messages were published
func MessagesPublished(count int) Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-publisher")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		lines := strings.Split(string(output), "\n")
		published := 0
		for _, line := range lines {
			if strings.Contains(line, "Message ") && strings.Contains(line, "published successfully") {
				published++
			}
		}
		if published != count {
			return fmt.Errorf("expected %d messages, got %d", count, published)
		}
		return nil
	}
}

// MessagesConsumed verifies that messages were consumed by checking consumer logs
func MessagesConsumed(count int) Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-consumer")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		re := regexp.MustCompile(`\[\d+\] Partition \d+, Offset \d+:`)
		matches := re.FindAllString(string(output), -1)
		consumed := len(matches)

		if consumed < count {
			return fmt.Errorf("expected at least %d messages consumed, got %d", count, consumed)
		}
		return nil
	}
}

// PublisherRetriedSuccessfully verifies publisher retry attempts in logs
func PublisherRetriedSuccessfully() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-publisher")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		logStr := string(output)
		if !strings.Contains(logStr, "Attempt") &&
			!strings.Contains(logStr, "retry") &&
			!strings.Contains(logStr, "Retrying") {
			return fmt.Errorf("no retry attempts found in publisher logs")
		}
		return nil
	}
}

// ConsumerJoinedGroup verifies consumer joined the group successfully
func ConsumerJoinedGroup() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-consumer")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		logStr := strings.ToLower(string(output))
		if !strings.Contains(logStr, "joined group") {
			return fmt.Errorf("consumer did not join group")
		}
		return nil
	}
}

// OffsetsCommitted verifies offsets were committed
func OffsetsCommitted() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-consumer")
		output, err := cmd.Output()
		if err != nil {
			return err
		}

		logStr := strings.ToLower(string(output))
		if !strings.Contains(logStr, "commit") {
			return fmt.Errorf("no offset commits found")
		}
		return nil
	}
}

// HeartbeatsSent verifies heartbeats were sent
func HeartbeatsSent() Expectation {
	return func(ctx *Context) error {
		cmd := exec.Command("docker", "logs", "broker-consumer")
		output, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("failed to get consumer logs: %w", err)
		}

		logStr := strings.ToLower(string(output))
		if !strings.Contains(logStr, "heartbeat") {
			return fmt.Errorf("no heartbeats found in consumer logs")
		}
		return nil
	}
}

func getLogsAfter(containerName string, after time.Time) (string, error) {
	cmd := exec.Command("docker", "logs", "--since", after.Format(time.RFC3339), containerName)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get logs for %s: %w", containerName, err)
	}
	return string(output), nil
}

func MessagesPublishedSince(count int, since time.Time) Expectation {
	return func(ctx *Context) error {
		output, err := getLogsAfter("broker-publisher", since)
		if err != nil {
			return err
		}

		re := regexp.MustCompile(`Message \d+ published successfully`)
		matches := re.FindAllString(output, -1)
		published := len(matches)

		if published != count {
			return fmt.Errorf("expected %d messages since %v, got %d", count, since, published)
		}
		return nil
	}
}
