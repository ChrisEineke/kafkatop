package main

import (
	"flag"
	"fmt"
    "os"
	"sort"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
    topicFlag := flag.String("topic", "", "shows detailed information about a topic")
    versionFlag := flag.Bool("version", false, "prints version of librdkafka and exists")
    flag.Parse()

    if *versionFlag {
        versionInt, versionString := kafka.LibraryVersion()
        fmt.Printf("librdkafka v%s (%d)\n", versionString, versionInt)
        os.Exit(0)
    }

	clientConf := &kafka.ConfigMap{}
	clientConf.Set("bootstrap.servers=localhost")
	clientConf.Set("group.id=kafkatop")

	consumerClient, err := kafka.NewConsumer(clientConf)
	if err != nil {
		panic(err)
	}
	defer consumerClient.Close()

	adminClient, err := kafka.NewAdminClientFromConsumer(consumerClient)
	if err != nil {
		panic(err)
	}
	defer adminClient.Close()

	view := View{}
    if *topicFlag != "" {
        view.SwitchMode(TopicDrillDownMode)
        view.drilldownTopic = *topicFlag
    }

    var prevSnapshot, snapshot *Snapshot
	for {
		snapshot = NewSnapshot()
		snapshot, err := snapshot.Fill(consumerClient, adminClient, prevSnapshot)
		if err != nil {
			panic(err)
		}
		view.Draw(snapshot)
		view.Delay()
        prevSnapshot = snapshot
	}
}

type Broker struct {
	ID   int32
	Host string
	Port int
}

type Partition struct {
	ID              int32
	leader          int32
	replicas        []int32
	isrs            []int32
	lowWwatermark   int64
	highWatermark   int64
	totalMessageCnt int64
    messageRate     int64
}

type Topic struct {
	name       string
	partitions map[int32]Partition
}

type Snapshot struct {
	timestamp time.Time
	brokers   []Broker
	topics    map[string]Topic
}

func NewSnapshot() *Snapshot {
	return &Snapshot{
		timestamp: time.Time{},
		brokers:   make([]Broker, 0),
		topics:    map[string]Topic{},
	}
}

func (s *Snapshot) Fill(consumerClient *kafka.Consumer, adminClient *kafka.AdminClient, prevSnapshot *Snapshot) (*Snapshot, error) {
	s.timestamp = time.Now()

	metadata, err := adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		return nil, err
	}

	for _, broker := range metadata.Brokers {
		s.brokers = append(s.brokers, Broker{
			ID:   broker.ID,
			Host: broker.Host,
			Port: broker.Port,
		})
	}

	for _, topic := range metadata.Topics {
		if topic.Topic == "__consumer_offsets" {
			continue
		}
		s.topics[topic.Topic] = Topic{
			partitions: map[int32]Partition{},
		}
		for _, partition := range topic.Partitions {
			low, high, err := consumerClient.QueryWatermarkOffsets(topic.Topic, partition.ID, 5000)
			if err != nil {
				return nil, err
			}
            part := Partition{
				ID:              partition.ID,
				leader:          partition.Leader,
				replicas:        partition.Replicas,
				isrs:            partition.Isrs,
				lowWwatermark:   low,
				highWatermark:   high,
                totalMessageCnt: high - low,
                messageRate:     -1,
			}
            if prevSnapshot != nil {
			    part.messageRate = part.totalMessageCnt - prevSnapshot.topics[topic.Topic].partitions[partition.ID].totalMessageCnt
            }
			s.topics[topic.Topic].partitions[partition.ID] = part
		}
	}
	return s, nil
}

type ViewMode int

const (
	OverviewMode       ViewMode = 0
	TopicDrillDownMode ViewMode = 1
)

type View struct {
	mode           ViewMode
	drilldownTopic string
}

func (v *View) SwitchMode(newMode ViewMode) {
	v.mode = newMode
}

func (v *View) Draw(snapshot *Snapshot) {
	switch v.mode {
	case OverviewMode:
		v.DrawOverview(snapshot)
	case TopicDrillDownMode:
		v.DrawTopicDrillDown(snapshot)
	}
}

func (v *View) DrawOverview(snapshot *Snapshot) {
	fmt.Print("\033[H\033[2J")
	longestBrokerHost := 11
	for _, broker := range snapshot.brokers {
		if l := len(broker.Host); l > longestBrokerHost {
			longestBrokerHost = len(broker.Host)
		}
	}
	fmt.Printf("%v\t%*v\t%v\n", "Broker ID", longestBrokerHost, "Broker Host", "Broker Port")
	for _, broker := range snapshot.brokers {
		fmt.Printf("%9v\t%*v\t%11v\n", broker.ID, longestBrokerHost, broker.Host, broker.Port)
	}
	fmt.Println()

	longestTopicName := len("Topic")
	for _, topic := range snapshot.topics {
		if l := len(topic.name); l > longestTopicName {
			longestTopicName = l
		}
	}
	fmt.Printf("%v\t%v\t%v\t%v\n", "Topic", "No. of Partitions", "Total No. of Messages", "Message Rate")
	for _, topicName := range topicsSortedByName(snapshot.topics) {
		topic := snapshot.topics[topicName]
		numPartitions := len(topic.partitions)
		numTotalMessages := int64(0)
        messageRate := int64(0)
		for _, partition := range topic.partitions {
			numTotalMessages = numTotalMessages + partition.totalMessageCnt
            if partition.messageRate != -1 {
                messageRate = messageRate + partition.messageRate
            }
        }
		fmt.Printf("%*v\t%*v\t%*v\t%*v\n",
			longestTopicName,
			topicName,
			len("No. of Partitions"),
			numPartitions,
			len("Total No. of Messages"),
			numTotalMessages,
            len("Message Rate"),
            messageRate)
	}
}

func (v *View) DrawTopicDrillDown(snapshot *Snapshot) {
	fmt.Print("\033[H\033[2J")

	longestTopicName := len("Topic")
	for _, topic := range snapshot.topics {
		if l := len(topic.name); l > longestTopicName {
			longestTopicName = l
		}
	}
	fmt.Printf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
		"Topic", "Partition",
		"Leader", "Replicas", "Isrs",
		"Low Offset", "High Offset", "Total")
	for _, topicName := range topicsSortedByName(snapshot.topics) {
		if topicName != v.drilldownTopic {
			continue
		}
		topic := snapshot.topics[topicName]
		for _, partitionID := range partitionsSortedByID(topic.partitions) {
			partition := topic.partitions[partitionID]
			fmt.Printf("%*v\t%*v\t%*v\t%*v\t%*v\t%*v\t%*v\t%*v\n",
				longestTopicName,
				topicName,
				len("Partition"),
				partitionID,
				len("Leader"),
				partition.leader,
				len("Replicas"),
				partition.replicas,
				len("Isrs"),
				partition.isrs,
				len("Low Offset"),
				partition.lowWwatermark,
				len("High Offset"),
				partition.highWatermark,
				len("Total"),
				partition.totalMessageCnt)
		}
	}
}

func (v *View) Delay() {
	time.Sleep(1 * time.Second)
}

func topicsSortedByName(topics map[string]Topic) []string {
	var sortedTopicNames []string
	for topicName := range topics {
		sortedTopicNames = append(sortedTopicNames, topicName)
	}
	sort.Strings(sortedTopicNames)
	return sortedTopicNames
}

func partitionsSortedByID(partitions map[int32]Partition) []int32 {
	var sortedPartitionIDs []int32
	for partitionID := range partitions {
		sortedPartitionIDs = append(sortedPartitionIDs, partitionID)
	}
	sort.Slice(sortedPartitionIDs, func(i, j int) bool { return sortedPartitionIDs[i] < sortedPartitionIDs[j] })
	return sortedPartitionIDs
}
