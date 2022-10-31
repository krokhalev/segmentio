package main

func main() {
	CreateTopicBeforePublication() // Missing topic creation before publication
	ConsumeAndCommit()
	ProduceMessages()
}
