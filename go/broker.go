package main

import brokerpkg "messageQ/mq/broker"

// Adapter to the mq/broker Broker so existing code that calls NewBroker() still works.
func NewBroker() *brokerpkg.Broker {
	return brokerpkg.NewBroker()
}
