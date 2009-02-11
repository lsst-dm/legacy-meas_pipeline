#! /usr/bin/env python

import lsst.daf.base as datap
import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "Issueing shutdown event ...\n"

    shutdownTopic = "triggerShutdownEvent"
    activemqBroker = "lsst8.ncsa.uiuc.edu"

    externalEventTransmitter = events.EventTransmitter(activemqBroker, shutdownTopic )

    root = datap.DataProperty.createPropertyNode("root");

    externalEventTransmitter.publish("eventtype", root)
    print "Sent.\n"

