import * as React from 'react'
import { useSharedReducer } from 'driftdb-react'


const messageReducer = (state, action) => {
    switch(action.type) {
    case "message":
	return [...state, action.message]
    case  "remove":
	return [...state.slice(action.amount)]
    }
}

export function useDriftDBSignalingChannel(p1, p2) {
    let [recvMessages, modifyRecvSigChannel] = useSharedReducer(p2+p1, messageReducer, [])
    let [_sendMessages, sendSignalingAction] = useSharedReducer(p1+p2, messageReducer, [])

    const sendSignalingMessage = (message) => sendSignalingAction({ type: "message", message })

    const remove = (amount) => {
	modifyRecvSigChannel({ type: "remove" , amount})
    }

    const emptySend = () => {
	sendSignalingAction({ type:"remove", amount: _sendMessages.length })
    }

    return [recvMessages, sendSignalingMessage, remove, emptySend]
}
