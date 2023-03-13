import * as React from 'react'
import { DriftDBProvider, usePresence, useLatency } from 'driftdb-react'
import { Chat } from '../components/chat.jsx'
import { DRIFTDB_URL } from '../config'

const Peers = ({ peers, myLatency }) => {
  const listItems = peers.map(([sessionPeer, { id, latency }]) => (
    <tr key={id}>
      <td className="border px-4 mx-3"> {sessionPeer} </td>
      <td className="border px-4 mx-3"> {id} </td>
      <td className="border px-4 mx-3"> {latency + myLatency} ms </td>
    </tr>
  ))
  return (
    <table className="border p-4 m-5">
      <thead className="border">
        <tr>
          <th colSpan="3" className="border">
            PeerTable
          </th>
        </tr>
        <tr>
          <th className="border mx-3"> Session ID </th>
          <th> Connection ID </th>
          <th> Latency through DriftDB server </th>
        </tr>
      </thead>
      <tbody>{listItems}</tbody>
    </table>
  )
}

const Chats = ({ myId, peers }) => {
  const listChats = peers.map(([sessionPeer, connPeer]) => (
    <li key={sessionPeer}>
      <h3 className="underline"> Chat with {sessionPeer} </h3>
      <Chat key={connPeer} myId={myId} withId={connPeer} />
    </li>
  ))
  return <ul>{listChats}</ul>
}

const useNewUniqueID = () => {
  let [id, _] = React.useState(typeof crypto !== 'undefined' ? crypto.randomUUID() : 'PLACEHOLDER')
  return id
}

const sessionPeerToValue = (obj) =>
  Object.entries(obj).map(([sessionId, { value }]) => [sessionId, value])

const RTCArea = () => {
  const clientId = useNewUniqueID()
  const myLatency = useLatency()
  const others = usePresence('presence', { id: clientId, latency: myLatency })
  const sessionPeerToConnPeer = Object.entries(others).map(([sessionId, val]) => [
    sessionId,
    val.value.id
  ])
  const peers = sessionPeerToValue(others)

  return (
    <section>
      <Peers peers={peers} myLatency={myLatency} />
      <h1 className="text-4xl text-center m-5"> Chats </h1>
      <Chats peers={sessionPeerToConnPeer} myId={clientId} />
    </section>
  )
}

const App = () => {
  return (
    <DriftDBProvider api={DRIFTDB_URL}>
      <RTCArea />
    </DriftDBProvider>
  )
}

export default App
