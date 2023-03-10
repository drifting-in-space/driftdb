import { DbConnection } from 'driftdb'
import { MessageFromDb, SequenceValue } from 'driftdb/dist/types'
import { useEffect, useRef, useState } from 'react'
import PrettyJson from './components/PrettyJson'
import { StatusIndicator } from './db-react'

interface MessageProps {
  message: MessageFromDb
}

function Message(props: MessageProps) {
  return <PrettyJson value={props.message} />
}

function useDb(): DbConnection {
  let db = useRef<DbConnection>()
  if (!db.current) {
    db.current = new DbConnection()
  }
  return db.current
}

function App() {
  let [messages, setMessages] = useState<Array<MessageFromDb>>([])
  let [keyState, setKeyState] = useState<Record<string, Array<SequenceValue>>>({})
  let [autoRefreshState, setAutoRefreshState] = useState(true)

  let db = useDb()

  useEffect(() => {
    // Check for URL in query string if it exists.
    let url = new URL(window.location.href)
    let socketUrl
    if (url.searchParams.has('url')) {
      socketUrl = url.searchParams.get('url')!
    } else {
      socketUrl = 'ws://localhost:8080/api/ws'
    }
    if (autoRefreshState) {
      socketUrl += '?debug=true'
    }

    db.connect(socketUrl)
  }, [autoRefreshState, db])

  useEffect(() => {
    const listener = (message: MessageFromDb) => {
      if (message.type === 'init') {
        setKeyState((keyState) => {
          const _keyState = structuredClone(keyState)
          _keyState[message.key] = message.data
          return _keyState
        })
      } else if (message.type === 'push' && message.seq !== undefined) {
        const key = message.key
        setKeyState((keyState) => {
          const value = keyState[key] || []
          return {
            ...keyState,
            [key]: [...value, { value: message.value, seq: message.seq }]
          }
        })
      }

      setMessages((messages) => [...messages.slice(-10), message])
    }

    db.messageListener.addListener(listener)

    return () => {
      db.messageListener.removeListener(listener)
    }
  }, [db])

  return (
    <div className="container mx-auto flex flex-col space-y-2 py-6">
      <div className="flex flex-row space-x-8 items-center mb-4">
        <div className="grow flex flex-row space-x-4">
          <h1 className="font-bold">DriftDB</h1>
          <StatusIndicator database={db} />
        </div>

        <div>
          <label className="text-sm">
            <input
              type="checkbox"
              checked={autoRefreshState}
              onChange={(e) => setAutoRefreshState(e.target.checked)}
            />{' '}
            Auto Refresh State
          </label>
        </div>
      </div>

      <div className="flex flex-row space-x-4">
        <div className="grow flex-1">
          <h2 className="font-bold mb-2">State</h2>
          <div className="flex-col space-y-4">
            {Object.entries(keyState)
              .sort()
              .map(([key, value]) => {
                return (
                  <div
                    key={key}
                    className="flex-col space-y-4 bg-gray-100 rounded-lg p-4 overflow-y-scroll font-mono"
                  >
                    <div className="text-lg font-bold">{key}</div>
                    {value.map((value, i) => (
                      <div key={i} className="text-sm">
                        <PrettyJson value={value} />
                      </div>
                    ))}
                  </div>
                )
              })}
          </div>
        </div>

        <div className="grow flex-1">
          <h2 className="font-bold mb-2">Messages</h2>
          <div className="flex flex-col space-y-4 bg-gray-100 rounded-lg p-4 overflow-y-scroll font-mono">
            {messages.length > 1 ? (
              messages.map((message, i) => (
                <div key={i} className="text-sm">
                  <Message message={message} />
                </div>
              ))
            ) : (
              <div className="text-sm italic text-gray-500">Messages will appear here.</div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default App
