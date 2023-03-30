import {
  Api,
  ConnectionStatus,
  DbConnection,
  PresenceListener,
  Reducer,
  RoomResult,
  StateListener,
  uniqueClientId,
  WrappedPresenceMessage,
  SyncedWebRTCConnections,
  DataChannelMsg
} from 'driftdb'
import React, { useCallback, useEffect, useRef, SetStateAction, useState } from 'react'

const ROOM_ID_KEY = '_driftdb_room'

function getRoomId(): string | null {
  const url = new URL(document.location.href)
  return url.searchParams.get(ROOM_ID_KEY)
}

function setRoomId(roomId: string): void {
  const url = new URL(document.location.href)
  url.searchParams.set(ROOM_ID_KEY, roomId)
  window.history.replaceState({}, '', url.toString())
}

/**
 * A React component that provides a `DbConnection` to all child components.
 *
 * @param props The props for the component.
 */
export function DriftDBProvider(props: {
  /** Elements under the provider in the tree. */
  children: React.ReactNode

  /** The URL of the DriftDB API. */
  api: string

  /** The room ID to connect to. If not provided, attempts to extract the room ID
   *  from the URL and creates a new room if one is not present. */
  room?: string

  /** Whether to use binary messages (enables raw typed arrays in messages). */
  useBinary?: boolean
}): React.ReactElement {
  const dbRef = useRef<DbConnection | null>(null)
  if (dbRef.current === null) {
    dbRef.current = new DbConnection()
  }

  React.useEffect(() => {
    let api = new Api(props.api)

    let roomId
    if (props.room) {
      roomId = props.room
    } else {
      roomId = getRoomId()
    }

    let promise
    if (roomId) {
      promise = api.getRoom(roomId)
    } else {
      promise = api.newRoom()
    }

    promise.then((result: RoomResult) => {
      if (!props.room) {
        setRoomId(result.room)
      }

      dbRef.current?.connect(result.socket_url, props.useBinary)
    })

    return () => {
      dbRef.current?.disconnect()
    }
  }, [props.room, props.useBinary, props.api])

  return <DatabaseContext.Provider value={dbRef.current}>{props.children}</DatabaseContext.Provider>
}

/**
 * A React context which is used to pass a database connection down the component tree
 * via the `DriftDBProvider` provider and `useDatabase` hook.
 */
export const DatabaseContext = React.createContext<DbConnection | null>(null)

/**
 * A React hook that returns a handle to the current database connection provided by the
 * nearest `DriftDBProvider` in the tree. If there is no `DriftDBProvider` in the tree,
 * throws an error.
 *
 * @returns A handle to the current database connection.
 */
export function useDatabase(): DbConnection {
  const db = React.useContext(DatabaseContext)
  if (db === null) {
    throw new Error('useDatabase must be used within a DriftDBProvider')
  }
  return db
}

type SetterFunction<T> = (value: T | ((v: T) => T)) => void

/**
 * A React hook that returns the current value of a shared state variable, and a function
 * to update it. The state variable is identified by a key, which must be unique within the
 * current room.
 *
 * @param key The key of the state variable.
 * @param initialValue The initial value of the state variable.
 *
 * @returns A tuple containing the current value of the state variable, and a function to
 * update it.
 */
export function useSharedState<T>(key: string, initialValue: T): [T, SetterFunction<T>] {
  const db = useDatabase()
  const [state, setInnerState] = React.useState<T>(initialValue)

  const stateListener = useRef<StateListener<SetStateAction<T>> | null>(null)

  useEffect(() => {
    stateListener.current = new StateListener({
      key,
      db,
      callback: setInnerState
    })

    stateListener.current!.subscribe()

    return () => {
      stateListener.current!.destroy()
    }
  }, [])

  const setState = useCallback(
    (value: T | ((v: T) => T)) => {
      if (typeof value === 'function') {
        const currentValue = stateListener.current!.state ?? initialValue
        const newValue = (value as any)(currentValue)
        stateListener.current?.setStateOptimistic(newValue)
      } else {
        stateListener.current?.setStateOptimistic(value)
      }
    },
    [initialValue]
  )

  return [state, setState]
}

/**
 * A React component that displays a QR code containing the current URL, including the room ID.
 * If there is no room ID in the URL, this component will not render anything.
 */
export function RoomQRCode(): React.ReactElement {
  const db = useDatabase()
  const [pageUrl, setPageUrl] = useState<string | null>(null)

  useEffect(() => {
    const callback = () => {
      if (getRoomId() !== null) {
        setPageUrl(document.location.href)
      }
    }

    db.statusListener.addListener(callback)

    return () => {
      db.statusListener.removeListener(callback)
    }
  }, [db])

  if (pageUrl) {
    return <img src={`https://api.jamsocket.live/qrcode?url=${pageUrl}`} />
  } else {
    return <></>
  }
}

/**
 * A React hook that returns a unique client ID for the current client. This ID is maintained
 * in the browser’s session storage, so it is retained across page reloads.
 *
 * @returns A unique client ID.
 */
export function useUniqueClientId(): string {
  const currentId = useRef<string>()

  if (typeof window === 'undefined') {
    return null!
  }

  if (!currentId.current) {
    currentId.current = uniqueClientId()
  }
  return currentId.current!
}

export function useSharedReducer<State, Action>(
  key: string,
  reducer: (state: State, action: Action) => State,
  initialValue: State
): [State, (action: Action) => void]

export function useSharedReducer<State, Action, InitialValue>(
  key: string,
  reducer: (state: State, action: Action) => State,
  initialValue: InitialValue,
  init: (initialValue: InitialValue) => State
): [State, (action: Action) => void]

/**
 * A React hook that returns a reducer state variable, and a function to update it. The state
 * variable is identified by a key, which must be unique within the current room.
 *
 * @param key The key that uniquely identifies the state variable within the current room.
 * @param reducer A reducer function that will be used to update the state variable.
 * @param initialValue The initial value of the state variable (if `init` is not passed),
 * or the value passed into `init` to produce the initial value.
 * @param init An optional function that will be used to produce the initial value of the
 * state variable.
 */
export function useSharedReducer<State, Action>(
  key: string,
  reducer: (state: State, action: Action) => State,
  initialValue: unknown,
  init: (v: any) => State = (a: any) => a
): [State, (action: Action) => void] {
  const db = useDatabase()

  const initialStateRef = useRef<State>(null!)
  if (initialStateRef.current === null) {
    initialStateRef.current = structuredClone(init(initialValue))
  }

  const [state, setState] = React.useState<State>(initialStateRef.current)

  const reducerRef = React.useRef<Reducer<State, Action> | null>(null)
  if (reducerRef.current === null) {
    reducerRef.current = new Reducer({
      key,
      reducer,
      initialValue: initialStateRef.current,
      sizeThreshold: 30,
      db,
      callback: setState
    })
  }

  useEffect(() => {
    reducerRef.current!.subscribe()

    return () => {
      reducerRef.current!.destroy()
    }
  }, [])

  const dispatch = reducerRef.current.dispatch

  return [state, dispatch]
}

/**
 * A React hook that returns the current connection status of the database
 * from the current `DriftDBProvider`.
 * The result is an object with a `connected` property that is `true` if the
 * database is connected to the server. When `connected` is `true`, a `debugUrl`
 * property is also returned.
 *
 * @returns The current connection status of the database.
 */
export function useConnectionStatus(): ConnectionStatus {
  const db = useDatabase()
  const [status, setStatus] = React.useState<ConnectionStatus>({ connected: false })

  React.useEffect(() => {
    const callback = (event: ConnectionStatus) => {
      setStatus(event)
    }
    db?.statusListener.addListener(callback)
    return () => {
      db?.statusListener.removeListener(callback)
    }
  }, [db])

  return status
}

/**
 * A React hook that measures the latency of the database connection in a
 * loop and returns the current latency in milliseconds, or `null` before
 * the first measurement.
 */
export function useLatency(): number | null {
  const db = useDatabase()
  const [latency, setLatency] = useState<number | null>(null!)

  React.useEffect(() => {
    const updateLatency = async () => {
      const result = await db?.testLatency()
      setLatency(result)
    }

    const interval = setInterval(updateLatency, 5000)
    updateLatency()

    return () => {
      clearInterval(interval)
    }
  }, [db])

  return latency
}

type AnyFunc = (...args: any[]) => void
function throttle(fn: AnyFunc, durationMs: number): AnyFunc {
  let lastTime = 0
  return (...args) => {
    let curTime = Date.now()
    if (curTime - lastTime > durationMs) {
      fn(...args)
      lastTime = curTime
    }
  }
}

/**
 * A React hook that creates a WebRTC based broadcast channel. sending messages to the channel
 * will send messages to all peers in the same DriftDB room. It takes a callback that will run
 * every `throttleMs` milliseconds with an object containing a mapping from peer ID to that peer's
 * most recent message.
 *  @param throttleMs minimum interval between setRtcMap calls
 *  @param setRtcMap function that gets called with a record from peers to their most recent message
 *  @returns function that takes a message and sends it to all peers.
 */
function useWebRtcBroadcastChannel<T>(
  throttleMs = 0,
  setRtcMap: (map: Record<string, WrappedPresenceMessage<T>>) => void
) {
  const db = useDatabase()
  const id = useUniqueClientId()
  const WebRtcBroadcastChannelRef = React.useRef<SyncedWebRTCConnections>()
  const send = React.useCallback((msg: string) => WebRtcBroadcastChannelRef.current!.send(msg), [])
  if (!WebRtcBroadcastChannelRef.current) {
    let rtcconns = new SyncedWebRTCConnections(db, id, throttleMs)
    rtcconns.setOnMessage(
      throttle((_msg) => setRtcMap({ ...rtcconns.getPeersToLastMsg() }), throttleMs)
    )
    WebRtcBroadcastChannelRef.current = rtcconns
  }
  return send
}

/**
 * A React hook that returns a map of the current presence of all clients in the current room.
 * The client also passes its own value, which wil be included in the map for other clients
 *  @param value The value that will be included in the map for other clients.
 *  @param throttle The minimum interval between messages being sent.
 *         NOTE: any messages sent in the interval will be dropped.
 *  @returns A map of the current presence of all clients in the current room.
 */
export function useWebRtcPresence<T>(
  value: T,
  throttle = 0
): Record<string, WrappedPresenceMessage<T>> {
  const [rtcMap, setRtcMap] = useState<Record<string, WrappedPresenceMessage<T>>>({})
  const send = useWebRtcBroadcastChannel(throttle, setRtcMap)
  React.useEffect(() => {
    send(JSON.stringify(value))
  }, [value])
  return rtcMap
}

/**
 * A React hook that returns a map of the current presence of all clients in the current room.
 * The client also passes its own value, which will be included in the map for other clients.
 *
 * @param key The key that uniquely identifies the presence variable within the current room.
 * @param value The value that will be included in the map for other clients.
 * @returns A map of the current presence of all clients in the current room.
 */
export function usePresence<T>(key: string, value: T): Record<string, WrappedPresenceMessage<T>> {
  const db = useDatabase()
  const clientId = useUniqueClientId()
  const [presence, setPresence] = useState<Record<string, WrappedPresenceMessage<T>>>({})

  const presenceListener = useRef<PresenceListener<T>>()
  if (presenceListener.current === undefined) {
    presenceListener.current = new PresenceListener({
      key,
      db,
      clientId,
      initialState: value,
      callback: setPresence
    })
  }

  useEffect(() => {
    presenceListener.current!.subscribe()

    return () => {
      presenceListener.current!.destroy()
    }
  }, [presenceListener.current])

  presenceListener.current.updateState(value)

  return presence
}

/**
 * A React component that displays the current connection status of the database.
 */
export function StatusIndicator(): React.ReactElement {
  const status = useConnectionStatus()
  const latency = useLatency()
  const latencyStr = latency === null ? '...' : Math.round(latency).toString()

  let color
  if (status.connected) {
    color = 'green'
  } else {
    color = 'red'
  }

  return (
    <div
      style={{
        display: 'inline-block',
        border: '1px solid #ccc',
        background: '#eee',
        borderRadius: 10,
        padding: 10
      }}
    >
      DriftDB status:{' '}
      <span style={{ color, fontWeight: 'bold' }}>
        {status.connected ? 'Connected' : 'Disconnected'}
      </span>
      {status.connected ? (
        <>
          {' '}
          <span style={{ fontSize: '70%', color: '#aaa' }}>
            <a
              target="_blank"
              rel="noreferrer"
              style={{ textDecoration: 'none', color: '#aaa' }}
              href={status.debugUrl}
            >
              (ui)
            </a>
            ({latencyStr}ms)
          </span>
        </>
      ) : null}
    </div>
  )
}
