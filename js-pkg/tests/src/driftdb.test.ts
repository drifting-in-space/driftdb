import { DbConnection, StateListener, Api, RoomResult, HttpConnection } from "driftdb";
import { expect, test } from "bun:test";
import { SequenceValue } from "driftdb/dist/types";

// "localhost" breaks on some versions of node because of this
// https://github.com/nodejs/undici/issues/1248#issuecomment-1214773044
const API_SERVER = "http://127.0.0.1:8080/";

class CallbackExpecter<T> {
    private resolve: ((v: T) => void) | null = null
    private nextValue: T | null = null
    private timeout: number | null = null

    expect(message: string, timeoutMillis = 5_000): Promise<T> {
        if (this.nextValue) {
            const value = this.nextValue
            this.nextValue = null
            return Promise.resolve(value)
        }

        if (this.resolve) {
            throw new Error("CallbackExpecter already has an expect call outstanding.");
        }

        return new Promise((resolve, reject) => {
            this.timeout = setTimeout(() => {
                reject(new Error(`${message} out.`));
            }, timeoutMillis) as any as number;
            this.resolve = resolve;
        });
    }

    accept = (value: T) => {
        if (this.timeout) {
            clearTimeout(this.timeout);
            this.timeout = null;
        }
        if (this.resolve) {
            this.resolve(value);
            this.resolve = null;
        } else {
            this.nextValue = value;
        }
    }
}

async function connectToNewRoom({binary}: {binary?: boolean} = {}): Promise<{ db: DbConnection, room: RoomResult }> {
    let api = new Api(API_SERVER);
    let room = await api.newRoom();
    let db = new DbConnection();
    await db.connect(room.socket_url, binary);
    return {db, room};
}

async function connectToRoom(room: RoomResult): Promise<DbConnection> {
    let db = new DbConnection();
    await db.connect(room.socket_url);
    return db;
}

test("Test room creation.", async () => {
    let api = new Api(API_SERVER);

    // Create a new room.
    let room = await api.newRoom();
    expect(room.room).not.toBeUndefined();
    expect(room.socket_url).not.toBeUndefined();
    expect(room.http_url).not.toBeUndefined();

    // If we access the same room, we should get the same result.
    let room2 = await api.getRoom(room.room);
    expect(room2.room).toEqual(room.room);
    expect(room2.socket_url).toEqual(room.socket_url);
    expect(room2.http_url).toEqual(room.http_url);

    let room3 = await api.newRoom();
    expect(room3.room).not.toEqual(room.room);
    expect(room3.socket_url).not.toEqual(room.socket_url);
    expect(room3.http_url).not.toEqual(room.http_url);
})

test("Test connecting and checking latency.", async () => {
    let {db} = await connectToNewRoom();

    // Check latency.
    let latency = await db.testLatency()
    expect(latency).not.toBeUndefined();

    db.disconnect()
})

test("Test optimistic set and get.", async () => {
    let {db} = await connectToNewRoom();

    let expecter = new CallbackExpecter<string>();
    let stateListener = new StateListener(expecter.accept, db, "key")

    stateListener.setStateOptimistic("foo")
    let result = await expecter.expect("Optimistic set not received.")
    expect(result).toEqual("foo")

    db.disconnect()
})

test("Send and receive binary.", async () => {
    let {db} = await connectToNewRoom({binary: true});

    let expecter = new CallbackExpecter<SequenceValue>();
    db.subscribe("key", expecter.accept)

    db.send({
        type: "push",
        key: "key",
        action: {type: "append"},
        value: "foo",
    })

    let result = await expecter.expect("Optimistic set not received.")
    expect(result).toEqual({
        seq: 1,
        value: "foo",
    })

    db.disconnect()
})

test("Send and receive UInt8Array.", async () => {
    let {db} = await connectToNewRoom({binary: true});

    let expecter = new CallbackExpecter<SequenceValue>();
    db.subscribe("key", expecter.accept)

    db.send({
        type: "push",
        key: "key",
        action: {type: "append"},
        value: {
            abc: "derp",
            v: new Uint8Array([1, 2, 3]),
        }
    })

    let result = await expecter.expect("Optimistic set not received.")
    expect(result).toEqual({
        seq: 1,
        value: {
            abc: "derp",
            v: new Uint8Array([1, 2, 3]),
        }
    })

    db.disconnect()
})

test("Test optimistic set and get.", async () => {
    let {db, room} = await connectToNewRoom();
    let db2 = await connectToRoom(room);

    let expecter = new CallbackExpecter<string>()
    let stateListener = new StateListener(expecter.accept, db, "key")

    let expecter2 = new CallbackExpecter<string>()
    new StateListener(expecter2.accept, db2, "key")

    stateListener.setStateOptimistic("foo")
    let result = await expecter.expect("Optimistic set not received.")
    expect(result).toEqual("foo")

    let result2 = await expecter2.expect("State set not received.")
    expect(result2).toEqual("foo")

    db.disconnect()
    db2.disconnect()
})

test("Test HTTP endpoint.", async () => {
    let api = new Api(API_SERVER);

    // Create a new room.
    let room = await api.newRoom();
    
    let conn = new HttpConnection(room.http_url)

    let result = await conn.send({
        type: "push",
        action: {type: "append"},
        key: "my-key",
        value: "foobar"
    })

    expect(result).toEqual(null)

    let result2 = await conn.send({
        type: "get",
        key: "my-key",
    })

    expect(result2).toEqual({
        data: [{
            seq: 1,
            value: "foobar",
        }],
        key: "my-key",
        type: "init"
    })
})
