# Soroban Pulse TypeScript SDK

Auto-generated TypeScript client for the Soroban Pulse API.

## Features

- Modern `fetch` API (no external dependencies)
- Full TypeScript support with typed request parameters and response models
- Support for versioned (`v1`) and deprecated endpoints
- Built-in SSE streaming with automatic reconnection and Last-Event-ID support

## Installation

```bash
npm install
# or
yarn install
```

## Usage

### REST API

```typescript
import { DefaultApi, Configuration } from "./index";

const config = new Configuration({
  basePath: "http://localhost:3000",
});

const api = new DefaultApi(config);

// Get events for a contract
async function main() {
  const events = await api.getEventsByContract({
    contractId: "C...",
  });
  console.log(events.data);
}

main();
```

### SSE Streaming

The SDK provides built-in support for Server-Sent Events (SSE) streaming with automatic reconnection and Last-Event-ID resumption.

#### Stream all events

```typescript
import { EventsApi, Configuration } from "./index";

const config = new Configuration({
  basePath: "http://localhost:3000",
});

const api = new EventsApi(config);

const stream = api.streamEventsSSE({
  apiKey: "your-api-key", // optional
  onMessage: (event) => {
    const data = JSON.parse(event.data);
    console.log("New event:", data);
  },
  onPing: (timestamp) => {
    console.log("Server ping:", timestamp);
  },
  onClose: () => {
    console.log("Stream closed by server");
  },
  onError: (error) => {
    console.error("Stream error:", error);
  },
  autoReconnect: true,
  maxReconnectAttempts: 5,
  reconnectDelayMs: 1000,
});

stream.connect();

// Later, disconnect
stream.disconnect();
```

#### Stream events for a specific contract

```typescript
const stream = api.streamEventsByContractSSE("CABC...", {
  apiKey: "your-api-key",
  onMessage: (event) => {
    const data = JSON.parse(event.data);
    console.log("New event for contract:", data);
  },
});

stream.connect();
```

#### Stream events for multiple contracts

```typescript
const stream = api.streamMultiEventsSSE(["CABC...", "CDEF..."], {
  apiKey: "your-api-key",
  onMessage: (event) => {
    const data = JSON.parse(event.data);
    console.log("New event:", data);
  },
});

stream.connect();
```

### SSE Features

- **Automatic Reconnection**: The stream automatically reconnects on connection loss with exponential backoff
- **Last-Event-ID Support**: Event IDs are persisted in `localStorage` (browser) or memory (Node.js) and sent on reconnect to resume from where you left off
- **Authentication**: Pass an `apiKey` to automatically add the `X-Api-Key` header
- **Custom Headers**: Add custom headers via the `headers` option
- **Event Callbacks**: Handle `onMessage`, `onPing`, `onClose`, and `onError` events
- **Manual Control**: Call `connect()` and `disconnect()` to manage the connection lifecycle

### SSE Event Types

The server emits three types of SSE events:

- **message**: Contains a JSON-serialized event object
- **ping**: Sent every 15 seconds to keep the connection alive (data is an RFC 3339 timestamp)
- **close**: Sent when the server is shutting down (clients should reconnect)
