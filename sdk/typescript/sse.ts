/**
 * Server-Sent Events (SSE) streaming support for Soroban Pulse
 */

import * as runtime from './runtime';

/**
 * SSE event types
 */
export enum SSEEventType {
  MESSAGE = 'message',
  PING = 'ping',
  CLOSE = 'close',
}

/**
 * Represents an SSE event from the server
 */
export interface SSEEvent {
  type: SSEEventType;
  data: string;
  id?: string;
}

/**
 * Options for SSE stream connection
 */
export interface SSEStreamOptions {
  /** Optional API key for authentication */
  apiKey?: string;
  /** Optional custom headers */
  headers?: Record<string, string>;
  /** Callback when a message event is received */
  onMessage?: (event: SSEEvent) => void;
  /** Callback when a ping event is received */
  onPing?: (timestamp: string) => void;
  /** Callback when the server closes the stream */
  onClose?: () => void;
  /** Callback when an error occurs */
  onError?: (error: Error) => void;
  /** Whether to automatically reconnect on close (default: true) */
  autoReconnect?: boolean;
  /** Maximum reconnection attempts (default: 5) */
  maxReconnectAttempts?: number;
  /** Initial reconnection delay in ms (default: 1000) */
  reconnectDelayMs?: number;
}

/**
 * Manages an SSE stream connection with automatic reconnection and Last-Event-ID support
 */
export class SSEStream {
  private eventSource: EventSource | null = null;
  private url: string;
  private options: Required<SSEStreamOptions>;
  private lastEventId: string | null = null;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private isManuallyClosed = false;

  constructor(url: string, options: SSEStreamOptions = {}) {
    this.url = url;
    this.options = {
      apiKey: options.apiKey,
      headers: options.headers || {},
      onMessage: options.onMessage || (() => {}),
      onPing: options.onPing || (() => {}),
      onClose: options.onClose || (() => {}),
      onError: options.onError || (() => {}),
      autoReconnect: options.autoReconnect !== false,
      maxReconnectAttempts: options.maxReconnectAttempts || 5,
      reconnectDelayMs: options.reconnectDelayMs || 1000,
    };

    // Load last event ID from storage if available
    this.loadLastEventId();
  }

  /**
   * Connect to the SSE stream
   */
  connect(): void {
    if (this.eventSource) {
      return; // Already connected
    }

    this.isManuallyClosed = false;
    const headers: Record<string, string> = { ...this.options.headers };

    // Add authentication header if API key is provided
    if (this.options.apiKey) {
      headers['X-Api-Key'] = this.options.apiKey;
    }

    // Add Last-Event-ID header for resumption
    if (this.lastEventId) {
      headers['Last-Event-ID'] = this.lastEventId;
    }

    // Create EventSource with headers
    const eventSourceInit: EventSourceInit = {};
    if (Object.keys(headers).length > 0) {
      // Note: EventSource doesn't support custom headers directly in the constructor
      // We'll need to use fetch-based approach for header support
      this.connectWithFetch(headers);
      return;
    }

    this.eventSource = new EventSource(this.url);
    this.setupEventListeners();
  }

  /**
   * Connect using fetch API to support custom headers
   */
  private connectWithFetch(headers: Record<string, string>): void {
    const controller = new AbortController();

    fetch(this.url, {
      method: 'GET',
      headers,
      signal: controller.signal,
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        if (!response.body) {
          throw new Error('Response body is null');
        }

        return this.readStream(response.body);
      })
      .catch((error) => {
        if (error.name !== 'AbortError') {
          this.options.onError(error);
          this.handleConnectionError();
        }
      });

    // Store controller for cleanup
    (this as any).fetchController = controller;
  }

  /**
   * Read and parse SSE stream from ReadableStream
   */
  private async readStream(body: ReadableStream<Uint8Array>): Promise<void> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');

        // Keep the last incomplete line in the buffer
        buffer = lines.pop() || '';

        for (const line of lines) {
          this.parseSSELine(line);
        }
      }
    } catch (error) {
      if (error instanceof Error) {
        this.options.onError(error);
      }
      this.handleConnectionError();
    } finally {
      reader.releaseLock();
    }
  }

  /**
   * Parse a single SSE line
   */
  private parseSSELine(line: string): void {
    if (!line.trim()) {
      // Empty line signals end of event
      return;
    }

    if (line.startsWith(':')) {
      // Comment, ignore
      return;
    }

    const colonIndex = line.indexOf(':');
    const field = colonIndex === -1 ? line : line.substring(0, colonIndex);
    const value = colonIndex === -1 ? '' : line.substring(colonIndex + 1).replace(/^ /, '');

    switch (field) {
      case 'event':
        this.handleSSEEvent(value);
        break;
      case 'data':
        this.handleSSEData(value);
        break;
      case 'id':
        this.lastEventId = value;
        this.saveLastEventId();
        break;
    }
  }

  /**
   * Handle SSE event type
   */
  private handleSSEEvent(eventType: string): void {
    switch (eventType) {
      case 'ping':
        this.options.onPing(new Date().toISOString());
        break;
      case 'close':
        this.options.onClose();
        this.disconnect();
        break;
      case 'message':
      default:
        // Data will be handled separately
        break;
    }
  }

  /**
   * Handle SSE data
   */
  private handleSSEData(data: string): void {
    try {
      const event: SSEEvent = {
        type: SSEEventType.MESSAGE,
        data,
        id: this.lastEventId || undefined,
      };
      this.options.onMessage(event);
    } catch (error) {
      if (error instanceof Error) {
        this.options.onError(error);
      }
    }
  }

  /**
   * Setup EventSource event listeners
   */
  private setupEventListeners(): void {
    if (!this.eventSource) return;

    this.eventSource.addEventListener('ping', (event: Event) => {
      const messageEvent = event as MessageEvent;
      this.lastEventId = messageEvent.lastEventId || this.lastEventId;
      this.saveLastEventId();
      this.options.onPing(messageEvent.data);
    });

    this.eventSource.addEventListener('close', () => {
      this.options.onClose();
      this.disconnect();
    });

    this.eventSource.addEventListener('message', (event: Event) => {
      const messageEvent = event as MessageEvent;
      this.lastEventId = messageEvent.lastEventId || this.lastEventId;
      this.saveLastEventId();

      try {
        const sseEvent: SSEEvent = {
          type: SSEEventType.MESSAGE,
          data: messageEvent.data,
          id: this.lastEventId || undefined,
        };
        this.options.onMessage(sseEvent);
      } catch (error) {
        if (error instanceof Error) {
          this.options.onError(error);
        }
      }
    });

    this.eventSource.onerror = () => {
      this.options.onError(new Error('EventSource connection error'));
      this.handleConnectionError();
    };
  }

  /**
   * Handle connection errors and attempt reconnection
   */
  private handleConnectionError(): void {
    this.disconnect();

    if (this.isManuallyClosed) {
      return;
    }

    if (!this.options.autoReconnect) {
      return;
    }

    if (this.reconnectAttempts >= this.options.maxReconnectAttempts) {
      this.options.onError(
        new Error(
          `Failed to reconnect after ${this.options.maxReconnectAttempts} attempts`
        )
      );
      return;
    }

    this.reconnectAttempts++;
    const delay = this.options.reconnectDelayMs * Math.pow(2, this.reconnectAttempts - 1);

    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, delay);
  }

  /**
   * Disconnect from the SSE stream
   */
  disconnect(): void {
    this.isManuallyClosed = true;

    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    const controller = (this as any).fetchController;
    if (controller) {
      controller.abort();
      (this as any).fetchController = null;
    }

    this.reconnectAttempts = 0;
  }

  /**
   * Save last event ID to storage (localStorage in browser, memory in Node.js)
   */
  private saveLastEventId(): void {
    if (!this.lastEventId) return;

    try {
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem(`soroban-pulse-sse-last-id-${this.url}`, this.lastEventId);
      }
    } catch (error) {
      // Silently fail if localStorage is not available
    }
  }

  /**
   * Load last event ID from storage
   */
  private loadLastEventId(): void {
    try {
      if (typeof localStorage !== 'undefined') {
        this.lastEventId =
          localStorage.getItem(`soroban-pulse-sse-last-id-${this.url}`) || null;
      }
    } catch (error) {
      // Silently fail if localStorage is not available
    }
  }

  /**
   * Get the current last event ID
   */
  getLastEventId(): string | null {
    return this.lastEventId;
  }

  /**
   * Check if the stream is currently connected
   */
  isConnected(): boolean {
    return this.eventSource !== null || (this as any).fetchController !== null;
  }
}
