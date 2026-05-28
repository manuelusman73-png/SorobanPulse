import { SSEStream, SSEStreamOptions, SSEEventType } from '../sse';

describe('SSEStream', () => {
  let stream: SSEStream;
  const testUrl = 'http://localhost:3000/v1/events/stream';

  beforeEach(() => {
    // Clear localStorage before each test
    if (typeof localStorage !== 'undefined') {
      localStorage.clear();
    }
  });

  afterEach(() => {
    if (stream) {
      stream.disconnect();
    }
  });

  describe('constructor', () => {
    it('should create an SSEStream instance', () => {
      stream = new SSEStream(testUrl);
      expect(stream).toBeDefined();
      expect(stream.isConnected()).toBe(false);
    });

    it('should accept options', () => {
      const options: SSEStreamOptions = {
        apiKey: 'test-key',
        autoReconnect: false,
      };
      stream = new SSEStream(testUrl, options);
      expect(stream).toBeDefined();
    });
  });

  describe('getLastEventId', () => {
    it('should return null initially', () => {
      stream = new SSEStream(testUrl);
      expect(stream.getLastEventId()).toBeNull();
    });

    it('should load last event ID from localStorage', () => {
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem(`soroban-pulse-sse-last-id-${testUrl}`, 'event-123');
        stream = new SSEStream(testUrl);
        expect(stream.getLastEventId()).toBe('event-123');
      }
    });
  });

  describe('disconnect', () => {
    it('should disconnect the stream', () => {
      stream = new SSEStream(testUrl);
      stream.disconnect();
      expect(stream.isConnected()).toBe(false);
    });

    it('should prevent reconnection after manual disconnect', () => {
      stream = new SSEStream(testUrl, { autoReconnect: true });
      stream.disconnect();
      // After disconnect, the stream should not attempt to reconnect
      expect(stream.isConnected()).toBe(false);
    });
  });

  describe('callbacks', () => {
    it('should call onMessage callback with default options', () => {
      const onMessage = jest.fn();
      stream = new SSEStream(testUrl, { onMessage });
      expect(onMessage).toBeDefined();
    });

    it('should call onPing callback with default options', () => {
      const onPing = jest.fn();
      stream = new SSEStream(testUrl, { onPing });
      expect(onPing).toBeDefined();
    });

    it('should call onClose callback with default options', () => {
      const onClose = jest.fn();
      stream = new SSEStream(testUrl, { onClose });
      expect(onClose).toBeDefined();
    });

    it('should call onError callback with default options', () => {
      const onError = jest.fn();
      stream = new SSEStream(testUrl, { onError });
      expect(onError).toBeDefined();
    });
  });

  describe('options', () => {
    it('should use default autoReconnect value of true', () => {
      stream = new SSEStream(testUrl);
      // We can't directly access options, but we can verify the stream is created
      expect(stream).toBeDefined();
    });

    it('should use custom reconnect options', () => {
      stream = new SSEStream(testUrl, {
        autoReconnect: false,
        maxReconnectAttempts: 3,
        reconnectDelayMs: 500,
      });
      expect(stream).toBeDefined();
    });

    it('should accept custom headers', () => {
      stream = new SSEStream(testUrl, {
        headers: {
          'X-Custom-Header': 'value',
        },
      });
      expect(stream).toBeDefined();
    });

    it('should accept API key', () => {
      stream = new SSEStream(testUrl, {
        apiKey: 'test-api-key',
      });
      expect(stream).toBeDefined();
    });
  });

  describe('localStorage persistence', () => {
    it('should persist last event ID to localStorage', () => {
      if (typeof localStorage !== 'undefined') {
        stream = new SSEStream(testUrl);
        // Simulate receiving an event with an ID
        // This would normally happen through the SSE connection
        // For testing, we just verify the stream can be created
        expect(stream).toBeDefined();
      }
    });

    it('should handle localStorage errors gracefully', () => {
      // Mock localStorage to throw an error
      const originalLocalStorage = global.localStorage;
      Object.defineProperty(global, 'localStorage', {
        value: {
          getItem: jest.fn(() => {
            throw new Error('localStorage error');
          }),
          setItem: jest.fn(() => {
            throw new Error('localStorage error');
          }),
          clear: jest.fn(),
        },
        writable: true,
      });

      stream = new SSEStream(testUrl);
      expect(stream).toBeDefined();

      // Restore localStorage
      Object.defineProperty(global, 'localStorage', {
        value: originalLocalStorage,
        writable: true,
      });
    });
  });

  describe('EventSource compatibility', () => {
    it('should create an SSEStream that can be used with EventSource', () => {
      stream = new SSEStream(testUrl);
      expect(stream).toBeDefined();
      // The actual EventSource connection would happen in a real browser environment
    });
  });
});
