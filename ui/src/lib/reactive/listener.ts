type NativeEventMap<T> = (T extends Window | typeof globalThis
  ? WindowEventMap
  : T extends Document
    ? DocumentEventMap
    : T extends HTMLElement
      ? HTMLElementEventMap
      : T extends MediaQueryList
        ? MediaQueryListEventMap
        : T extends VisualViewport
          ? VisualViewportEventMap
          : T extends ServiceWorkerContainer
            ? ServiceWorkerContainerEventMap
            : T extends MessagePort
              ? MessagePortEventMap
              : T extends WebSocket
                ? WebSocketEventMap
                : T extends Worker
                  ? WorkerEventMap
                  : T extends RTCDataChannel
                    ? RTCDataChannelEventMap
                    : T extends RTCPeerConnection
                      ? RTCPeerConnectionEventMap
                      : T extends MediaStreamTrack
                        ? MediaStreamTrackEventMap
                        : T extends FileReader
                          ? FileReaderEventMap
                          : Record<string, Event>) &
  Record<string, Event>;

export function listen<T extends EventTarget, K extends string>(
  target: T,
  type: K,
  handler: ((event: NativeEventMap<T>[K]) => void) | EventListenerObject,
  options?: boolean | AddEventListenerOptions,
) {
  // SAFETY: The target's native event map supplies the callback's event type.
  const listener = handler as EventListenerOrEventListenerObject;
  target.addEventListener(type, listener, options);
  return () => target.removeEventListener(type, listener, options);
}

/** One external operation owns these listeners and retires them together. */
export function createListenerScope() {
  const controller = new AbortController();
  return {
    signal: controller.signal,
    listen<T extends EventTarget, K extends string>(
      target: T,
      type: K,
      handler: ((event: NativeEventMap<T>[K]) => void) | EventListenerObject,
      options?: boolean | Omit<AddEventListenerOptions, "signal">,
    ) {
      listen(target, type, handler, {
        ...(typeof options === "boolean" ? { capture: options } : options),
        signal: controller.signal,
      });
    },
    dispose: () => controller.abort(),
  };
}
