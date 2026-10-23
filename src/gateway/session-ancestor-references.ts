import { createHash, randomUUID } from "node:crypto";
import type {
  SessionAncestorRef,
  SessionRow,
} from "../../packages/gateway-protocol/src/schema/sessions-row.js";
import {
  SESSION_ROW_DELTA_MAX_ENTRIES,
  sessionRowIdentity,
  type SessionRowWire,
} from "../../packages/gateway-protocol/src/session-row-deltas.js";
import type { SessionEventRowsResetCause } from "../infra/diagnostic-rpc-types.js";
import { isDeeplyFrozenPlainData } from "../shared/immutable-data.js";
import {
  recordSessionEventRowReset,
  updateSessionEventRowRetention,
} from "./session-event-row-retention.js";
import {
  prepareSessionRowDeltaFields,
  type SessionRowDeltaFields,
  type createSessionRowDeltaDiffer,
} from "./session-row-delta-fields.js";
import { serializeSessionRow } from "./session-row-serialization.js";

const MAX_ROWS = 128;
const MAX_CONTENT_CHARS = 128 * 1024;
const MAX_VECTOR_BYTES = 2 * 1024 * 1024;
const EMPTY_ANCESTORS = {};

type RowDelivery = {
  wire: SessionRowWire[];
  serializeSession: () => string;
  delivered: (connection: ConnectionSessionRows) => void;
};
type AncestorDelivery = {
  ancestorSessions: SessionRow[];
  ancestorSessionRefs?: SessionAncestorRef[];
  delivered: (connection: ConnectionSessionRows) => void;
};

type DeliveredRow = { version: number; fields: SessionRowDeltaFields; bytes: number };
const encodedRowBytes = new WeakMap<SessionRow, number>();

function rowByteLength(row: SessionRow): number {
  let bytes = encodedRowBytes.get(row);
  if (bytes === undefined) {
    bytes = Buffer.byteLength(serializeSessionRow(row));
    encodedRowBytes.set(row, bytes);
  }
  return bytes;
}

type DeliveredAncestor = { key: string; content: string; chars: number; revision: string };
const preparedAncestors = new WeakMap<
  SessionRow,
  {
    row: SessionRow;
    identity: string;
    content: string;
    chars: number;
  }
>();

/** Prepare immutable viewer content once, before consulting connection delivery history. */
export function prepareSessionAncestor(row: SessionRow) {
  const cached = preparedAncestors.get(row);
  if (cached) {
    return cached;
  }
  const serialized = JSON.stringify(row.snapshotAt === undefined ? row : { ...row, snapshotAt: 0 });
  const prepared = {
    row,
    identity: JSON.stringify([row.agentId, row.key]),
    content: createHash("sha256").update(serialized).digest("base64url"),
    chars: serialized.length,
  };
  // Presentation replacement retires these facts; mutable callers still encode each sample.
  if (isDeeplyFrozenPlainData(row)) {
    preparedAncestors.set(row, prepared);
  }
  return prepared;
}

/** Connection-owned, bounded history of exactly the presented wire content. */
export class ConnectionSessionRows {
  readonly #rows = new Map<string, DeliveredAncestor>();
  #chars = 0;
  // Separate indexes preserve legacy ancestor admission order and wire-row LRU order.
  readonly #wireRows = new Map<string, DeliveredRow>();
  #wireBytes = 0;
  #version = 0;
  // Tokens certify the whole history, including order and capacity, without retaining it.
  #wireRevision: object | number = 0;
  #ancestorRevision = EMPTY_ANCESTORS;

  clear(cause: SessionEventRowsResetCause = "close"): void {
    recordSessionEventRowReset(cause, this.#wireRows.size);
    updateSessionEventRowRetention(-this.#wireRows.size, -this.#wireBytes);
    this.#wireRows.clear();
    this.#wireBytes = 0;
    this.#wireRevision = this.#version;
    this.clearAncestors();
  }

  clearAncestors(): void {
    this.#rows.clear();
    this.#chars = 0;
    this.#ancestorRevision = EMPTY_ANCESTORS;
  }

  static forget(this: void, key: string) {
    const revisions = new WeakMap<object, object>();
    return (connection: ConnectionSessionRows) => {
      const before = connection.#rows.size;
      for (const [identity, row] of connection.#rows) {
        if (row.key === key) {
          connection.#delete(identity);
        }
      }
      if (before !== connection.#rows.size) {
        let next = revisions.get(connection.#ancestorRevision);
        if (!next) {
          next = connection.#rows.size ? {} : EMPTY_ANCESTORS;
          revisions.set(connection.#ancestorRevision, next);
        }
        connection.#ancestorRevision = next;
      }
    };
  }

  #delete(identity: string): void {
    const previous = this.#rows.get(identity);
    if (previous) {
      this.#chars -= previous.chars;
      this.#rows.delete(identity);
    }
  }

  /** One publication prepares each identical socket history once; only handoff mutates it. */
  static prepareRows(rows: SessionRow[], diff: ReturnType<typeof createSessionRowDeltaDiffer>) {
    const deliveries = new Map<object | number, RowDelivery>();
    return (connection: ConnectionSessionRows): RowDelivery => {
      const revision = connection.#wireRevision;
      const cached = deliveries.get(revision);
      if (cached) {
        return cached;
      }
      const updates = new Map<string, DeliveredRow | undefined>();
      let count = connection.#wireRows.size;
      let bytes = connection.#wireBytes;
      let version = connection.#version;
      let generations = 0;
      let evictions = 0;
      const update = (identity: string, next: DeliveredRow | undefined) => {
        const previous = updates.has(identity)
          ? updates.get(identity)
          : connection.#wireRows.get(identity);
        count += Number(Boolean(next)) - Number(Boolean(previous));
        bytes += (next?.bytes ?? 0) - (previous?.bytes ?? 0);
        updates.delete(identity);
        updates.set(identity, next);
      };
      const wire = rows.map((row): SessionRowWire => {
        const identity = sessionRowIdentity(row.key, row.agentId);
        const fields = prepareSessionRowDeltaFields(row);
        const previous = updates.has(identity)
          ? updates.get(identity)
          : connection.#wireRows.get(identity);
        const sameGeneration =
          previous &&
          previous.fields.fingerprints.get("sessionId") === fields.fingerprints.get("sessionId") &&
          previous.fields.fingerprints.get("lifecycleRevision") ===
            fields.fingerprints.get("lifecycleRevision");
        const charge = 64 + 16 * fields.fieldCount + 2 * identity.length;
        let eviction: string | undefined;
        if (!previous && count === SESSION_ROW_DELTA_MAX_ENTRIES) {
          for (const candidate of connection.#wireRows.keys()) {
            if (!updates.has(candidate)) {
              eviction = candidate;
              break;
            }
          }
          if (!eviction) {
            for (const [candidate, value] of updates) {
              if (value) {
                eviction = candidate;
                break;
              }
            }
          }
        }
        const evictionBytes = eviction
          ? ((updates.get(eviction) ?? connection.#wireRows.get(eviction))?.bytes ?? 0)
          : 0;
        const retain = bytes - (previous?.bytes ?? 0) - evictionBytes + charge <= MAX_VECTOR_BYTES;
        const next = { version: ++version, fields, bytes: charge };
        const header = { key: row.key, agentId: row.agentId, version: next.version };
        const full = { ...header, full: row, ...(!retain ? { retain: false as const } : {}) };
        let result: SessionRowWire = full;
        if (retain && previous && sameGeneration) {
          const patch = {
            ...header,
            base: previous.version,
            ...diff(previous.fields, fields, row),
          };
          const fullBytes =
            Buffer.byteLength(JSON.stringify({ ...header, full: null })) - 4 + rowByteLength(row);
          if (Buffer.byteLength(JSON.stringify(patch)) < fullBytes) {
            result = patch;
          }
        }
        if (retain && eviction) {
          update(eviction, undefined);
          evictions++;
        }
        if (previous && !sameGeneration) {
          generations++;
        } else if (previous && !retain) {
          evictions++;
        }
        update(identity, retain ? next : undefined);
        return result;
      });
      const nextRevision = count ? {} : version;
      let sessionBytes: string | undefined;
      const delivery: RowDelivery = {
        wire,
        serializeSession: () => (sessionBytes ??= JSON.stringify(wire[0])),
        delivered: (recipient) => {
          const previousCount = recipient.#wireRows.size;
          const previousBytes = recipient.#wireBytes;
          for (const [identity, next] of updates) {
            recipient.#wireRows.delete(identity);
            if (next) {
              recipient.#wireRows.set(identity, next);
            }
          }
          recipient.#wireBytes = bytes;
          recipient.#version = version;
          recipient.#wireRevision = nextRevision;
          updateSessionEventRowRetention(count - previousCount, bytes - previousBytes);
          recordSessionEventRowReset("generation", generations);
          recordSessionEventRowReset("evicted", evictions);
        },
      };
      deliveries.set(revision, delivery);
      return delivery;
    };
  }

  static prepare(rows: ReturnType<typeof prepareSessionAncestor>[], revision = randomUUID()) {
    const deliveries = new WeakMap<object, AncestorDelivery>();
    return (connection: ConnectionSessionRows): AncestorDelivery => {
      const baseline = connection.#ancestorRevision;
      const cached = deliveries.get(baseline);
      if (cached) {
        return cached;
      }
      const ancestorSessions: SessionRow[] = [];
      const ancestorSessionRefs: SessionAncestorRef[] = [];
      let updates: Map<string, DeliveredAncestor | undefined> | undefined;
      for (const { row, identity, content, chars } of rows) {
        const snapshotAt = row.snapshotAt;
        const previous = connection.#rows.get(identity);
        if (previous?.content === content && snapshotAt !== undefined) {
          ancestorSessionRefs.push({
            key: row.key,
            sessionId: row.sessionId,
            agentId: row.agentId,
            revision: previous.revision,
            snapshotAt,
          });
          continue;
        }
        ancestorSessions.push({ ...row, ancestorRevision: revision });
        (updates ??= new Map()).set(
          identity,
          snapshotAt !== undefined && chars <= MAX_CONTENT_CHARS
            ? { key: row.key, content, chars, revision }
            : undefined,
        );
      }
      const revisions = new WeakMap<object, object>();
      const delivery: AncestorDelivery = {
        ancestorSessions,
        ...(ancestorSessionRefs.length ? { ancestorSessionRefs } : {}),
        delivered: (recipient) => {
          if (!updates) {
            return;
          }
          for (const [identity, row] of updates) {
            recipient.#delete(identity);
            if (row) {
              recipient.#rows.set(identity, row);
              recipient.#chars += row.chars;
            }
          }
          while (recipient.#rows.size > MAX_ROWS || recipient.#chars > MAX_CONTENT_CHARS) {
            recipient.#delete(recipient.#rows.keys().next().value!);
          }
          // The publication forgets the current row before certifying ancestors.
          let next = revisions.get(recipient.#ancestorRevision);
          if (!next) {
            next = recipient.#rows.size ? {} : EMPTY_ANCESTORS;
            revisions.set(recipient.#ancestorRevision, next);
          }
          recipient.#ancestorRevision = next;
        },
      };
      deliveries.set(baseline, delivery);
      return delivery;
    };
  }
}
