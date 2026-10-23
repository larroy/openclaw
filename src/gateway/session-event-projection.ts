import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import {
  GATEWAY_CLIENT_CAPS,
  GATEWAY_CLIENT_IDS,
  hasGatewayClientCap,
} from "../../packages/gateway-protocol/src/client-info.js";
import type { SessionRow } from "../../packages/gateway-protocol/src/schema/sessions-row.js";
import { parseAgentSessionKey } from "../routing/session-key.js";
import { getOrCreateWeakOwnerMap } from "../shared/weak-owner-map.js";
import { readGatewayAccessRevision } from "./gateway-access-revision.js";
import type {
  PrepareSessionEventProjection,
  SessionEventProjection,
} from "./server-broadcast-types.js";
import type { GatewayClient } from "./server-methods/client-types.js";
import { ConnectionSessionRows, prepareSessionAncestor } from "./session-ancestor-references.js";
import { buildGatewaySessionSnapshot } from "./session-event-payload.js";
import { createSessionRowDeltaDiffer } from "./session-row-delta-fields.js";
import type { SessionRowReadView } from "./session-row-prepared-read.js";
import { prepareSessionRowPublication } from "./session-row-presentation.js";
import type { SessionRowProjection } from "./session-row-projection.js";
import { serializeSessionRow } from "./session-row-serialization.js";
import type { GatewaySessionRow } from "./session-utils.types.js";

type ProjectedPayload = Record<string, unknown> & {
  session: SessionRow;
  ancestorSessions?: SessionRow[];
};

/** Bind prepared rows without exposing their implementation to transport contracts. */
export function prepareSessionEventProjection(
  projection: SessionRowProjection,
  read?: SessionRowReadView,
  prepared?: {
    ancestors: ReturnType<SessionRowProjection["ancestorRows"]>;
    /** The transcript publisher owns the message graph for synchronous fanout. */
    encodeMessage?: true;
    /** Rechecked per recipient after row projection; exact interest always wins. */
    omitMessage?: (client: GatewayClient) => boolean;
    controlUiOnly?: boolean;
  },
): PrepareSessionEventProjection {
  return (event, payload, eventScope, connection) => {
    if (!connection.isCurrentProjection(projection)) {
      return () => undefined;
    }
    if ((event !== "sessions.changed" && event !== "session.message") || !isRecord(payload)) {
      return undefined;
    }
    const source = payload;
    if (source.reason === "delete" || typeof source.sessionKey !== "string") {
      connection.forgetAncestors(
        typeof source.sessionKey === "string" ? source.sessionKey : undefined,
      );
      return undefined;
    }
    const scope = connection.resolveAgentScope(
      source.sessionKey,
      typeof source.agentId === "string" ? source.agentId : eventScope.agentId,
    );
    if (!scope?.[1] || (!scope[0] && !scope[2] && !parseAgentSessionKey(source.sessionKey))) {
      return undefined;
    }
    const query = { key: source.sessionKey, agentId: scope[1] };
    const rows = read ?? projection;
    const record = rows.describe(query);
    if (
      !record ||
      (typeof source.sessionId === "string" && source.sessionId !== record.entry.sessionId)
    ) {
      return () => undefined;
    }
    const base = isRecord(source.session)
      ? source
      : {
          ...buildGatewaySessionSnapshot({
            sessionRow: rows.present(record),
            agentId: scope[0],
          }),
          ...source,
        };
    const sourceRow = base.session;
    if (
      !isRecord(sourceRow) ||
      sourceRow.sessionId !== record.entry.sessionId ||
      (sourceRow.lifecycleRevision !== undefined &&
        sourceRow.lifecycleRevision !== record.entry.lifecycleRevision)
    ) {
      return () => undefined;
    }
    // Transcript producers publish one message; only the recipient envelope varies.
    let messageBytes: Buffer | undefined;
    const serializeMessage =
      event === "session.message" && prepared?.encodeMessage
        ? () => {
            if (!messageBytes) {
              const field = JSON.stringify({ message: base.message }).slice(1, -1);
              messageBytes = Buffer.from(field ? `${field},` : "");
            }
            return messageBytes;
          }
        : undefined;
    const presentRecipient = prepareSessionRowPublication(projection, Date.now(), rows);
    const ancestors = prepared ? prepared.ancestors : projection.ancestorRows(record, read);
    const enrichment = { includeDerivedTitles: true, includeLastMessage: true };
    const ancestorRevision = randomUUID();
    const audiences = new Map<
      string,
      {
        row: GatewaySessionRow;
        ancestors: ReturnType<typeof ConnectionSessionRows.prepare> | undefined;
        hiddenAncestors: ReturnType<typeof ConnectionSessionRows.forget>[];
        payloads: Map<
          object | undefined,
          {
            payload: ProjectedPayload;
            rows: ReturnType<typeof ConnectionSessionRows.prepareRows>;
            forgetRecapAncestors?: ReturnType<typeof ConnectionSessionRows.forget>[];
          }
        >;
      }
    >();
    const diff = createSessionRowDeltaDiffer();
    const deltaPayloads = new WeakMap<object, Map<object, Record<string, unknown>>>();
    const fieldRefPayloads = new WeakMap<object, Record<string, unknown>>();
    const bodylessPayloads = new WeakMap<object, Record<string, unknown>>();
    const deliveries = new WeakMap<object, SessionEventProjection>();
    const forgetCurrentRow = ConnectionSessionRows.forget(record.key);
    let revision = projection.state.revision;
    let accessRevision = readGatewayAccessRevision();
    // Run display facts belong to this synchronous publication; recipient authority stays live.
    const runProjector = connection.createRunProjector();
    return (client) => {
      // Check before audience payload/encoding caches: equal sharing scopes do not imply equal UI.
      if (prepared?.controlUiOnly && client.connect.client.id !== GATEWAY_CLIENT_IDS.CONTROL_UI) {
        return undefined;
      }
      if (!connection.isCurrentProjection(projection) || !projection.isCurrent(record)) {
        return undefined;
      }
      const currentAccessRevision = readGatewayAccessRevision();
      if (revision !== projection.state.revision || accessRevision !== currentAccessRevision) {
        audiences.clear();
        revision = projection.state.revision;
        accessRevision = currentAccessRevision;
      }
      // Resolve each recipient's live authority before reusing this synchronous publication.
      const recipient = presentRecipient(client, runProjector);
      const audienceKey = recipient.getAudienceKey();
      let audience = audiences.get(audienceKey);
      if (!audience) {
        const presentation = recipient.getPresentation();
        const row = presentation.present(record, enrichment);
        if (!row) {
          return undefined;
        }
        const hiddenAncestors: string[] = [];
        const ancestorRows = ancestors?.every((ancestor) => projection.isCurrent(ancestor))
          ? ancestors.flatMap((ancestor) => {
              if (presentation.sharing.entryFilter?.(ancestor.key, ancestor.entry) === false) {
                hiddenAncestors.push(ancestor.key);
                return [];
              }
              const presented = presentation.present(ancestor, enrichment);
              return presented ? [prepareSessionAncestor(presented)] : [];
            })
          : undefined;
        audience = {
          row,
          ancestors: ancestorRows && ConnectionSessionRows.prepare(ancestorRows, ancestorRevision),
          hiddenAncestors: hiddenAncestors.map(ConnectionSessionRows.forget),
          payloads: new Map(),
        };
        audiences.set(audienceKey, audience);
      }
      const { row } = audience;
      const references = connection.references(client);
      for (const forget of audience.hiddenAncestors) {
        forget(references);
      }
      const ancestorDelivery = audience.ancestors?.(references);
      if (!ancestorDelivery) {
        connection.forgetConnectionAncestors(client);
      }
      let preparedPayload = audience.payloads.get(ancestorDelivery);
      if (!preparedPayload) {
        const projected: ProjectedPayload = {
          ...base,
          session: row,
          fastMode: row.fastMode,
          effectiveFastMode: row.effectiveFastMode,
          ancestorSessions: ancestorDelivery?.ancestorSessions,
          ancestorSessionRefs: ancestorDelivery?.ancestorSessionRefs,
          visibility: row.visibility,
          sharingRole: row.sharingRole,
          ...(isRecord(base.activitySummary) && row.activitySummary
            ? {
                activitySummary: {
                  ...base.activitySummary,
                  canEnsure: row.activitySummary.canEnsure,
                },
              }
            : {}),
        };
        if (Object.hasOwn(projected, "childSessions")) {
          projected.childSessions = row.childSessions;
        }
        preparedPayload = {
          payload: projected,
          rows: ConnectionSessionRows.prepareRows(
            [row, ...(projected.ancestorSessions ?? [])],
            diff,
          ),
          forgetRecapAncestors:
            event === "sessions.changed" && source.reason === "activity-summary"
              ? ancestorDelivery?.ancestorSessions.map((ancestor) =>
                  ConnectionSessionRows.forget(ancestor.key),
                )
              : undefined,
        };
        audience.payloads.set(ancestorDelivery, preparedPayload);
      }
      const projected = preparedPayload.payload;
      const rowDelivery = hasGatewayClientCap(
        client.connect.caps,
        GATEWAY_CLIENT_CAPS.SESSION_ROW_DELTAS,
      )
        ? preparedPayload.rows(references)
        : undefined;
      let wirePayload: Record<string, unknown> = projected;
      if (rowDelivery) {
        if (
          hasGatewayClientCap(client.connect.caps, GATEWAY_CLIENT_CAPS.SESSION_EVENT_FIELD_REFS)
        ) {
          let compact = fieldRefPayloads.get(projected);
          if (!compact) {
            compact = { ...projected };
            const sessionFieldRefs: number[] = [];
            // Reference the reconstructed JSON row, independent of patch insertion order.
            const fields = Object.entries(row)
              .filter(([, value]) => value !== undefined)
              .toSorted(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
            for (let index = 0; index < fields.length; index++) {
              const [field, value] = fields[index]!;
              if (
                field !== "key" &&
                field !== "agentId" &&
                Object.hasOwn(projected, field) &&
                isDeepStrictEqual(projected[field], value)
              ) {
                sessionFieldRefs.push(index);
                delete compact[field];
              }
            }
            compact.sessionFieldRefs = sessionFieldRefs;
            fieldRefPayloads.set(projected, compact);
          }
          wirePayload = compact;
        }
        const cached = getOrCreateWeakOwnerMap(deltaPayloads, wirePayload);
        let deltaPayload = cached.get(rowDelivery);
        if (!deltaPayload) {
          deltaPayload = {
            ...wirePayload,
            session: rowDelivery.wire[0],
            ...(Array.isArray(projected.ancestorSessions)
              ? { ancestorSessions: rowDelivery.wire.slice(1) }
              : {}),
          };
          cached.set(rowDelivery, deltaPayload);
        }
        wirePayload = deltaPayload;
      }
      const omitMessage = event === "session.message" && prepared?.omitMessage?.(client);
      if (omitMessage) {
        let bodyless = bodylessPayloads.get(wirePayload);
        if (!bodyless) {
          bodyless = { ...wirePayload };
          delete bodyless.message;
          bodylessPayloads.set(wirePayload, bodyless);
        }
        wirePayload = bodyless;
      }
      let delivery = deliveries.get(wirePayload);
      if (delivery) {
        return delivery;
      }
      const { forgetRecapAncestors } = preparedPayload;
      delivery = {
        payload: wirePayload,
        serializeSession: rowDelivery?.serializeSession ?? (() => serializeSessionRow(row)),
        serializeMessage: omitMessage ? undefined : serializeMessage,
        delivered: (targetClient) => {
          const recipientRows = connection.references(targetClient);
          rowDelivery?.delivered(recipientRows);
          forgetCurrentRow(recipientRows);
          if (forgetRecapAncestors) {
            // Rosters skip recaps, so a full recap row cannot certify a later reference.
            for (const forget of forgetRecapAncestors) {
              forget(recipientRows);
            }
          } else {
            ancestorDelivery?.delivered(recipientRows);
          }
        },
      };
      deliveries.set(wirePayload, delivery);
      return delivery;
    };
  };
}
