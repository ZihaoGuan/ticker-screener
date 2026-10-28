import assert from "node:assert/strict";
import { groupMartinActivity } from "../src/lib/martinActivity.ts";

const positions = [
  { id: 10, ticker: "ABC", status: "closed", opened_on: "2026-01-01", closed_on: "2026-01-04" },
  { id: 11, ticker: "ABC", status: "open", opened_on: "2026-01-03" },
  { id: 12, ticker: "XYZ", status: "open", opened_on: "2026-01-02" },
];
const events = [
  { position_id: 10, ticker: "ABC", event_type: "order_filled", event_date: "2026-01-01", payload_json: {} },
  { position_id: 11, ticker: "ABC", event_type: "rotation_exit", event_date: "2026-01-04", payload_json: { price: 12 } },
  { ticker: "ABC", event_type: "trim_3r", event_date: "2026-01-03", payload_json: { price: 11 } },
  { ticker: "XYZ", event_type: "order_filled", event_date: "2026-01-02", payload_json: {} },
  { position_id: 10, ticker: "ABC", event_type: "candidate_blocked", event_date: "2026-01-02", payload_json: {} },
];
const grouped = groupMartinActivity(positions, events);
assert.deepEqual(grouped.map((items) => items.map((event) => event.event_type)), [["order_filled"], ["rotation_exit"], ["order_filled"]]);
assert.equal(grouped[0][0].payload_json.price, undefined, "missing execution price remains missing");
console.log("Martin activity grouping self-check passed.");
