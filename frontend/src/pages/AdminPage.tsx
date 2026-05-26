import { Panel } from "../components/Panel";

const exclusions = ["ABCD", "EFGH", "IJKL", "MNOP", "QRST"];

export function AdminPage() {
  return (
    <div className="page-grid">
      <Panel title="Exclusions" aside={<span className="eyebrow">{exclusions.length} symbols</span>}>
        <div className="pill-list">
          {exclusions.map((item) => (
            <span key={item} className="symbol-pill">
              {item}
            </span>
          ))}
        </div>
      </Panel>
    </div>
  );
}
