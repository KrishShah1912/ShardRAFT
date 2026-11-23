import { Construction } from "lucide-react";

export function Placeholder({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">SHARDRAFT</div>
          <h1>{title}</h1>
          <p>{description}</p>
        </div>
      </header>

      <div className="placeholder">
        <div className="placeholder-icon">
          <Construction size={28} />
        </div>
        <h2>{title}</h2>
        <p>
          The interface is prepared. Backend observability for
          this section will be connected in the next phase.
        </p>
      </div>
    </>
  );
}
