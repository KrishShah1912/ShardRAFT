import {
  Play,
  RefreshCw,
  Square,
} from "lucide-react";

import type { ControlledNode } from "../api/control";

interface Props {
  node?: ControlledNode;
  isLeader: boolean;
  busy: boolean;

  onStart: () => void;
  onStop: () => void;
  onRestart: () => void;
}

export function NodeControls({
  node,
  isLeader,
  busy,
  onStart,
  onStop,
  onRestart,
}: Props) {
  const running =
    node?.running === true ||
    node?.reachable === true;

  if (!running) {
    return (
      <div className="node-controls">
        <button
          className="node-action start"
          disabled={busy}
          onClick={onStart}
        >
          <Play size={12} />
          Start
        </button>
      </div>
    );
  }

  return (
    <div className="node-controls">
      <button
        className="node-action"
        disabled={busy}
        onClick={onRestart}
      >
        <RefreshCw size={12} />
        Restart
      </button>

      <button
        className={
          isLeader
            ? "node-action stop leader-stop"
            : "node-action stop"
        }
        disabled={busy}
        onClick={onStop}
      >
        <Square size={11} />
        {isLeader ? "Stop leader" : "Stop"}
      </button>
    </div>
  );
}
