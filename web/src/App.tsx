import LegacyApp from "./LegacyApp";
import MultiRaftApp from "./multi/MultiRaftApp";

export default function App() {
  const params = new URLSearchParams(
    window.location.search,
  );

  return params.get("mode") === "legacy"
    ? <LegacyApp />
    : <MultiRaftApp />;
}
