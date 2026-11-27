import {
  useEffect,
  useRef,
  useState,
} from "react";

import {
  Network,
  Server,
  Route,
  GitBranch,
  Database,
  Maximize,
  Minimize,
  Activity,
  Pause,
} from "lucide-react";

import "./multiArchitectureNavigation.css";

const SECTIONS = [
  {
    id: "sr-arch-logical",
    label: "Logical architecture",
    icon: Network,
  },
  {
    id: "sr-arch-physical",
    label: "Physical deployment",
    icon: Server,
  },
  {
    id: "sr-arch-request",
    label: "Request journey",
    icon: Route,
  },
  {
    id: "sr-arch-consensus",
    label: "Consensus",
    icon: GitBranch,
  },
  {
    id: "sr-arch-storage",
    label: "Storage",
    icon: Database,
  },
] as const;

export function MultiArchitectureNavigation() {
  const root = useRef<HTMLDivElement>(null);

  const [active, setActive] =
    useState<string>(SECTIONS[0].id);

  const [motion, setMotion] = useState(
    () => !window.matchMedia(
      "(prefers-reduced-motion: reduce)",
    ).matches,
  );

  const [fullscreen, setFullscreen] =
    useState(false);

  const [error, setError] =
    useState<string | null>(null);

  useEffect(() => {
    const page = root.current?.closest(
      ".sr-architecture-page",
    );

    if (!page) return;

    page.classList.toggle(
      "sr-architecture-motion-off",
      !motion,
    );

    return () => {
      page.classList.remove(
        "sr-architecture-motion-off",
      );
    };
  }, [motion]);

  useEffect(() => {
    const page = root.current?.closest(
      ".sr-architecture-page",
    );

    if (!page) return;

    let frame = 0;

    function update() {
      const nav = root.current;

      if (!nav || !page) return;

      const threshold =
        nav.getBoundingClientRect().bottom + 32;

      let selected: string = SECTIONS[0].id;

      for (const section of SECTIONS) {
        const element = page.querySelector(
          `#${section.id}`,
        );

        if (
          element &&
          element.getBoundingClientRect().top
            <= threshold
        ) {
          selected = section.id;
        }
      }

      setActive(selected);
    }

    function schedule() {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(update);
    }

    function fullscreenChanged() {
      setFullscreen(
        document.fullscreenElement === page,
      );

      schedule();
    }

    window.addEventListener(
      "scroll",
      schedule,
      true,
    );

    window.addEventListener(
      "resize",
      schedule,
    );

    document.addEventListener(
      "fullscreenchange",
      fullscreenChanged,
    );

    schedule();

    return () => {
      cancelAnimationFrame(frame);

      window.removeEventListener(
        "scroll",
        schedule,
        true,
      );

      window.removeEventListener(
        "resize",
        schedule,
      );

      document.removeEventListener(
        "fullscreenchange",
        fullscreenChanged,
      );
    };
  }, []);

  function navigate(id: string) {
    const element =
      document.getElementById(id);

    if (!element) return;

    const reducedMotion = window.matchMedia(
      "(prefers-reduced-motion: reduce)",
    ).matches;

    element.scrollIntoView({
      behavior:
        motion && !reducedMotion
          ? "smooth"
          : "instant",
      block: "start",
    });

    setActive(id);
  }

  async function toggleFullscreen() {
    const page = root.current?.closest(
      ".sr-architecture-page",
    );

    if (!page) return;

    setError(null);

    try {
      if (document.fullscreenElement === page) {
        await document.exitFullscreen();
      } else {
        await page.requestFullscreen();
      }
    } catch (failure) {
      setError(String(failure));
    }
  }

  return (
    <div
      ref={root}
      className="san-navigation"
    >
      <div className="san-header">
        <div className="san-brand">
          <Network size={20} />

          <div>
            <strong>Architecture Explorer</strong>
            <small>
              Multi-Raft engineering visualization
            </small>
          </div>
        </div>

        <div className="san-actions">
          <button
            type="button"
            aria-pressed={motion}
            onClick={() =>
              setMotion(value => !value)
            }
            className={
              motion ? "san-motion-active" : ""
            }
          >
            {motion
              ? <Activity size={15} />
              : <Pause size={15} />}
            Motion {motion ? "on" : "off"}
          </button>

          <button
            type="button"
            onClick={() => void toggleFullscreen()}
            disabled={!document.fullscreenEnabled}
          >
            {fullscreen
              ? <Minimize size={15} />
              : <Maximize size={15} />}

            {fullscreen
              ? "Exit fullscreen"
              : "Presentation mode"}
          </button>
        </div>
      </div>

      <nav
        className="san-tabs"
        aria-label="Architecture sections"
      >
        {SECTIONS.map(section => {
          const Icon = section.icon;

          return (
            <button
              type="button"
              key={section.id}
              className={
                active === section.id
                  ? "san-tab-active"
                  : ""
              }
              aria-current={
                active === section.id
                  ? "location"
                  : undefined
              }
              onClick={() =>
                navigate(section.id)
              }
            >
              <Icon size={16} />
              {section.label}
            </button>
          );
        })}
      </nav>

      <div className="san-legend">
        <span>
          <i className="san-configured" />
          Configured architecture
        </span>

        <span>
          <i className="san-live" />
          Observed telemetry
        </span>

        <span>
          <i className="san-preview" />
          Routing preview
        </span>
      </div>

      {error && (
        <div className="san-error" role="alert">
          {error}
        </div>
      )}
    </div>
  );
}
