"use client";

import { useEffect, useRef } from "react";
import { usePathname } from "next/navigation";

const IGNORE_PREFIXES = ["/api", "/_next", "/ctr-geo"];

export default function VisitTracker() {
  const pathname = usePathname();
  const lastTrackedRef = useRef("");

  useEffect(() => {
    const path = pathname || "/";
    if (IGNORE_PREFIXES.some((prefix) => path.startsWith(prefix))) return;

    if (lastTrackedRef.current === path) return;
    lastTrackedRef.current = path;

    fetch("/api/v1/internal/visit-hit", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ path }),
      keepalive: true,
    }).catch(() => {
      // Silently ignore analytics failures.
    });
  }, [pathname]);

  return null;
}
