"use client";

import dynamic from "next/dynamic";

const TerrenoClient = dynamic(() => import("./TerrenoClient"), { ssr: false });

export default function TerrenoClientWrapper() {
  return <TerrenoClient />;
}
