"use client";

import { useMemo } from "react";
import { useSearchParams } from "next/navigation";
import { normalizeLang, t, withLang } from "./i18n-core";
export { normalizeLang, t, withLang };

export function useLang() {
  const searchParams = useSearchParams();
  return useMemo(() => normalizeLang(searchParams?.get("lang")), [searchParams]);
}
