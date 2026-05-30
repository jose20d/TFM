import { Suspense } from "react";
import ConsultasClient from "./ConsultasClient";

export const metadata = {
  title: "GeoContext | Queries",
  description: "Guided queries to explore deposits, minerals, and spatial proximity.",
};

export default function ConsultasPage() {
  return (
    <Suspense fallback={null}>
      <ConsultasClient />
    </Suspense>
  );
}
